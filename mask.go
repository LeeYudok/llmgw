package llmgw

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// 기본 마스킹 패턴. 순서가 중요하다 — 주민번호·카드·전화번호를 먼저 가려야 계좌번호 패턴이 그 일부를 잘못 먹지 않는다.
var defaultMaskKinds = []struct {
	name  string
	re    *regexp.Regexp
	check func(string) bool // nil 이면 정규식만으로 판단
}{
	// 주민등록번호·외국인등록번호: YYMMDD-[1-8]NNNNNN (하이픈·공백 생략 가능)
	{"rrn", regexp.MustCompile(`\b\d{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01])[-\s]?[1-8]\d{6}\b`), nil},
	// 카드번호: 16자리(4자리씩 하이픈·공백 구분 가능) + Luhn 검사
	{"card", regexp.MustCompile(`\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b`), luhnValid},
	// 휴대전화·유선전화
	{"phone", regexp.MustCompile(`\b(?:01[016789]|02|0[3-6][1-5]|070)[-\s]?\d{3,4}[-\s]?\d{4}\b`), nil},
	// 계좌번호: 하이픈으로 3~4 묶음, 숫자 합계 10~16자리(날짜·시각 같은 짧은 숫자열은 제외).
	// Luhn 이 맞지 않는 4-4-4-4 숫자열도 여기서 통째로 가린다(오타 난 카드번호일 수 있어 넓게 가리는 쪽을 택한다).
	{"account", regexp.MustCompile(`\b\d{2,6}-\d{2,6}-\d{2,7}(?:-\d{1,6})?\b`), digitCount(10, 16)},
	{"email", regexp.MustCompile(`\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b`), nil},
	// API 키·토큰: OpenAI(sk-), Upstage(up_), AWS access key, Bearer 토큰
	{"secret", regexp.MustCompile(`\b(?:sk-[A-Za-z0-9_-]{20,}|up_[A-Za-z0-9]{20,}|AKIA[0-9A-Z]{16})\b|Bearer\s+[A-Za-z0-9._~+/-]{20,}=*`), nil},
}

// MaskKinds 는 기본 제공 마스킹 패턴 이름이다.
func MaskKinds() []string {
	out := make([]string, 0, len(defaultMaskKinds))
	for _, k := range defaultMaskKinds {
		out = append(out, k.name)
	}
	return out
}

type maskRule struct {
	name  string
	re    *regexp.Regexp
	check func(string) bool
}

// masker 는 프롬프트의 개인정보·비밀값을 [REDACTED:<종류>] 로 바꾼다. 되돌릴 수 없다(원문은 보관하지 않는다).
type masker struct{ rules []maskRule }

func newMasker(c MaskConfig) (*masker, error) {
	want := map[string]bool{}
	for _, k := range c.Kinds {
		want[k] = true
	}
	m := &masker{}
	for _, k := range defaultMaskKinds {
		if len(want) == 0 || want[k.name] {
			m.rules = append(m.rules, maskRule{k.name, k.re, k.check})
		}
	}
	names := make([]string, 0, len(c.Custom))
	for name := range c.Custom {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		re, err := regexp.Compile(c.Custom[name])
		if err != nil {
			return nil, fmt.Errorf("mask.custom %s: %w", name, err)
		}
		m.rules = append(m.rules, maskRule{name: name, re: re})
	}
	return m, nil
}

// mask 는 s 를 가린 결과와 가린 건수를 돌려준다.
func (m *masker) mask(s string) (string, int) {
	n := 0
	for _, r := range m.rules {
		s = r.re.ReplaceAllStringFunc(s, func(v string) string {
			if r.check != nil && !r.check(v) {
				return v
			}
			n++
			return "[REDACTED:" + r.name + "]"
		})
	}
	return s, n
}

// maskMessages 는 메시지를 복사해 가린다(호출자의 원본 slice 는 건드리지 않는다).
func (m *masker) maskMessages(msgs []Message) ([]Message, int) {
	out := make([]Message, len(msgs))
	total := 0
	for i, msg := range msgs {
		c, n := m.mask(msg.Content)
		out[i] = Message{Role: msg.Role, Content: c}
		total += n
	}
	return out, total
}

func digits(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func digitCount(lo, hi int) func(string) bool {
	return func(s string) bool { n := len(digits(s)); return n >= lo && n <= hi }
}

// luhnValid 는 카드번호 체크섬(Luhn)을 확인해 우연히 16자리인 숫자열을 걸러낸다.
func luhnValid(s string) bool {
	d := digits(s)
	sum := 0
	for i := len(d) - 1; i >= 0; i-- {
		v := int(d[i] - '0')
		if (len(d)-1-i)%2 == 1 {
			v *= 2
			if v > 9 {
				v -= 9
			}
		}
		sum += v
	}
	return len(d) > 0 && sum%10 == 0
}
