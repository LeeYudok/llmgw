package llmgw

import (
	"context"
	"net/http"
	"strings"
	"testing"
)

func TestMaskDefaultKinds(t *testing.T) {
	m, err := newMasker(MaskConfig{})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct{ in, want string }{
		{"주민번호 900101-1234567 입니다", "주민번호 [REDACTED:rrn] 입니다"},
		{"외국인 9001015234567", "외국인 [REDACTED:rrn]"},
		{"카드 4111-1111-1111-1111 결제", "카드 [REDACTED:card] 결제"},
		{"카드 4111111111111111", "카드 [REDACTED:card]"},
		{"연락처 010-1234-5678, 02-123-4567", "연락처 [REDACTED:phone], [REDACTED:phone]"},
		{"계좌 1002-123-456789 로 입금", "계좌 [REDACTED:account] 로 입금"},
		{"번호 1234-5678-9012-3456", "번호 [REDACTED:account]"}, // Luhn 불일치 16자리는 일부만 남기지 않고 통째로
		{"메일 hong.gd@jbbank.co.kr", "메일 [REDACTED:email]"},
		{"키 sk-abcdefghijklmnopqrstuvwxyz0123", "키 [REDACTED:secret]"},
		{"Authorization: Bearer abcdefghijklmnopqrstuvwxyz.0123", "Authorization: [REDACTED:secret]"},
	} {
		if got, n := m.mask(c.in); got != c.want || n == 0 {
			t.Errorf("mask(%q) = %q (%d), want %q", c.in, got, n, c.want)
		}
	}
}

func TestMaskLeavesOrdinaryNumbers(t *testing.T) {
	m, _ := newMasker(MaskConfig{})
	for _, s := range []string{
		"2026-09-25 10:30 기준",        // 날짜·시각
		"매출 1,234,567원, 전년 대비 12.5%", // 금액
		"문서 번호 2026-001, 제123-45호",   // 짧은 하이픈 숫자열
		"코스피 2,650.12 (+1.2%)",
	} {
		if got, n := m.mask(s); n != 0 || got != s {
			t.Errorf("가리면 안 됨: %q → %q", s, got)
		}
	}
}

func TestMaskKindsAndCustom(t *testing.T) {
	m, err := newMasker(MaskConfig{Kinds: []string{"email"}, Custom: map[string]string{"emp": `\bE\d{6}\b`}})
	if err != nil {
		t.Fatal(err)
	}
	got, n := m.mask("사번 E123456, 메일 a@b.com, 전화 010-1234-5678")
	if want := "사번 [REDACTED:emp], 메일 [REDACTED:email], 전화 010-1234-5678"; got != want || n != 2 {
		t.Fatalf("got %q (%d), want %q", got, n, want)
	}
}

func TestMaskConfigValidation(t *testing.T) {
	c := &Config{
		Providers: map[string]*ProviderConfig{"a": {BaseURL: "http://x", Model: "m"}},
		Mask:      MaskConfig{Kinds: []string{"rrn", "passport"}, Custom: map[string]string{"bad": `(`}},
	}
	c.applyDefaults()
	err := c.validate()
	if err == nil || !strings.Contains(err.Error(), "passport") || !strings.Contains(err.Error(), "mask.custom bad") {
		t.Fatalf("모르는 kind·깨진 정규식을 잡아야 함: %v", err)
	}
}

func TestMaskAppliedOnlyToExternalProviders(t *testing.T) {
	ext := newFake(t, func(_ int64, w http.ResponseWriter) { http.Error(w, "down", 500) })
	in := newFake(t, func(_ int64, w http.ResponseWriter) { okJSON(w, "ok") })
	pe := prov(ext.srv.URL)
	pe.External = true
	g := build(t, &Config{
		Providers: map[string]*ProviderConfig{"ext": pe, "in": prov(in.srv.URL)},
		Routes:    map[string]*RouteConfig{"r": {Steps: []StepConfig{{Provider: "ext"}, {Provider: "in"}}}},
		Mask:      MaskConfig{External: true},
	})
	req := userReq("r", "고객 900101-1234567 상담 요약")
	resp, err := g.Call(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	extMsg := ext.bodies[0]["messages"].([]any)[0].(map[string]any)["content"]
	inMsg := in.bodies[0]["messages"].([]any)[0].(map[string]any)["content"]
	if extMsg != "고객 [REDACTED:rrn] 상담 요약" {
		t.Fatalf("external provider 로는 가려서 보내야 함: %v", extMsg)
	}
	if inMsg != "고객 900101-1234567 상담 요약" {
		t.Fatalf("내부 provider 로는 원문 그대로: %v", inMsg)
	}
	if req.Messages[0].Content != "고객 900101-1234567 상담 요약" {
		t.Fatal("호출자의 원본 메시지를 바꾸면 안 됨")
	}
	if resp.Attempts[0].Masked != 1 || resp.Attempts[1].Masked != 0 {
		t.Fatalf("시도별 masked 수 %+v", resp.Attempts)
	}
}

func TestMaskProviderFlagWithoutExternal(t *testing.T) {
	f := newFake(t, func(_ int64, w http.ResponseWriter) { okJSON(w, "ok") })
	p := prov(f.srv.URL)
	p.Mask = true
	g := build(t, &Config{
		Providers: map[string]*ProviderConfig{"a": p},
		Routes:    map[string]*RouteConfig{"r": {Steps: []StepConfig{{Provider: "a"}}}},
	})
	if _, err := g.Call(context.Background(), userReq("r", "메일 a@b.com")); err != nil {
		t.Fatal(err)
	}
	if got := f.bodies[0]["messages"].([]any)[0].(map[string]any)["content"]; got != "메일 [REDACTED:email]" {
		t.Fatalf("provider mask=true 면 가려야 함: %v", got)
	}
}
