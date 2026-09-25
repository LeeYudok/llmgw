// Package llmgw 는 설정 파일 하나로 여러 OpenAI 호환 LLM 엔드포인트를 묶어 호출하는 게이트웨이다.
//
// 라우트(route)가 호출 순서(step 체인)를 정하고, 각 step 은 프로바이더·추론 여부·토큰 상한·재시도·
// 검증 규칙을 가진다. 프로바이더마다 분당 요청 한도·동시 처리 수·대기열을 두어, 호출이 몰리면
// 대기열에서 기다리고, 넘치거나 오래 기다리면 다음 step 으로 넘긴다(폴백).
package llmgw

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"gopkg.in/yaml.v3"
)

// Config 는 게이트웨이 전체 설정이다. TOML·YAML 둘 다 같은 키로 읽는다.
type Config struct {
	Providers map[string]*ProviderConfig `toml:"providers" yaml:"providers"`
	Routes    map[string]*RouteConfig    `toml:"routes" yaml:"routes"`
	Clients   map[string]*ClientConfig   `toml:"clients" yaml:"clients"`
	Server    ServerConfig               `toml:"server" yaml:"server"`
	// LogPath 에 요청 1건당 JSONL 한 줄(클라이언트·라우트·처리 provider·지연·토큰·에러)을 남긴다.
	// 프롬프트·응답 본문은 남기지 않는다. 비우면 기록하지 않는다.
	LogPath string `toml:"log_path" yaml:"log_path"`
	// Mask 는 provider 로 보내기 전에 프롬프트의 개인정보·비밀값을 가리는 규칙이다.
	Mask MaskConfig `toml:"mask" yaml:"mask"`
}

// MaskConfig 는 프롬프트 마스킹 규칙이다. 가린 값은 [REDACTED:<종류>] 로 바뀌고 원문은 어디에도 남지 않는다.
// passthrough 요청 본문(파일 등)은 형식을 알 수 없어 가리지 않는다.
type MaskConfig struct {
	External bool              `toml:"external" yaml:"external"` // external provider 로 보내는 요청을 가린다
	Kinds    []string          `toml:"kinds" yaml:"kinds"`       // 쓸 기본 패턴(rrn, card, phone, account, email, secret). 비우면 전부
	Custom   map[string]string `toml:"custom" yaml:"custom"`     // 추가 패턴: 이름 → 정규식(RE2)
}

// ProviderConfig 는 OpenAI 호환 엔드포인트 하나와 그 트래픽 제어값이다.
type ProviderConfig struct {
	BaseURL   string `toml:"base_url" yaml:"base_url"`       // 예: https://api.upstage.ai/v1
	APIKeyEnv string `toml:"api_key_env" yaml:"api_key_env"` // 키를 담은 환경변수 이름(값은 설정에 쓰지 않는다)
	Model     string `toml:"model" yaml:"model"`

	// Reasoning 은 추론 on/off 를 요청에 싣는 방식이다.
	//   "chat_template" — body.chat_template_kwargs.enable_thinking (vLLM Qwen·EXAONE)
	//   "effort"        — body.reasoning_effort = low|medium|high (Upstage·OpenAI)
	//   "none"          — 추론 설정을 보내지 않는다
	Reasoning string `toml:"reasoning" yaml:"reasoning"`
	// ReasoningMinTokens — 추론을 켠 step 의 max_tokens 가 이보다 작으면 이 값으로 올린다.
	// 추론 토큰과 본문 토큰이 max_tokens 를 공유해 본문이 빈 채로 잘리는 것을 막는다.
	ReasoningMinTokens int `toml:"reasoning_min_tokens" yaml:"reasoning_min_tokens"`

	RPM            int      `toml:"rpm" yaml:"rpm"`                         // 분당 요청 한도(0 = 무제한)
	MaxConcurrency int      `toml:"max_concurrency" yaml:"max_concurrency"` // 동시 처리 수(0 = 무제한)
	MaxQueue       int      `toml:"max_queue" yaml:"max_queue"`             // 대기열 길이. 넘치면 즉시 다음 step 으로
	QueueTimeout   Duration `toml:"queue_timeout" yaml:"queue_timeout"`     // 대기열에서 기다릴 최대 시간
	Timeout        Duration `toml:"timeout" yaml:"timeout"`                 // 요청 1회 타임아웃

	// 서킷 브레이커: 연속 BreakerFailures 번 실패하면 BreakerCooldown 동안 이 프로바이더를 건너뛴다.
	BreakerFailures int      `toml:"breaker_failures" yaml:"breaker_failures"`
	BreakerCooldown Duration `toml:"breaker_cooldown" yaml:"breaker_cooldown"`

	// JSONSchema — response_format.type=json_schema 를 지원하는가. false 면 json_object 로 낮춰 보낸다.
	JSONSchema bool `toml:"json_schema" yaml:"json_schema"`
	// Passthrough — true 면 서버의 /passthrough/<provider>/<path> 로 OpenAI 형식이 아닌 API 도
	// 이 provider 의 키·한도·대기열을 거쳐 그대로 중계한다(예: 문서 파싱 API).
	Passthrough bool `toml:"passthrough" yaml:"passthrough"`
	// External — 조직 밖으로 데이터가 나가는 provider(외부 API). 외부 차단 요청·클라이언트는 이 step 을 건너뛴다.
	External bool `toml:"external" yaml:"external"`
	// Mask — true 면 external 여부와 상관없이 이 provider 로 보내는 요청을 mask 규칙으로 가린다.
	Mask bool `toml:"mask" yaml:"mask"`
	// StreamIdleTimeout — 스트림이 시작된 뒤 이 시간 동안 새 줄이 오지 않으면 끊는다(기본 60s).
	// timeout 은 스트림에서는 응답 헤더를 받을 때까지만 적용된다.
	StreamIdleTimeout Duration `toml:"stream_idle_timeout" yaml:"stream_idle_timeout"`

	Extra map[string]any `toml:"extra" yaml:"extra"` // 요청 body 에 그대로 합칠 추가 필드
}

// ClientConfig 는 게이트웨이를 호출하는 쪽(서비스·앱) 하나다. 서버 모드에서 Bearer 키로 식별한다.
// clients 를 하나도 정의하지 않으면 서버는 인증 없이 모든 라우트를 연다(로컬 전용 운용).
type ClientConfig struct {
	APIKeyEnv      string   `toml:"api_key_env" yaml:"api_key_env"`         // 이 클라이언트가 쓸 키를 담은 환경변수 이름
	Routes         []string `toml:"routes" yaml:"routes"`                   // 허용 라우트. 비우거나 "*" 면 전부
	Passthrough    []string `toml:"passthrough" yaml:"passthrough"`         // 중계를 허용할 provider 이름
	RPM            int      `toml:"rpm" yaml:"rpm"`                         // 이 클라이언트의 분당 요청 한도(0 = 무제한)
	MaxConcurrency int      `toml:"max_concurrency" yaml:"max_concurrency"` // 동시 요청 수(0 = 무제한)
	MaxQueue       int      `toml:"max_queue" yaml:"max_queue"`             // 넘치면 즉시 429
	QueueTimeout   Duration `toml:"queue_timeout" yaml:"queue_timeout"`
	// AllowExternal — false 면 이 클라이언트의 요청은 external provider step 을 모두 건너뛴다(기본 true).
	AllowExternal *bool `toml:"allow_external" yaml:"allow_external"`
}

func (c *ClientConfig) externalAllowed() bool { return c.AllowExternal == nil || *c.AllowExternal }

// allows 는 라우트 허용 여부다.
func (c *ClientConfig) allows(route string) bool {
	if len(c.Routes) == 0 {
		return true
	}
	for _, r := range c.Routes {
		if r == "*" || r == route {
			return true
		}
	}
	return false
}

func (c *ClientConfig) allowsPassthrough(provider string) bool {
	for _, p := range c.Passthrough {
		if p == "*" || p == provider {
			return true
		}
	}
	return false
}

// RouteConfig 는 호출 이름 하나(예: news-deep)의 step 체인과 공통 규칙이다.
type RouteConfig struct {
	Steps    []StepConfig `toml:"steps" yaml:"steps"`
	CacheTTL Duration     `toml:"cache_ttl" yaml:"cache_ttl"` // 같은 요청 결과를 재사용할 시간(0 = 캐시 안 함)
	Validate Validation   `toml:"validate" yaml:"validate"`
}

// StepConfig 는 체인의 한 칸이다. 위에서부터 순서대로 시도한다.
type StepConfig struct {
	Provider     string   `toml:"provider" yaml:"provider"`
	Model        string   `toml:"model" yaml:"model"`         // 비우면 프로바이더 기본 모델
	Reasoning    string   `toml:"reasoning" yaml:"reasoning"` // off | on | low | medium | high | inherit(호출자가 보낸 값, 없으면 off)
	MaxTokens    int      `toml:"max_tokens" yaml:"max_tokens"`
	Temperature  *float64 `toml:"temperature" yaml:"temperature"`
	Retries      int      `toml:"retries" yaml:"retries"`               // 429·5xx·네트워크 오류 재시도 횟수
	MaxRetryWait Duration `toml:"max_retry_wait" yaml:"max_retry_wait"` // Retry-After 가 이보다 길면 재시도 대신 다음 step
	// MaxWait — 이 step 의 provider 에서 자리를 기다릴 최대 시간. 예상 대기가 이보다 길면 기다리지 않고 바로
	// 다음 step 으로 넘긴다. 0 이면 provider 의 queue_timeout 만 적용한다. 마지막 step 에는 적용하지 않는다.
	MaxWait Duration `toml:"max_wait" yaml:"max_wait"`
	// Spill — "auto" 면 이 provider 에서 기다렸다 처리하는 예상 완료 시간(대기 + 평균 소요)이 다음 step 보다
	// 길 때 바로 다음 step 으로 넘긴다. 두 provider 모두 소요 시간 기록이 쌓인 뒤에만 동작한다. 마지막 step 에는 적용하지 않는다.
	Spill string `toml:"spill" yaml:"spill"`
}

// Validation 은 응답을 받아들일 조건이다. 어기면 재시도하지 않고 다음 step 으로 넘긴다.
type Validation struct {
	MinChars    int      `toml:"min_chars" yaml:"min_chars"`
	RequireJSON bool     `toml:"require_json" yaml:"require_json"`
	MustContain []string `toml:"must_contain" yaml:"must_contain"`
}

// ServerConfig 는 OpenAI 호환 HTTP 서버 모드 설정이다.
type ServerConfig struct {
	Addr string `toml:"addr" yaml:"addr"` // 기본 127.0.0.1:17902
}

// Duration 은 "30s", "2m" 같은 문자열을 받는 time.Duration 이다.
type Duration struct{ time.Duration }

func (d *Duration) UnmarshalText(b []byte) error {
	v, err := time.ParseDuration(string(b))
	if err != nil {
		return fmt.Errorf("duration %q: %w", b, err)
	}
	d.Duration = v
	return nil
}

func (d *Duration) UnmarshalYAML(n *yaml.Node) error { return d.UnmarshalText([]byte(n.Value)) }

// LoadConfig 는 확장자(.toml / .yaml / .yml)로 형식을 골라 읽고 기본값을 채운 뒤 검증한다.
func LoadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	switch strings.ToLower(filepath.Ext(path)) {
	case ".toml":
		if _, err := toml.Decode(string(b), &c); err != nil {
			return nil, fmt.Errorf("toml %s: %w", path, err)
		}
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(b, &c); err != nil {
			return nil, fmt.Errorf("yaml %s: %w", path, err)
		}
	default:
		return nil, fmt.Errorf("설정 형식을 알 수 없음(.toml/.yaml): %s", path)
	}
	c.applyDefaults()
	if err := c.validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Config) applyDefaults() {
	if c.Server.Addr == "" {
		c.Server.Addr = "127.0.0.1:17902"
	}
	for _, p := range c.Providers {
		p.BaseURL = strings.TrimRight(p.BaseURL, "/")
		if p.Reasoning == "" {
			p.Reasoning = "none"
		}
		if p.ReasoningMinTokens == 0 {
			p.ReasoningMinTokens = 4096
		}
		if p.Timeout.Duration == 0 {
			p.Timeout.Duration = 120 * time.Second
		}
		if p.QueueTimeout.Duration == 0 {
			p.QueueTimeout.Duration = 30 * time.Second
		}
		if p.BreakerCooldown.Duration == 0 {
			p.BreakerCooldown.Duration = 30 * time.Second
		}
		if p.StreamIdleTimeout.Duration == 0 {
			p.StreamIdleTimeout.Duration = 60 * time.Second
		}
	}
	for _, cl := range c.Clients {
		if cl.QueueTimeout.Duration == 0 {
			cl.QueueTimeout.Duration = 60 * time.Second
		}
	}
	for _, r := range c.Routes {
		for i := range r.Steps {
			s := &r.Steps[i]
			if s.Reasoning == "" {
				s.Reasoning = "off"
			}
			if s.MaxTokens == 0 {
				s.MaxTokens = 1024
			}
			if s.MaxRetryWait.Duration == 0 {
				s.MaxRetryWait.Duration = 10 * time.Second
			}
		}
	}
}

func (c *Config) validate() error {
	var errs []error
	if len(c.Providers) == 0 {
		errs = append(errs, errors.New("providers 가 비어 있음"))
	}
	for name, p := range c.Providers {
		if p.BaseURL == "" {
			errs = append(errs, fmt.Errorf("provider %s: base_url 없음", name))
		}
		switch p.Reasoning {
		case "none", "chat_template", "effort":
		default:
			errs = append(errs, fmt.Errorf("provider %s: reasoning %q (none|chat_template|effort)", name, p.Reasoning))
		}
	}
	for name, r := range c.Routes {
		if len(r.Steps) == 0 {
			errs = append(errs, fmt.Errorf("route %s: steps 가 비어 있음", name))
		}
		for i, s := range r.Steps {
			p, ok := c.Providers[s.Provider]
			if !ok {
				errs = append(errs, fmt.Errorf("route %s step %d: provider %q 없음", name, i, s.Provider))
				continue
			}
			if s.Model == "" && p.Model == "" {
				errs = append(errs, fmt.Errorf("route %s step %d: model 없음", name, i))
			}
			if s.Spill != "" && s.Spill != "auto" {
				errs = append(errs, fmt.Errorf("route %s step %d: spill %q (auto 또는 비움)", name, i, s.Spill))
			}
			switch s.Reasoning {
			case "off", "on", "low", "medium", "high", "inherit":
			default:
				errs = append(errs, fmt.Errorf("route %s step %d: reasoning %q (off|on|low|medium|high|inherit)", name, i, s.Reasoning))
			}
		}
	}
	known := map[string]bool{}
	for _, k := range MaskKinds() {
		known[k] = true
	}
	for _, k := range c.Mask.Kinds {
		if !known[k] {
			errs = append(errs, fmt.Errorf("mask.kinds: %q (%s)", k, strings.Join(MaskKinds(), "|")))
		}
	}
	if _, err := newMasker(c.Mask); err != nil {
		errs = append(errs, err)
	}
	for name, cl := range c.Clients {
		if cl.APIKeyEnv == "" {
			errs = append(errs, fmt.Errorf("client %s: api_key_env 없음", name))
		}
		for _, r := range cl.Routes {
			if _, ok := c.Routes[r]; !ok && r != "*" {
				errs = append(errs, fmt.Errorf("client %s: route %q 없음", name, r))
			}
		}
		for _, p := range cl.Passthrough {
			pc, ok := c.Providers[p]
			if p != "*" && (!ok || !pc.Passthrough) {
				errs = append(errs, fmt.Errorf("client %s: passthrough provider %q 가 없거나 passthrough=false", name, p))
			}
		}
	}
	return errors.Join(errs...)
}
