package llmgw

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// Request 는 게이트웨이 호출 1건이다. Route 가 step 체인을 고른다.
type Request struct {
	Client      string    `json:"client,omitempty"` // 호출한 클라이언트 이름(설정의 clients). 비우면 제한 없음
	Route       string    `json:"route"`
	Messages    []Message `json:"messages"`
	MaxTokens   int       `json:"max_tokens,omitempty"`  // 0 이면 step 값
	Temperature *float64  `json:"temperature,omitempty"` // nil 이면 step 값
	JSON        bool      `json:"json,omitempty"`        // JSON 객체 응답 강제 + 검증
	// ResponseFormat 은 OpenAI response_format 을 그대로 받는다(json_object, json_schema 등).
	// json_schema 를 지원하지 않는 provider 로 가면 json_object 로 낮춘다.
	ResponseFormat json.RawMessage `json:"response_format,omitempty"`
	// Reasoning 은 호출자가 원하는 추론 수준(off|on|low|medium|high). reasoning=inherit 인 step 만 따른다.
	Reasoning string `json:"reasoning,omitempty"`
	// NoExternal 이면 external provider step 을 건너뛴다(클라이언트 allow_external=false 와 같은 효과).
	NoExternal bool `json:"no_external,omitempty"`
}

// Response 는 최종 성공 응답과 거쳐온 시도 기록이다.
type Response struct {
	Content   string    `json:"content"`
	Reasoning string    `json:"reasoning,omitempty"`
	Provider  string    `json:"provider"`
	Model     string    `json:"model"`
	Usage     Usage     `json:"usage"`
	Attempts  []Attempt `json:"attempts"`
	Cached    bool      `json:"cached,omitempty"`
	Latency   Duration  `json:"-"`
}

// Attempt 는 시도 1회의 결과다. Error 가 비어 있으면 성공이다.
type Attempt struct {
	Step      int    `json:"step"`
	Provider  string `json:"provider"`
	Model     string `json:"model"`
	Reasoning string `json:"reasoning"`
	Status    int    `json:"status,omitempty"`
	Error     string `json:"error,omitempty"`
	WaitedMS  int64  `json:"waited_ms"`
	LatencyMS int64  `json:"latency_ms"`
}

// ErrAllFailed 는 체인의 모든 step 이 실패했을 때 반환한다. errors.As 로 *ChainError 를 꺼낼 수 있다.
var ErrAllFailed = errors.New("모든 step 실패")

// ErrExternalBlocked 는 외부 차단 요청에서 external step 을 건너뛴 사유다.
var ErrExternalBlocked = errors.New("외부 provider 차단")

// ErrUnknownRoute 는 설정에 없는 라우트를 부를 때 반환한다.
var ErrUnknownRoute = errors.New("라우트 없음")

// 클라이언트 단위 거절 사유.
var (
	ErrUnknownClient   = errors.New("알 수 없는 클라이언트")
	ErrRouteNotAllowed = errors.New("허용되지 않은 라우트")
	ErrClientLimited   = errors.New("클라이언트 한도 초과")
)

// ChainError 는 전 step 실패 시 시도 기록을 담는다.
type ChainError struct{ Attempts []Attempt }

func (e *ChainError) Error() string {
	parts := make([]string, 0, len(e.Attempts))
	for _, a := range e.Attempts {
		parts = append(parts, fmt.Sprintf("%s: %s", a.Provider, a.Error))
	}
	return ErrAllFailed.Error() + " (" + strings.Join(parts, "; ") + ")"
}

func (e *ChainError) Unwrap() error { return ErrAllFailed }

// Gateway 는 설정을 들고 요청을 라우팅한다. 여러 고루틴에서 동시에 써도 된다.
type Gateway struct {
	cfg         *Config
	gates       map[string]*gate // provider 별
	clientGates map[string]*gate // client 별
	clientStats map[string]*clientStats
	keys        map[string]string
	hc          *http.Client
	log         *requestLog

	mu       sync.Mutex
	inflight map[string]*flight // 같은 요청 동시 호출 합치기
	cache    map[string]cacheEntry
}

type flight struct {
	done chan struct{}
	resp *Response
	err  error
}

type cacheEntry struct {
	resp    *Response
	expires time.Time
}

// New 는 설정으로 게이트웨이를 만든다. 키는 각 프로바이더의 api_key_env 환경변수에서 읽는다.
func New(cfg *Config) (*Gateway, error) {
	g := &Gateway{
		cfg:         cfg,
		gates:       map[string]*gate{},
		clientGates: map[string]*gate{},
		clientStats: map[string]*clientStats{},
		keys:        map[string]string{},
		hc:          &http.Client{},
		inflight:    map[string]*flight{},
		cache:       map[string]cacheEntry{},
	}
	for name, c := range cfg.Clients {
		g.clientGates[name] = newGate(&ProviderConfig{
			RPM: c.RPM, MaxConcurrency: c.MaxConcurrency, MaxQueue: c.MaxQueue, QueueTimeout: c.QueueTimeout,
		})
		g.clientStats[name] = &clientStats{}
	}
	if cfg.LogPath != "" {
		l, err := openRequestLog(cfg.LogPath)
		if err != nil {
			return nil, err
		}
		g.log = l
	}
	for name, p := range cfg.Providers {
		g.gates[name] = newGate(p)
		if p.APIKeyEnv != "" {
			k := os.Getenv(p.APIKeyEnv)
			if k == "" {
				return nil, fmt.Errorf("provider %s: 환경변수 %s 가 비어 있음", name, p.APIKeyEnv)
			}
			g.keys[name] = k
		}
	}
	return g, nil
}

// Call 은 라우트의 step 을 위에서부터 시도해 첫 성공 응답을 돌려준다.
// 같은 내용의 요청이 동시에 들어오면 한 번만 호출하고 결과를 나눠 쓴다(cache_ttl 이 있으면 그동안 재사용).
func (g *Gateway) Call(ctx context.Context, req Request) (resp *Response, err error) {
	start := time.Now()
	defer func() { g.record(req, resp, err, time.Since(start)) }()

	route, ok := g.cfg.Routes[req.Route]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownRoute, req.Route)
	}
	if req.Client != "" {
		c, ok := g.cfg.Clients[req.Client]
		if !ok {
			return nil, fmt.Errorf("%w: %q", ErrUnknownClient, req.Client)
		}
		if !c.allows(req.Route) {
			return nil, fmt.Errorf("%w: client %q → route %q", ErrRouteNotAllowed, req.Client, req.Route)
		}
		if !c.externalAllowed() {
			req.NoExternal = true
		}
		release, err := g.clientGates[req.Client].acquire(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, fmt.Errorf("%w: %s (%v)", ErrClientLimited, req.Client, err)
		}
		defer release()
	}
	return g.callRoute(ctx, route, req)
}

// callRoute 는 같은 요청 합치기·캐시를 거쳐 라우트를 실행한다.
func (g *Gateway) callRoute(ctx context.Context, route *RouteConfig, req Request) (*Response, error) {
	key := requestKey(req)

	g.mu.Lock()
	if e, ok := g.cache[key]; ok && time.Now().Before(e.expires) {
		g.mu.Unlock()
		r := *e.resp
		r.Cached = true
		return &r, nil
	}
	if f, ok := g.inflight[key]; ok {
		g.mu.Unlock()
		select {
		case <-f.done:
			if f.err != nil {
				return nil, f.err
			}
			r := *f.resp
			r.Cached = true
			return &r, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	f := &flight{done: make(chan struct{})}
	g.inflight[key] = f
	g.mu.Unlock()

	f.resp, f.err = g.run(ctx, route, req)

	g.mu.Lock()
	delete(g.inflight, key)
	if f.err == nil && route.CacheTTL.Duration > 0 {
		now := time.Now()
		if len(g.cache) >= 1024 { // 만료된 항목만 걷어낸다(상한 없는 증가 방지)
			for k, e := range g.cache {
				if now.After(e.expires) {
					delete(g.cache, k)
				}
			}
		}
		g.cache[key] = cacheEntry{resp: f.resp, expires: now.Add(route.CacheTTL.Duration)}
	}
	g.mu.Unlock()
	close(f.done)
	return f.resp, f.err
}

func (g *Gateway) run(ctx context.Context, route *RouteConfig, req Request) (*Response, error) {
	start := time.Now()
	var attempts []Attempt
	for i := range route.Steps {
		s := &route.Steps[i]
		p := g.cfg.Providers[s.Provider]
		gt := g.gates[s.Provider]
		body := buildBody(p, s, &req)
		model, _ := body["model"].(string)
		if p.External && req.NoExternal {
			attempts = append(attempts, Attempt{Step: i, Provider: s.Provider, Model: model, Reasoning: s.Reasoning, Error: ErrExternalBlocked.Error()})
			continue
		}

		for try := 0; ; try++ {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			a := Attempt{Step: i, Provider: s.Provider, Model: model, Reasoning: s.Reasoning}
			t0 := time.Now()
			release, err := gt.acquire(ctx)
			a.WaitedMS = time.Since(t0).Milliseconds()
			if err != nil {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				a.Error = err.Error() // 대기열·브레이커 거절 → 재시도 없이 다음 step
				attempts = append(attempts, a)
				break
			}
			t1 := time.Now()
			c, err := doCall(ctx, g.hc, p, g.keys[s.Provider], body)
			release()
			a.LatencyMS = time.Since(t1).Milliseconds()

			if err == nil {
				verr := validate(c.content, route.Validate, wantsJSON(&req))
				if verr == nil {
					verr = checkSchema(c.content, schemaOf(req.ResponseFormat))
				}
				if verr != nil {
					gt.report(false)
					a.Error = "검증 실패: " + verr.Error()
					attempts = append(attempts, a)
					break // 같은 설정으로 다시 불러도 같은 결과일 가능성이 커서 다음 step 으로
				}
				gt.report(true)
				attempts = append(attempts, a)
				if c.model == "" {
					c.model = model
				}
				return &Response{
					Content: c.content, Reasoning: c.reasoning, Provider: s.Provider, Model: c.model,
					Usage: c.usage, Attempts: attempts, Latency: Duration{time.Since(start)},
				}, nil
			}

			gt.report(false)
			var ce *callError
			errors.As(err, &ce)
			a.Error = err.Error()
			if ce != nil {
				a.Status = ce.status
			}
			attempts = append(attempts, a)
			if ce == nil || !ce.retryable || try >= s.Retries {
				break
			}
			wait := backoff(try)
			if ce.retryAfter > 0 {
				gt.deferRate(time.Now().Add(ce.retryAfter)) // 같은 프로바이더를 쓰는 다른 요청도 같이 늦춘다
				wait = ce.retryAfter
			}
			if wait > s.MaxRetryWait.Duration {
				break // 오래 기다려야 하면 기다리지 말고 다음 step 으로
			}
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	return nil, &ChainError{Attempts: attempts}
}

// backoff 는 0.5s·1s·2s… 지수 증가에 0~250ms 지터를 더한다(최대 8s).
func backoff(try int) time.Duration {
	d := min(500*time.Millisecond<<try, 8*time.Second)
	return d + time.Duration(rand.Int64N(int64(250*time.Millisecond)))
}

func validate(content string, v Validation, wantJSON bool) error {
	if strings.TrimSpace(content) == "" {
		return errors.New("빈 응답")
	}
	if v.MinChars > 0 && len([]rune(content)) < v.MinChars {
		return fmt.Errorf("%d자 미만", v.MinChars)
	}
	if v.RequireJSON || wantJSON {
		if !json.Valid([]byte(stripFence(content))) {
			return errors.New("JSON 아님")
		}
	}
	for _, m := range v.MustContain {
		if !strings.Contains(content, m) {
			return fmt.Errorf("%q 없음", m)
		}
	}
	return nil
}

func requestKey(req Request) string {
	b, _ := json.Marshal(req)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// ProviderStat 은 프로바이더별 누적·현재 상태다.
type ProviderStat struct {
	Calls, OK, Fail, QueueRejects, QueueTimeouts, BreakerSkips int64
	InFlight, Waiting                                          int64
}

// Stats 는 프로바이더별 상태 스냅샷을 돌려준다.
func (g *Gateway) Stats() map[string]ProviderStat {
	out := make(map[string]ProviderStat, len(g.gates))
	for name, gt := range g.gates {
		out[name] = ProviderStat{
			Calls: gt.stats.Calls.Load(), OK: gt.stats.OK.Load(), Fail: gt.stats.Fail.Load(),
			QueueRejects: gt.stats.QueueRejects.Load(), QueueTimeouts: gt.stats.QueueTimeouts.Load(),
			BreakerSkips: gt.stats.BreakerSkips.Load(),
			InFlight:     gt.inFlight.Load(), Waiting: gt.waiting.Load(),
		}
	}
	return out
}

// Routes 는 설정된 라우트 이름 목록이다.
func (g *Gateway) Routes() []string {
	out := make([]string, 0, len(g.cfg.Routes))
	for name := range g.cfg.Routes {
		out = append(out, name)
	}
	return out
}
