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
	Content   string `json:"content"`
	Reasoning string `json:"reasoning,omitempty"`
	// FinishReason 은 provider 가 준 종료 사유(stop, length 등)다. length 면 max_tokens 에서 잘린 것이다.
	FinishReason string    `json:"finish_reason,omitempty"`
	Provider     string    `json:"provider"`
	Model        string    `json:"model"`
	Usage        Usage     `json:"usage"`
	Attempts     []Attempt `json:"attempts"`
	Cached       bool      `json:"cached,omitempty"`
	Latency      Duration  `json:"-"`
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
	Masked    int    `json:"masked,omitempty"` // 이 시도에서 가린 개인정보·비밀값 수
	// Detail 은 upstream 에러 원문(내부 주소·응답 본문 포함)이다. 호출자에게는 내보내지 않고 요청 로그에만 남긴다.
	Detail string `json:"-"`
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

// Detail 은 upstream 원문을 포함한 에러 설명이다(서버 로그·CLI 용 — 호출자에게 내보내지 않는다).
func (e *ChainError) Detail() string {
	parts := make([]string, 0, len(e.Attempts))
	for _, a := range e.Attempts {
		d := a.Error
		if a.Detail != "" && a.Detail != a.Error {
			d += " — " + a.Detail
		}
		parts = append(parts, fmt.Sprintf("%s: %s", a.Provider, d))
	}
	return ErrAllFailed.Error() + " (" + strings.Join(parts, "; ") + ")"
}

// setError 는 시도 기록에 에러를 남긴다. 호출자에게 보이는 Error 는 분류만, 원문은 Detail 에 둔다.
func (a *Attempt) setError(err error) {
	var ce *callError
	if errors.As(err, &ce) {
		a.Status = ce.status
		a.Error = ce.public()
		a.Detail = ce.Error()
		return
	}
	a.Error = err.Error()
}

// Gateway 는 설정을 들고 요청을 라우팅한다. 여러 고루틴에서 동시에 써도 된다.
type Gateway struct {
	cfg         *Config
	gates       map[string]*gate // provider 별
	clientGates map[string]*gate // client 별
	clientStats map[string]*clientStats
	keys        map[string]string
	hc          *http.Client
	log         *requestLog
	masker      *masker
	// inflightGauge 는 Handler 가 만든 서버 동시 요청 카운터다(통계용, 서버 모드가 아니면 nil).
	inflightGauge *inflight

	mu         sync.Mutex
	inflight   map[string]*flight // 같은 요청 동시 호출 합치기
	cache      map[string]cacheEntry
	cacheBytes int64 // 캐시 항목 size 합계(g.mu 로 보호)
}

// flight 는 같은 요청을 한 번만 호출하는 공유 호출이다. 호출은 기다리는 사람이 한 명이라도 남아 있는 동안
// 계속되고, 모두 떠나면(각자 ctx 취소) 그때 취소된다 — 먼저 온 호출자가 취소해도 뒤에 붙은 호출자는 영향이 없다.
type flight struct {
	done    chan struct{}
	resp    *Response
	err     error
	waiters int                // g.mu 로 보호
	cancel  context.CancelFunc // 마지막 대기자가 떠날 때 호출
}

// maxCacheEntries 는 응답 캐시의 최대 항목 수다. 넘치면 만료된 항목을, 그래도 넘치면 오래된 항목부터 버린다.
const maxCacheEntries = 1024

type cacheEntry struct {
	resp    *Response
	size    int64 // responseBytes 어림값
	added   time.Time
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
	m, err := newMasker(cfg.Mask)
	if err != nil {
		return nil, err
	}
	g.masker = m
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
	f, joined := g.inflight[key]
	if !joined {
		// 호출은 첫 호출자의 ctx 와 떼어 낸다(값은 유지). 취소는 대기자가 모두 떠났을 때만 한다.
		fctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		f = &flight{done: make(chan struct{}), cancel: cancel}
		g.inflight[key] = f
		go g.fly(fctx, key, f, route, req)
	}
	f.waiters++
	g.mu.Unlock()

	select {
	case <-f.done:
		if f.err != nil {
			return nil, f.err
		}
		if !joined {
			return f.resp, nil
		}
		r := *f.resp
		r.Cached = true
		return &r, nil
	case <-ctx.Done():
		g.mu.Lock()
		f.waiters--
		if f.waiters == 0 {
			f.cancel()
			if g.inflight[key] == f {
				delete(g.inflight, key) // 취소된 호출에 새 요청이 붙지 않게
			}
		}
		g.mu.Unlock()
		return nil, ctx.Err()
	}
}

// fly 는 공유 호출을 실행하고 결과를 캐시에 넣은 뒤 대기자에게 알린다.
func (g *Gateway) fly(ctx context.Context, key string, f *flight, route *RouteConfig, req Request) {
	defer f.cancel()
	f.resp, f.err = g.run(ctx, route, req)

	g.mu.Lock()
	if g.inflight[key] == f {
		delete(g.inflight, key)
	}
	if f.err == nil && route.CacheTTL.Duration > 0 {
		g.putCache(key, f.resp, route.CacheTTL.Duration)
	}
	g.mu.Unlock()
	close(f.done)
}

// responseBytes 는 캐시에 둔 응답이 차지하는 메모리를 어림한다(본문·추론·시도 기록).
func responseBytes(r *Response) int64 {
	n := len(r.Content) + len(r.Reasoning) + len(r.Provider) + len(r.Model) + 256
	for _, a := range r.Attempts {
		n += 128 + len(a.Error) + len(a.Detail)
	}
	return int64(n)
}

// putCache 는 g.mu 를 잡은 채로 부른다. 항목 수는 maxCacheEntries, 메모리는 cache_max_bytes 를 넘지 않게 한다.
// 넘치면 만료된 항목을, 그래도 넘치면 오래된 항목부터 버린다. 혼자서 상한을 넘는 응답은 캐시하지 않는다.
func (g *Gateway) putCache(key string, resp *Response, ttl time.Duration) {
	size := responseBytes(resp)
	limit := int64(g.cfg.CacheMaxBytes)
	if limit > 0 && size > limit {
		return
	}
	if old, ok := g.cache[key]; ok {
		g.cacheBytes -= old.size
		delete(g.cache, key)
	}
	full := func() bool {
		return len(g.cache) >= maxCacheEntries || (limit > 0 && g.cacheBytes+size > limit)
	}
	now := time.Now()
	if full() {
		for k, e := range g.cache {
			if now.After(e.expires) {
				g.cacheBytes -= e.size
				delete(g.cache, k)
			}
		}
	}
	for full() && len(g.cache) > 0 {
		var oldest string
		var at time.Time
		for k, e := range g.cache {
			if oldest == "" || e.added.Before(at) {
				oldest, at = k, e.added
			}
		}
		g.cacheBytes -= g.cache[oldest].size
		delete(g.cache, oldest)
	}
	g.cache[key] = cacheEntry{resp: resp, size: size, added: now, expires: now.Add(ttl)}
	g.cacheBytes += size
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
		masked := g.maskBody(p, body)
		if why := g.autoSpill(route, i, req.NoExternal); why != "" {
			attempts = append(attempts, Attempt{Step: i, Provider: s.Provider, Model: model, Reasoning: s.Reasoning, Error: why})
			continue
		}

		for try := 0; ; try++ {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			a := Attempt{Step: i, Provider: s.Provider, Model: model, Reasoning: s.Reasoning, Masked: masked}
			t0 := time.Now()
			pm, err := gt.acquireWithin(ctx, g.stepMaxWait(route, i, req.NoExternal))
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
			pm.release()
			a.LatencyMS = time.Since(t1).Milliseconds()

			if err == nil {
				verr := validate(c.content, route.Validate, wantsJSON(&req))
				if verr == nil {
					verr = checkSchema(c.content, schemaOf(req.ResponseFormat))
				}
				if verr != nil {
					pm.reportNeutral() // 요청 내용에 따라 갈리는 실패라 provider 전체를 막지 않는다
					a.Error = "검증 실패: " + verr.Error()
					attempts = append(attempts, a)
					break // 같은 설정으로 다시 불러도 같은 결과일 가능성이 커서 다음 step 으로
				}
				pm.report(true)
				gt.lat.observe(time.Since(t1))
				attempts = append(attempts, a)
				if c.model == "" {
					c.model = model
				}
				return &Response{
					Content: c.content, Reasoning: c.reasoning, FinishReason: c.finish, Provider: s.Provider, Model: c.model,
					Usage: c.usage, Attempts: attempts, Latency: Duration{time.Since(start)},
				}, nil
			}

			if ctx.Err() != nil {
				return nil, ctx.Err() // 호출자 취소는 provider 실패로 세지 않는다
			}
			var ce *callError
			errors.As(err, &ce)
			if ce != nil && !ce.providerFault() {
				pm.reportNeutral()
			} else {
				pm.report(false)
			}
			a.setError(err)
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

// maskBody 는 provider 규칙에 따라 body 의 messages 를 가리고 가린 건수를 돌려준다.
func (g *Gateway) maskBody(p *ProviderConfig, body map[string]any) int {
	if !(p.Mask || (p.External && g.cfg.Mask.External)) {
		return 0
	}
	msgs, _ := body["messages"].([]Message)
	out, n := g.masker.maskMessages(msgs)
	body["messages"] = out
	return n
}

// autoSpill 은 step i 가 spill=auto 일 때, 다음으로 시도할 step 에서 처리하는 편이 빨리 끝날 것으로 보이면
// 그 사유를 돌려준다(아니면 ""). 다음 step 은 외부 차단으로 건너뛸 step 을 뺀 첫 step 이다.
func (g *Gateway) autoSpill(route *RouteConfig, i int, noExternal bool) string {
	if route.Steps[i].Spill != "auto" {
		return ""
	}
	for j := i + 1; j < len(route.Steps); j++ {
		np := g.cfg.Providers[route.Steps[j].Provider]
		if np.External && noExternal {
			continue
		}
		ng := g.gates[route.Steps[j].Provider]
		if !ng.admits() {
			return "" // 넘겨 봐야 바로 거절될 곳이면 여기서 기다린다
		}
		here, ok1 := g.gates[route.Steps[i].Provider].expectedFinish()
		next, ok2 := ng.expectedFinish()
		if ok1 && ok2 && here > next {
			g.gates[route.Steps[i].Provider].stats.EarlySpills.Add(1)
			return fmt.Sprintf("%s: 예상 완료 %s > %s %s", ErrWaitTooLong, here.Round(time.Millisecond), route.Steps[j].Provider, next.Round(time.Millisecond))
		}
		return ""
	}
	return ""
}

// stepMaxWait 는 i 번째 step 의 max_wait 다. 뒤에 실제로 시도할 step 이 없으면(마지막 step 이거나,
// 남은 step 이 모두 외부 차단으로 건너뛸 external provider 면) 넘길 곳이 없으므로 적용하지 않는다.
func (g *Gateway) stepMaxWait(route *RouteConfig, i int, noExternal bool) time.Duration {
	for j := i + 1; j < len(route.Steps); j++ {
		if !(noExternal && g.cfg.Providers[route.Steps[j].Provider].External) {
			return route.Steps[i].MaxWait.Duration
		}
	}
	return 0
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
	EarlySpills                                                int64 // 예상 대기가 max_wait 를 넘어 바로 다음 step 으로 넘긴 수
	InFlight, Waiting                                          int64
	LatencyP50MS, LatencyP95MS                                 int64 // 최근 성공 호출 256건의 소요 시간
	EstWaitMS                                                  int64 // 지금 들어오면 예상되는 대기 시간
}

// Stats 는 프로바이더별 상태 스냅샷을 돌려준다.
func (g *Gateway) Stats() map[string]ProviderStat {
	out := make(map[string]ProviderStat, len(g.gates))
	for name, gt := range g.gates {
		p50, p95 := gt.lat.percentiles()
		out[name] = ProviderStat{
			Calls: gt.stats.Calls.Load(), OK: gt.stats.OK.Load(), Fail: gt.stats.Fail.Load(),
			QueueRejects: gt.stats.QueueRejects.Load(), QueueTimeouts: gt.stats.QueueTimeouts.Load(),
			BreakerSkips: gt.stats.BreakerSkips.Load(), EarlySpills: gt.stats.EarlySpills.Load(),
			InFlight: gt.inFlight.Load(), Waiting: gt.waiting.Load(),
			LatencyP50MS: p50.Milliseconds(), LatencyP95MS: p95.Milliseconds(),
			EstWaitMS: gt.estimateWait().Milliseconds(),
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
