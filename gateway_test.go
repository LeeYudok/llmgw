package llmgw

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeLLM 은 요청 body 를 기록하고 handler 가 정한 응답을 돌려주는 OpenAI 호환 가짜 서버다.
type fakeLLM struct {
	srv    *httptest.Server
	calls  atomic.Int64
	mu     sync.Mutex
	bodies []map[string]any
}

func newFake(t *testing.T, h func(n int64, w http.ResponseWriter)) *fakeLLM {
	t.Helper()
	f := &fakeLLM{}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b map[string]any
		raw, _ := io.ReadAll(r.Body)
		json.Unmarshal(raw, &b)
		f.mu.Lock()
		f.bodies = append(f.bodies, b)
		f.mu.Unlock()
		h(f.calls.Add(1), w)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func okJSON(w http.ResponseWriter, content string) {
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"model":"m","choices":[{"finish_reason":"stop","message":{"content":%q}}],"usage":{"prompt_tokens":3,"completion_tokens":2}}`, content)
}

func prov(url string) *ProviderConfig {
	return &ProviderConfig{BaseURL: url, Model: "m"}
}

func build(t *testing.T, c *Config) *Gateway {
	t.Helper()
	c.applyDefaults()
	if err := c.validate(); err != nil {
		t.Fatal(err)
	}
	g, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func userReq(route, text string) Request {
	return Request{Route: route, Messages: []Message{{Role: "user", Content: text}}}
}

func TestFallbackAfterRetriesExhausted(t *testing.T) {
	bad := newFake(t, func(_ int64, w http.ResponseWriter) { http.Error(w, "boom", 503) })
	good := newFake(t, func(_ int64, w http.ResponseWriter) { okJSON(w, "hello") })
	g := build(t, &Config{
		Providers: map[string]*ProviderConfig{"a": prov(bad.srv.URL), "b": prov(good.srv.URL)},
		Routes:    map[string]*RouteConfig{"r": {Steps: []StepConfig{{Provider: "a", Retries: 1}, {Provider: "b"}}}},
	})
	resp, err := g.Call(context.Background(), userReq("r", "x"))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Provider != "b" || resp.Content != "hello" {
		t.Fatalf("got %+v", resp)
	}
	if bad.calls.Load() != 2 { // 첫 시도 + 재시도 1회
		t.Fatalf("a 호출 %d회, 2회여야 함", bad.calls.Load())
	}
	if len(resp.Attempts) != 3 || resp.Attempts[0].Status != 503 {
		t.Fatalf("attempts %+v", resp.Attempts)
	}
}

func TestRetryAfterRespectedThenSuccess(t *testing.T) {
	f := newFake(t, func(n int64, w http.ResponseWriter) {
		if n == 1 {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "slow down", 429)
			return
		}
		okJSON(w, "ok")
	})
	g := build(t, &Config{
		Providers: map[string]*ProviderConfig{"a": prov(f.srv.URL)},
		Routes:    map[string]*RouteConfig{"r": {Steps: []StepConfig{{Provider: "a", Retries: 1}}}},
	})
	t0 := time.Now()
	if _, err := g.Call(context.Background(), userReq("r", "x")); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(t0); d < 900*time.Millisecond {
		t.Fatalf("Retry-After 1s 를 안 기다림: %s", d)
	}
}

func TestLongRetryAfterSkipsToNextStep(t *testing.T) {
	a := newFake(t, func(_ int64, w http.ResponseWriter) {
		w.Header().Set("Retry-After", "60")
		http.Error(w, "quota", 429)
	})
	b := newFake(t, func(_ int64, w http.ResponseWriter) { okJSON(w, "fallback") })
	g := build(t, &Config{
		Providers: map[string]*ProviderConfig{"a": prov(a.srv.URL), "b": prov(b.srv.URL)},
		Routes: map[string]*RouteConfig{"r": {Steps: []StepConfig{
			{Provider: "a", Retries: 3, MaxRetryWait: Duration{2 * time.Second}}, {Provider: "b"},
		}}},
	})
	t0 := time.Now()
	resp, err := g.Call(context.Background(), userReq("r", "x"))
	if err != nil || resp.Provider != "b" {
		t.Fatalf("resp=%+v err=%v", resp, err)
	}
	if time.Since(t0) > time.Second || a.calls.Load() != 1 {
		t.Fatalf("60s 대기 대신 즉시 폴백해야 함 (a 호출 %d회)", a.calls.Load())
	}
}

func TestValidationFailureFallsBack(t *testing.T) {
	a := newFake(t, func(_ int64, w http.ResponseWriter) { okJSON(w, "") })
	b := newFake(t, func(_ int64, w http.ResponseWriter) { okJSON(w, `{"label":"bullish"}`) })
	g := build(t, &Config{
		Providers: map[string]*ProviderConfig{"a": prov(a.srv.URL), "b": prov(b.srv.URL)},
		Routes: map[string]*RouteConfig{"r": {
			Validate: Validation{RequireJSON: true},
			Steps:    []StepConfig{{Provider: "a", Retries: 3}, {Provider: "b"}},
		}},
	})
	resp, err := g.Call(context.Background(), userReq("r", "x"))
	if err != nil || resp.Provider != "b" {
		t.Fatalf("resp=%+v err=%v", resp, err)
	}
	if a.calls.Load() != 1 {
		t.Fatalf("검증 실패는 재시도 없이 넘겨야 함: a %d회", a.calls.Load())
	}
}

func TestAllFailedReturnsChainError(t *testing.T) {
	a := newFake(t, func(_ int64, w http.ResponseWriter) { http.Error(w, "bad request", 400) })
	g := build(t, &Config{
		Providers: map[string]*ProviderConfig{"a": prov(a.srv.URL)},
		Routes:    map[string]*RouteConfig{"r": {Steps: []StepConfig{{Provider: "a", Retries: 3}}}},
	})
	_, err := g.Call(context.Background(), userReq("r", "x"))
	var ce *ChainError
	if !errors.As(err, &ce) || !errors.Is(err, ErrAllFailed) || len(ce.Attempts) != 1 {
		t.Fatalf("err=%v", err)
	}
}

func TestQueueFullSpillsToNextStep(t *testing.T) {
	release := make(chan struct{})
	slow := newFake(t, func(_ int64, w http.ResponseWriter) { <-release; okJSON(w, "slow") })
	fast := newFake(t, func(_ int64, w http.ResponseWriter) { okJSON(w, "fast") })
	ps := prov(slow.srv.URL)
	ps.MaxConcurrency, ps.MaxQueue = 1, 1
	g := build(t, &Config{
		Providers: map[string]*ProviderConfig{"slow": ps, "fast": prov(fast.srv.URL)},
		Routes:    map[string]*RouteConfig{"r": {Steps: []StepConfig{{Provider: "slow"}, {Provider: "fast"}}}},
	})
	// 1번: slow 에서 처리 중 / 2번: slow 대기열 / 3~5번: 대기열 가득 → fast
	results := make(chan string, 5)
	var wg sync.WaitGroup
	for i := range 5 {
		wg.Go(func() {
			resp, err := g.Call(context.Background(), userReq("r", fmt.Sprint(i)))
			if err != nil {
				results <- "err:" + err.Error()
				return
			}
			results <- resp.Provider
		})
		time.Sleep(30 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()
	close(results)
	count := map[string]int{}
	for r := range results {
		count[r]++
	}
	if count["slow"] != 2 || count["fast"] != 3 {
		t.Fatalf("분포 %v, slow 2 / fast 3 이어야 함", count)
	}
	if st := g.Stats()["slow"]; st.QueueRejects != 3 {
		t.Fatalf("queue rejects %d", st.QueueRejects)
	}
}

func TestRPMSpacing(t *testing.T) {
	f := newFake(t, func(_ int64, w http.ResponseWriter) { okJSON(w, "ok") })
	p := prov(f.srv.URL)
	p.RPM = 600 // 100ms 간격
	g := build(t, &Config{
		Providers: map[string]*ProviderConfig{"a": p},
		Routes:    map[string]*RouteConfig{"r": {Steps: []StepConfig{{Provider: "a"}}}},
	})
	t0 := time.Now()
	var wg sync.WaitGroup
	for i := range 5 {
		wg.Go(func() { g.Call(context.Background(), userReq("r", fmt.Sprint(i))) })
	}
	wg.Wait()
	if d := time.Since(t0); d < 380*time.Millisecond {
		t.Fatalf("5건이 %s 만에 끝남 — 100ms 간격이면 400ms 이상이어야 함", d)
	}
}

func TestBreakerOpensAndSkips(t *testing.T) {
	a := newFake(t, func(_ int64, w http.ResponseWriter) { http.Error(w, "down", 500) })
	b := newFake(t, func(_ int64, w http.ResponseWriter) { okJSON(w, "ok") })
	pa := prov(a.srv.URL)
	pa.BreakerFailures, pa.BreakerCooldown = 2, Duration{time.Minute}
	g := build(t, &Config{
		Providers: map[string]*ProviderConfig{"a": pa, "b": prov(b.srv.URL)},
		Routes:    map[string]*RouteConfig{"r": {Steps: []StepConfig{{Provider: "a"}, {Provider: "b"}}}},
	})
	for i := range 4 {
		if _, err := g.Call(context.Background(), userReq("r", fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
	}
	if a.calls.Load() != 2 {
		t.Fatalf("브레이커가 2회 실패 후 열려야 함: a %d회", a.calls.Load())
	}
	if g.Stats()["a"].BreakerSkips != 2 {
		t.Fatalf("breaker skips %d", g.Stats()["a"].BreakerSkips)
	}
}

func TestSingleflightAndCache(t *testing.T) {
	f := newFake(t, func(_ int64, w http.ResponseWriter) { time.Sleep(100 * time.Millisecond); okJSON(w, "same") })
	g := build(t, &Config{
		Providers: map[string]*ProviderConfig{"a": prov(f.srv.URL)},
		Routes:    map[string]*RouteConfig{"r": {CacheTTL: Duration{time.Minute}, Steps: []StepConfig{{Provider: "a"}}}},
	})
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() { g.Call(context.Background(), userReq("r", "동일")) })
	}
	wg.Wait()
	resp, _ := g.Call(context.Background(), userReq("r", "동일"))
	if f.calls.Load() != 1 || !resp.Cached {
		t.Fatalf("같은 요청 11건이 %d회 호출됨(1회여야 함), cached=%v", f.calls.Load(), resp.Cached)
	}
}

func TestReasoningBodyMapping(t *testing.T) {
	f := newFake(t, func(_ int64, w http.ResponseWriter) { okJSON(w, "ok") })
	pq := prov(f.srv.URL)
	pq.Reasoning, pq.ReasoningMinTokens = "chat_template", 4096
	pu := prov(f.srv.URL)
	pu.Reasoning = "effort"
	g := build(t, &Config{
		Providers: map[string]*ProviderConfig{"q": pq, "u": pu},
		Routes: map[string]*RouteConfig{
			"q-on":  {Steps: []StepConfig{{Provider: "q", Reasoning: "on", MaxTokens: 500}}},
			"q-off": {Steps: []StepConfig{{Provider: "q", Reasoning: "off", MaxTokens: 500}}},
			"u-on":  {Steps: []StepConfig{{Provider: "u", Reasoning: "on"}}},
			"u-off": {Steps: []StepConfig{{Provider: "u", Reasoning: "off"}}},
		},
	})
	for _, r := range []string{"q-on", "q-off", "u-on", "u-off"} {
		if _, err := g.Call(context.Background(), userReq(r, r)); err != nil {
			t.Fatal(err)
		}
	}
	b := f.bodies
	if kw := b[0]["chat_template_kwargs"].(map[string]any); kw["enable_thinking"] != true || b[0]["max_tokens"].(float64) != 4096 {
		t.Fatalf("q-on body %v (추론 켜면 max_tokens 가 reasoning_min_tokens 로 올라야 함)", b[0])
	}
	if kw := b[1]["chat_template_kwargs"].(map[string]any); kw["enable_thinking"] != false || b[1]["max_tokens"].(float64) != 500 {
		t.Fatalf("q-off body %v", b[1])
	}
	if b[2]["reasoning_effort"] != "medium" {
		t.Fatalf("u-on body %v", b[2])
	}
	if _, ok := b[3]["reasoning_effort"]; ok {
		t.Fatalf("u-off 는 reasoning_effort 를 보내지 않아야 함: %v", b[3])
	}
}

func TestThinkTagStripped(t *testing.T) {
	f := newFake(t, func(_ int64, w http.ResponseWriter) { okJSON(w, "<think>고민</think>\n\n답") })
	g := build(t, &Config{
		Providers: map[string]*ProviderConfig{"a": prov(f.srv.URL)},
		Routes:    map[string]*RouteConfig{"r": {Steps: []StepConfig{{Provider: "a"}}}},
	})
	resp, err := g.Call(context.Background(), userReq("r", "x"))
	if err != nil || resp.Content != "답" || resp.Reasoning != "고민" {
		t.Fatalf("resp=%+v err=%v", resp, err)
	}
}

func TestExampleConfigsLoad(t *testing.T) {
	for _, name := range []string{"llmgw.example.toml", "llmgw.example.yaml"} {
		c, err := LoadConfig(name)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		nd := c.Routes["news-deep"]
		if len(nd.Steps) != 2 || nd.Steps[0].Provider != "upstage" || nd.Steps[1].Reasoning != "on" ||
			nd.CacheTTL.Duration != 6*time.Hour || c.Providers["upstage"].RPM != 50 {
			t.Fatalf("%s 해석 이상: %+v", name, nd)
		}
		wb, bw := c.Clients["wiki-builder"], c.Clients["batch-worker"]
		if wb == nil || bw == nil || len(wb.Passthrough) != 1 || wb.Passthrough[0] != "upstage" || wb.RPM != 30 ||
			bw.QueueTimeout.Duration != 5*time.Minute || !c.Providers["qwen"].JSONSchema || !c.Providers["upstage"].Passthrough ||
			c.LogPath == "" {
			t.Fatalf("%s clients/provider 옵션 해석 이상", name)
		}
	}
}

func TestConfigValidationErrors(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "bad.toml")
	os.WriteFile(p, []byte(`
[providers.a]
base_url = "http://x"
reasoning = "maybe"
[[routes.r.steps]]
provider = "nope"
`), 0o600)
	if _, err := LoadConfig(p); err == nil {
		t.Fatal("잘못된 설정이 통과함")
	}
}
