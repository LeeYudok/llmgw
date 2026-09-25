package llmgw

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// provider extra 의 안쪽 map 을 요청끼리 공유하면 동시 요청에서 경쟁·섞임이 생긴다(-race 로 잡힌다).
func TestExtraNotSharedBetweenRequests(t *testing.T) {
	f := newFake(t, func(_ int64, w http.ResponseWriter) { okJSON(w, "ok") })
	p := prov(f.srv.URL)
	p.Reasoning = "chat_template"
	p.Extra = map[string]any{"chat_template_kwargs": map[string]any{"top_k": 20}}
	g := build(t, &Config{
		Providers: map[string]*ProviderConfig{"q": p},
		Routes: map[string]*RouteConfig{
			"on":  {Steps: []StepConfig{{Provider: "q", Reasoning: "on"}}},
			"off": {Steps: []StepConfig{{Provider: "q", Reasoning: "off"}}},
		},
	})
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() {
			route := map[bool]string{true: "on", false: "off"}[i%2 == 0]
			g.Call(context.Background(), userReq(route, fmt.Sprint(i)))
		})
	}
	wg.Wait()
	if _, leaked := p.Extra["chat_template_kwargs"].(map[string]any)["enable_thinking"]; leaked {
		t.Fatal("요청의 enable_thinking 이 provider 설정에 남으면 안 됨")
	}
	for _, b := range f.bodies {
		kw := b["chat_template_kwargs"].(map[string]any)
		want := b["max_tokens"].(float64) >= 4096 // 추론을 켠 요청만 max_tokens 가 올라간다
		if kw["enable_thinking"] != want || kw["top_k"] != float64(20) {
			t.Fatalf("요청마다 자기 설정이어야 함: %v", b)
		}
	}
}

// 대기 마감 뒤의 분당 한도 자리는 예약하지 않는다 — 포기한 요청이 자리를 붙잡아 뒤 요청을 막으면 안 된다.
func TestRateSlotBeyondDeadlineNotReserved(t *testing.T) {
	g := newGate(&ProviderConfig{RPM: 60, QueueTimeout: Duration{300 * time.Millisecond}}) // 간격 1초
	rel, err := g.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	rel()
	var wg sync.WaitGroup
	for range 5 {
		wg.Go(func() {
			t0 := time.Now()
			if _, err := g.acquire(context.Background()); err != ErrQueueTimeout {
				t.Errorf("1초 뒤 자리는 300ms 안에 못 쓰므로 거절: %v", err)
			}
			if d := time.Since(t0); d > 100*time.Millisecond {
				t.Errorf("못 쓸 자리를 기다리지 말고 바로 거절해야 함: %s", d)
			}
		})
	}
	wg.Wait()
	g.mu.Lock()
	ahead := time.Until(g.next)
	g.mu.Unlock()
	if ahead > time.Second {
		t.Fatalf("거절된 요청이 자리를 예약해 다음 발송 시각이 밀림: %s 뒤", ahead)
	}
}

// rpm 이 없어도 429 Retry-After 는 같은 provider 의 다음 요청을 늦춘다.
func TestRetryAfterWithoutRPM(t *testing.T) {
	g := newGate(&ProviderConfig{QueueTimeout: Duration{time.Second}})
	g.deferRate(time.Now().Add(300 * time.Millisecond))
	if g.estimateWait() < 250*time.Millisecond {
		t.Fatalf("예상 대기에 Retry-After 가 보여야 함: %s", g.estimateWait())
	}
	t0 := time.Now()
	rel, err := g.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	rel()
	if d := time.Since(t0); d < 250*time.Millisecond {
		t.Fatalf("Retry-After 시각까지 기다려야 함: %s", d)
	}
}

// 호출자가 느리게 읽는 것은 provider 의 idle 로 세지 않는다.
func TestSlowStreamConsumerNotIdle(t *testing.T) {
	f := newFake(t, func(_ int64, w http.ResponseWriter) {
		w.Header().Set("Content-Type", "text/event-stream")
		for range 3 { // 한 줄씩 흘려보내 다음 줄을 실제로 네트워크에서 읽게 한다
			fmt.Fprint(w, "data: {}\n\n")
			w.(http.Flusher).Flush()
			time.Sleep(20 * time.Millisecond)
		}
		fmt.Fprint(w, "data: [DONE]\n\n")
	})
	p := prov(f.srv.URL)
	p.StreamIdleTimeout, p.BreakerFailures, p.BreakerCooldown = Duration{100 * time.Millisecond}, 1, Duration{time.Minute}
	g := build(t, &Config{
		Providers: map[string]*ProviderConfig{"a": p},
		Routes:    map[string]*RouteConfig{"r": {Steps: []StepConfig{{Provider: "a"}}}},
	})
	_, err := g.Stream(context.Background(), userReq("r", "x"), func([]byte) error { time.Sleep(150 * time.Millisecond); return nil })
	if err != nil {
		t.Fatalf("느린 호출자 때문에 idle 타임아웃이 나면 안 됨: %v", err)
	}
	if g.gates["a"].breakerBlocked() {
		t.Fatal("느린 호출자 때문에 브레이커가 열리면 안 됨")
	}
}

// 캐시·합치기로 받은 응답은 upstream 을 다시 부르지 않았으므로 토큰을 세지 않는다.
func TestCachedResponseTokensNotCounted(t *testing.T) {
	f := newFake(t, func(_ int64, w http.ResponseWriter) { okJSON(w, "ok") })
	t.Setenv("KEY_A", "k")
	g := build(t, &Config{
		Providers: map[string]*ProviderConfig{"p": prov(f.srv.URL)},
		Routes:    map[string]*RouteConfig{"r": {CacheTTL: Duration{time.Minute}, Steps: []StepConfig{{Provider: "p"}}}},
		Clients:   map[string]*ClientConfig{"app": {APIKeyEnv: "KEY_A"}},
	})
	for range 3 {
		req := userReq("r", "같은 질문")
		req.Client = "app"
		if _, err := g.Call(context.Background(), req); err != nil {
			t.Fatal(err)
		}
	}
	if st := g.ClientStats()["app"]; st.PromptTokens != 3 || st.Cached != 2 {
		t.Fatalf("upstream 1회분(3)만 세야 함: %+v", st)
	}
}

// stream_usage 면 include_usage 를 요청하고, 마지막 청크의 usage 를 기록한다.
func TestStreamUsageRecorded(t *testing.T) {
	f := newFake(t, func(_ int64, w http.ResponseWriter) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n")
		fmt.Fprint(w, "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":7}}\n\n")
		fmt.Fprint(w, "data: [DONE]\n\n")
	})
	p := prov(f.srv.URL)
	p.StreamUsage = true
	g := build(t, &Config{
		Providers: map[string]*ProviderConfig{"a": p},
		Routes:    map[string]*RouteConfig{"r": {Steps: []StepConfig{{Provider: "a"}}}},
	})
	resp, err := g.Stream(context.Background(), userReq("r", "x"), func([]byte) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if so, _ := f.bodies[0]["stream_options"].(map[string]any); so["include_usage"] != true {
		t.Fatalf("stream_options.include_usage 를 보내야 함: %v", f.bodies[0])
	}
	if resp.Usage.PromptTokens != 5 || resp.Usage.CompletionTokens != 7 {
		t.Fatalf("usage %+v", resp.Usage)
	}
}

// passthrough 경로로 base_url 밖에 나가거나 쿼리를 끼워 넣을 수 없어야 한다.
func TestPassthroughPathTraversal(t *testing.T) {
	var got atomic.Value
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.ReadAll(r.Body)
		got.Store(r.RequestURI)
		w.Write([]byte("{}"))
	}))
	t.Cleanup(up.Close)
	p := prov(up.URL + "/v1")
	p.Passthrough = true
	g := build(t, &Config{
		Providers: map[string]*ProviderConfig{"u": p},
		Routes:    map[string]*RouteConfig{"r": {Steps: []StepConfig{{Provider: "u"}}}},
	})
	s := serve(t, g)
	get := func(path string) int {
		resp, err := http.Get(s.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if code := get("/passthrough/u/parse/%2E%2E%2F%2E%2E%2Fadmin%3Fx=1"); code != 400 {
		t.Fatalf("인코딩된 .. 은 거절해야 함: %d (upstream 에 %v)", code, got.Load())
	}
	if code := get("/passthrough/u/files/a%3Fb?q=1"); code != 200 || got.Load() != "/v1/files/a%3Fb?q=1" {
		t.Fatalf("인코딩된 경로는 그대로 전달해야 함: %d %v", code, got.Load())
	}
}

// 브레이커는 쿨다운 뒤 시험 요청 한 건만 보내고, 실패하면 바로 다시 연다.
func TestBreakerHalfOpen(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	f := newFake(t, func(_ int64, w http.ResponseWriter) {
		time.Sleep(50 * time.Millisecond)
		if fail.Load() {
			http.Error(w, "down", 500)
			return
		}
		okJSON(w, "ok")
	})
	p := prov(f.srv.URL)
	p.BreakerFailures, p.BreakerCooldown = 1, Duration{150 * time.Millisecond}
	g := build(t, &Config{
		Providers: map[string]*ProviderConfig{"a": p},
		Routes:    map[string]*RouteConfig{"r": {Steps: []StepConfig{{Provider: "a"}}}},
	})
	g.Call(context.Background(), userReq("r", "trip")) // 1회 실패로 열림
	time.Sleep(200 * time.Millisecond)                 // 쿨다운이 지나 half-open

	var wg sync.WaitGroup
	before := f.calls.Load()
	for i := range 5 {
		wg.Go(func() { g.Call(context.Background(), userReq("r", fmt.Sprint("probe", i))) })
	}
	wg.Wait()
	if n := f.calls.Load() - before; n != 1 {
		t.Fatalf("half-open 에서는 시험 요청 1건만 보내야 함: %d건", n)
	}
	if !g.gates["a"].breakerBlocked() {
		t.Fatal("시험 요청이 실패하면 바로 다시 열려야 함")
	}

	fail.Store(false)
	time.Sleep(200 * time.Millisecond)
	if _, err := g.Call(context.Background(), userReq("r", "recover")); err != nil {
		t.Fatalf("시험 요청 성공: %v", err)
	}
	if _, err := g.Call(context.Background(), userReq("r", "after")); err != nil || g.gates["a"].breakerBlocked() {
		t.Fatalf("시험 요청이 성공하면 닫혀야 함: %v", err)
	}
}
