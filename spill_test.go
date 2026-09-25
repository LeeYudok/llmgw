package llmgw

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestLatencyWindow(t *testing.T) {
	var w latencyWindow
	if p50, p95 := w.percentiles(); p50 != 0 || p95 != 0 || w.mean() != 0 {
		t.Fatal("빈 기록은 0")
	}
	for i := 1; i <= 100; i++ {
		w.observe(time.Duration(i) * time.Millisecond)
	}
	p50, p95 := w.percentiles()
	if p50 != 50*time.Millisecond || p95 != 95*time.Millisecond || w.mean() != 50500*time.Microsecond {
		t.Fatalf("p50 %s p95 %s mean %s", p50, p95, w.mean())
	}
	for range 300 { // 링 버퍼가 넘치면 오래된 기록이 빠진다
		w.observe(time.Second)
	}
	if w.mean() != time.Second {
		t.Fatalf("최근 256건만 남아야 함: mean %s", w.mean())
	}
}

func TestEstimateWait(t *testing.T) {
	g := newGate(&ProviderConfig{RPM: 60, MaxConcurrency: 2, QueueTimeout: Duration{time.Second}})
	if g.estimateWait() != 0 {
		t.Fatal("빈 게이트는 대기 0")
	}
	g.mu.Lock()
	g.next = time.Now().Add(3 * time.Second) // 분당 한도로 3초 뒤까지 예약돼 있음
	g.mu.Unlock()
	if d := g.estimateWait(); d < 2900*time.Millisecond || d > 3*time.Second {
		t.Fatalf("분당 한도 대기 %s", d)
	}
	g.mu.Lock()
	g.next = time.Time{}
	g.mu.Unlock()
	g.slots <- struct{}{}
	g.slots <- struct{}{} // 동시 처리 2칸이 모두 찼다
	if d := g.estimateWait(); d != 0 {
		t.Fatalf("소요 시간 기록이 없으면 동시 처리 대기는 0 으로 본다: %s", d)
	}
	g.lat.observe(400 * time.Millisecond)
	if d := g.estimateWait(); d != 200*time.Millisecond { // 앞에 1건 ÷ 2칸 × 400ms
		t.Fatalf("동시 처리 대기 %s", d)
	}
}

func TestMaxWaitSpillsEarly(t *testing.T) {
	a := newFake(t, func(_ int64, w http.ResponseWriter) { okJSON(w, "a") })
	b := newFake(t, func(_ int64, w http.ResponseWriter) { okJSON(w, "b") })
	pa := prov(a.srv.URL)
	pa.RPM, pa.QueueTimeout = 60, Duration{5 * time.Second} // 요청 간격 1초
	g := build(t, &Config{
		Providers: map[string]*ProviderConfig{"a": pa, "b": prov(b.srv.URL)},
		Routes: map[string]*RouteConfig{
			"r":    {Steps: []StepConfig{{Provider: "a", MaxWait: Duration{200 * time.Millisecond}}, {Provider: "b"}}},
			"last": {Steps: []StepConfig{{Provider: "a", MaxWait: Duration{200 * time.Millisecond}}}},
		},
	})
	if r, err := g.Call(context.Background(), userReq("r", "1")); err != nil || r.Provider != "a" {
		t.Fatalf("첫 요청은 a: %v %v", r, err)
	}
	t0 := time.Now()
	r, err := g.Call(context.Background(), userReq("r", "2"))
	if err != nil || r.Provider != "b" {
		t.Fatalf("a 는 1초 기다려야 하므로 b 로 넘겨야 함: %v %v", r, err)
	}
	if d := time.Since(t0); d > 150*time.Millisecond {
		t.Fatalf("예상 대기가 길면 기다리지 않고 바로 넘겨야 함: %s", d)
	}
	if !strings.Contains(r.Attempts[0].Error, ErrWaitTooLong.Error()) || g.Stats()["a"].EarlySpills != 1 {
		t.Fatalf("attempt %+v stats %+v", r.Attempts[0], g.Stats()["a"])
	}
	// 마지막 step 은 넘길 곳이 없으므로 max_wait 를 적용하지 않고 기다린다.
	if r, err := g.Call(context.Background(), userReq("last", "3")); err != nil || r.Provider != "a" {
		t.Fatalf("마지막 step 은 기다려서라도 처리해야 함: %v %v", r, err)
	}
}

func TestAutoSpill(t *testing.T) {
	release := make(chan struct{})
	a := newFake(t, func(n int64, w http.ResponseWriter) {
		if n == 1 {
			<-release
		}
		okJSON(w, "a")
	})
	b := newFake(t, func(_ int64, w http.ResponseWriter) { okJSON(w, "b") })
	pa := prov(a.srv.URL)
	pa.MaxConcurrency = 1
	g := build(t, &Config{
		Providers: map[string]*ProviderConfig{"a": pa, "b": prov(b.srv.URL)},
		Routes:    map[string]*RouteConfig{"r": {Steps: []StepConfig{{Provider: "a", Spill: "auto"}, {Provider: "b"}}}},
	})
	g.gates["a"].lat.observe(300 * time.Millisecond)
	done := make(chan struct{})
	go func() { g.Call(context.Background(), userReq("r", "slow")); close(done) }()
	for len(g.gates["a"].slots) == 0 { // a 의 유일한 슬롯이 찰 때까지
		time.Sleep(time.Millisecond)
	}
	// b 기록이 없으면 비교할 수 없으므로 넘기지 않는다(a 에서 기다린다).
	if why := g.autoSpill(g.cfg.Routes["r"], 0, false); why != "" {
		t.Fatalf("다음 step 기록이 없으면 넘기지 않아야 함: %s", why)
	}
	g.gates["b"].lat.observe(50 * time.Millisecond)
	r, err := g.Call(context.Background(), userReq("r", "fast"))
	if err != nil || r.Provider != "b" {
		t.Fatalf("a 예상 완료(600ms) > b(50ms) 면 b 로: %v %v", r, err)
	}
	if !strings.Contains(r.Attempts[0].Error, "예상 완료") {
		t.Fatalf("attempt %+v", r.Attempts[0])
	}
	// 다음 step 이 요청을 받을 수 없는 상태(브레이커 열림)면 넘기지 않는다.
	g.gates["b"].mu.Lock()
	g.gates["b"].breakerFailures, g.gates["b"].openUntil = 1, time.Now().Add(time.Minute)
	g.gates["b"].mu.Unlock()
	if why := g.autoSpill(g.cfg.Routes["r"], 0, false); why != "" {
		t.Fatalf("받을 수 없는 step 으로 넘기면 안 됨: %s", why)
	}
	close(release)
	<-done
	// external 차단으로 건너뛸 다음 step 과는 비교하지 않는다.
	g.cfg.Providers["b"].External = true
	if why := g.autoSpill(g.cfg.Routes["r"], 0, true); why != "" {
		t.Fatalf("차단된 step 으로 넘기면 안 됨: %s", why)
	}
}

func TestSpillConfigValidation(t *testing.T) {
	c := &Config{
		Providers: map[string]*ProviderConfig{"a": {BaseURL: "http://x", Model: "m"}},
		Routes:    map[string]*RouteConfig{"r": {Steps: []StepConfig{{Provider: "a", Spill: "fast"}}}},
	}
	c.applyDefaults()
	if err := c.validate(); err == nil || !strings.Contains(err.Error(), "spill") {
		t.Fatalf("잘못된 spill 값을 잡아야 함: %v", err)
	}
}
