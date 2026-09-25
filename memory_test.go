package llmgw

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

func TestByteSizeParse(t *testing.T) {
	for in, want := range map[string]ByteSize{
		"64MiB": 64 << 20, "512KB": 512e3, "1GiB": 1 << 30, "1.5GB": 1.5e9, "1048576": 1 << 20, "8 MiB": 8 << 20,
	} {
		var b ByteSize
		if err := b.UnmarshalText([]byte(in)); err != nil || b != want {
			t.Errorf("%q → %d (%v), want %d", in, b, err, want)
		}
	}
	for _, bad := range []string{"", "MiB", "-1MiB", "10XB"} {
		var b ByteSize
		if err := b.UnmarshalText([]byte(bad)); err == nil {
			t.Errorf("%q 는 거절해야 함", bad)
		}
	}
}

// 캐시는 항목 수뿐 아니라 바이트로도 상한을 지킨다.
func TestCacheBoundedByBytes(t *testing.T) {
	g := &Gateway{cfg: &Config{CacheMaxBytes: 10 << 10}, cache: map[string]cacheEntry{}}
	big := strings.Repeat("가", 1000) // 3000 바이트
	g.mu.Lock()
	for i := range 20 {
		g.putCache(fmt.Sprint(i), &Response{Content: big}, time.Hour)
	}
	g.putCache("huge", &Response{Content: strings.Repeat("x", 20<<10)}, time.Hour) // 혼자 상한을 넘으면 캐시 안 함
	_, newest := g.cache["19"]
	_, oldest := g.cache["0"]
	_, huge := g.cache["huge"]
	var sum int64
	for _, e := range g.cache {
		sum += e.size
	}
	bytes := g.cacheBytes
	g.mu.Unlock()
	if bytes > 10<<10 || sum != bytes || !newest || oldest || huge {
		t.Fatalf("cacheBytes %d (합계 %d, 상한 %d) newest=%v oldest=%v huge=%v", bytes, sum, 10<<10, newest, oldest, huge)
	}
}

// 너무 큰 응답은 끝까지 읽지 않고 실패로 보고 다음 step 으로 넘긴다.
func TestMaxResponseBytes(t *testing.T) {
	big := newFake(t, func(_ int64, w http.ResponseWriter) { okJSON(w, strings.Repeat("x", 4<<10)) })
	ok := newFake(t, func(_ int64, w http.ResponseWriter) { okJSON(w, "ok") })
	pb := prov(big.srv.URL)
	pb.MaxResponseBytes = 1 << 10
	g := build(t, &Config{
		Providers: map[string]*ProviderConfig{"big": pb, "ok": prov(ok.srv.URL)},
		Routes:    map[string]*RouteConfig{"r": {Steps: []StepConfig{{Provider: "big"}, {Provider: "ok"}}}},
	})
	r, err := g.Call(context.Background(), userReq("r", "x"))
	if err != nil || r.Provider != "ok" || r.Attempts[0].Error != "응답이 너무 큼" {
		t.Fatalf("%v %+v", err, r)
	}
}

// 스트림 한 줄이 상한을 넘으면(줄바꿈 없이 큰 데이터) 버퍼를 키우지 않고 끊는다.
func TestStreamLineLimit(t *testing.T) {
	f := newFake(t, func(_ int64, w http.ResponseWriter) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {}\n\n")
		fmt.Fprint(w, "data: "+strings.Repeat("x", 200<<10)) // 줄바꿈 없는 200KiB
	})
	p := prov(f.srv.URL)
	p.MaxResponseBytes = 64 << 10
	g := build(t, &Config{
		Providers: map[string]*ProviderConfig{"a": p},
		Routes:    map[string]*RouteConfig{"r": {Steps: []StepConfig{{Provider: "a"}}}},
	})
	resp, err := g.Stream(context.Background(), userReq("r", "x"), func([]byte) error { return nil })
	if err == nil || resp.Attempts[0].Error != "스트림 줄이 너무 김" {
		t.Fatalf("%v %+v", err, resp)
	}
}

// server.max_inflight 를 넘는 요청은 본문을 읽기 전에 503 으로 돌려보낸다.
func TestServerMaxInflight(t *testing.T) {
	release := make(chan struct{})
	f := newFake(t, func(_ int64, w http.ResponseWriter) { <-release; okJSON(w, "ok") })
	c := &Config{
		Server:    ServerConfig{MaxInflight: 1},
		Providers: map[string]*ProviderConfig{"a": prov(f.srv.URL)},
		Routes:    map[string]*RouteConfig{"r": {Steps: []StepConfig{{Provider: "a"}}}},
	}
	g := build(t, c)
	s := serve(t, g)
	done := make(chan int)
	go func() {
		code, _ := post(t, s.URL+"/v1/chat/completions", "", `{"model":"r","messages":[{"role":"user","content":"1"}]}`)
		done <- code
	}()
	for g.MemoryStats().Inflight == 0 {
		time.Sleep(time.Millisecond)
	}
	code, _ := post(t, s.URL+"/v1/chat/completions", "", `{"model":"r","messages":[{"role":"user","content":"2"}]}`)
	close(release)
	if first := <-done; code != 503 || first != 200 {
		t.Fatalf("두 번째 요청은 503 이어야 함: %d (첫 요청 %d)", code, first)
	}
	if st := g.MemoryStats(); st.Shed != 1 || st.Inflight != 0 {
		t.Fatalf("memory stats %+v", st)
	}
}

func TestMaxRequestBytes413(t *testing.T) {
	f := newFake(t, func(_ int64, w http.ResponseWriter) { okJSON(w, "ok") })
	g := build(t, &Config{
		Server:    ServerConfig{MaxRequestBytes: 1 << 10},
		Providers: map[string]*ProviderConfig{"a": prov(f.srv.URL)},
		Routes:    map[string]*RouteConfig{"r": {Steps: []StepConfig{{Provider: "a"}}}},
	})
	s := serve(t, g)
	code, _ := post(t, s.URL+"/v1/chat/completions", "", `{"model":"r","messages":[{"role":"user","content":"`+strings.Repeat("x", 4<<10)+`"}]}`)
	if code != 413 || f.calls.Load() != 0 {
		t.Fatalf("큰 본문은 413: %d (upstream %d회)", code, f.calls.Load())
	}
}

// 모르는 키(오타, 다른 표에 잘못 들어간 키)는 설정을 읽을 때 에러로 알린다.
func TestConfigRejectsUnknownKeys(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		p := dir + "/" + name
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	base := "[providers.a]\nbase_url = \"http://x\"\nmodel = \"m\"\n"
	// [server] 아래에 적어 server.cache_max_bytes 가 된 경우
	if _, err := LoadConfig(write("a.toml", "[server]\ncache_max_bytes = \"1MiB\"\n"+base)); err == nil || !strings.Contains(err.Error(), "server.cache_max_bytes") {
		t.Fatalf("잘못 놓인 키를 잡아야 함: %v", err)
	}
	if _, err := LoadConfig(write("b.yaml", "providers:\n  a:\n    base_url: http://x\n    model: m\n    max_concurency: 3\n")); err == nil || !strings.Contains(err.Error(), "max_concurency") {
		t.Fatalf("오타 키를 잡아야 함: %v", err)
	}
	if _, err := LoadConfig(write("c.toml", "cache_max_bytes = \"1MiB\"\n"+base)); err != nil {
		t.Fatalf("올바른 설정: %v", err)
	}
}
