package llmgw

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func serve(t *testing.T, g *Gateway) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(g.Handler())
	t.Cleanup(s.Close)
	return s
}

func post(t *testing.T, url, key, body string) (int, map[string]any) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func twoClients(t *testing.T, upstream string) *Gateway {
	t.Helper()
	t.Setenv("KEY_A", "secret-a")
	t.Setenv("KEY_B", "secret-b")
	return build(t, &Config{
		Providers: map[string]*ProviderConfig{"p": prov(upstream)},
		Routes: map[string]*RouteConfig{
			"public":  {Steps: []StepConfig{{Provider: "p"}}},
			"private": {Steps: []StepConfig{{Provider: "p"}}},
		},
		Clients: map[string]*ClientConfig{
			"app-a": {APIKeyEnv: "KEY_A"},
			"app-b": {APIKeyEnv: "KEY_B", Routes: []string{"public"}, MaxConcurrency: 1, MaxQueue: 1},
		},
	})
}

func TestClientAuthAndRouteAllowlist(t *testing.T) {
	f := newFake(t, func(_ int64, w http.ResponseWriter) { okJSON(w, "hi") })
	s := serve(t, twoClients(t, f.srv.URL))
	body := func(route string) string {
		return fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"x"}]}`, route)
	}
	if code, _ := post(t, s.URL+"/v1/chat/completions", "", body("public")); code != 401 {
		t.Fatalf("키 없음 → 401 이어야 함, got %d", code)
	}
	if code, _ := post(t, s.URL+"/v1/chat/completions", "wrong", body("public")); code != 401 {
		t.Fatalf("틀린 키 → 401, got %d", code)
	}
	if code, _ := post(t, s.URL+"/v1/chat/completions", "secret-b", body("private")); code != 403 {
		t.Fatalf("허용 안 된 라우트 → 403, got %d", code)
	}
	code, out := post(t, s.URL+"/v1/chat/completions", "secret-a", body("private"))
	if code != 200 || out["llmgw"].(map[string]any)["provider"] != "p" {
		t.Fatalf("app-a private → 200, got %d %v", code, out)
	}
	// /v1/models 는 호출자가 쓸 수 있는 라우트만 보여준다.
	req, _ := http.NewRequest(http.MethodGet, s.URL+"/v1/models", nil)
	req.Header.Set("Authorization", "Bearer secret-b")
	resp, _ := http.DefaultClient.Do(req)
	var models struct{ Data []struct{ ID string } }
	json.NewDecoder(resp.Body).Decode(&models)
	resp.Body.Close()
	if len(models.Data) != 1 || models.Data[0].ID != "public" {
		t.Fatalf("app-b 모델 목록 %v", models.Data)
	}
}

func TestClientLimitReturns429(t *testing.T) {
	release := make(chan struct{})
	f := newFake(t, func(_ int64, w http.ResponseWriter) { <-release; okJSON(w, "ok") })
	g := twoClients(t, f.srv.URL)
	s := serve(t, g)
	codes := make(chan int, 4)
	var wg sync.WaitGroup
	for i := range 4 { // app-b: 동시 1 + 대기 1 → 나머지 2건 429
		wg.Go(func() {
			code, _ := post(t, s.URL+"/v1/chat/completions", "secret-b",
				fmt.Sprintf(`{"model":"public","messages":[{"role":"user","content":"%d"}]}`, i))
			codes <- code
		})
		time.Sleep(30 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()
	close(codes)
	count := map[int]int{}
	for c := range codes {
		count[c]++
	}
	if count[200] != 2 || count[429] != 2 {
		t.Fatalf("상태 분포 %v, 200×2 / 429×2 여야 함", count)
	}
	if st := g.ClientStats()["app-b"]; st.Rejected != 2 || st.OK != 2 {
		t.Fatalf("client stats %+v", st)
	}
}

func TestJSONSchemaDowngrade(t *testing.T) {
	f := newFake(t, func(_ int64, w http.ResponseWriter) { okJSON(w, `{"a":1}`) })
	strict := prov(f.srv.URL)
	strict.JSONSchema = true
	loose := prov(f.srv.URL)
	g := build(t, &Config{
		Providers: map[string]*ProviderConfig{"strict": strict, "loose": loose},
		Routes: map[string]*RouteConfig{
			"s": {Steps: []StepConfig{{Provider: "strict"}}},
			"l": {Steps: []StepConfig{{Provider: "loose"}}},
		},
	})
	rf := json.RawMessage(`{"type":"json_schema","json_schema":{"name":"x","strict":true,"schema":{"type":"object"}}}`)
	for _, r := range []string{"s", "l"} {
		req := userReq(r, "x")
		req.ResponseFormat = rf
		if _, err := g.Call(t.Context(), req); err != nil {
			t.Fatal(err)
		}
	}
	got0 := f.bodies[0]["response_format"].(map[string]any)
	got1 := f.bodies[1]["response_format"].(map[string]any)
	if got0["type"] != "json_schema" || got0["json_schema"] == nil {
		t.Fatalf("json_schema 지원 provider 엔 그대로: %v", got0)
	}
	if got1["type"] != "json_object" || got1["json_schema"] != nil {
		t.Fatalf("미지원 provider 엔 json_object 로: %v", got1)
	}
}

func TestJSONSchemaResponseValidated(t *testing.T) {
	a := newFake(t, func(_ int64, w http.ResponseWriter) { okJSON(w, "JSON 아님") })
	b := newFake(t, func(_ int64, w http.ResponseWriter) { okJSON(w, `{"ok":true}`) })
	g := build(t, &Config{
		Providers: map[string]*ProviderConfig{"a": prov(a.srv.URL), "b": prov(b.srv.URL)},
		Routes:    map[string]*RouteConfig{"r": {Steps: []StepConfig{{Provider: "a"}, {Provider: "b"}}}},
	})
	req := userReq("r", "x")
	req.ResponseFormat = json.RawMessage(`{"type":"json_schema","json_schema":{"name":"x","schema":{}}}`)
	resp, err := g.Call(t.Context(), req)
	if err != nil || resp.Provider != "b" {
		t.Fatalf("JSON 이 아니면 다음 step 으로: resp=%+v err=%v", resp, err)
	}
}

func TestPassthroughUsesProviderKeyAndGate(t *testing.T) {
	var gotAuth, gotPath, gotBody string
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth, gotPath = r.Header.Get("Authorization"), r.URL.Path+"?"+r.URL.RawQuery
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"parsed":true}`))
	}))
	t.Cleanup(up.Close)
	t.Setenv("UP_KEY", "provider-secret")
	t.Setenv("KEY_A", "secret-a")
	t.Setenv("KEY_B", "secret-b")
	p := prov(up.URL + "/v1")
	p.APIKeyEnv, p.Passthrough = "UP_KEY", true
	g := build(t, &Config{
		Providers: map[string]*ProviderConfig{"up": p},
		Routes:    map[string]*RouteConfig{"r": {Steps: []StepConfig{{Provider: "up"}}}},
		Clients: map[string]*ClientConfig{
			"app-a": {APIKeyEnv: "KEY_A", Passthrough: []string{"up"}},
			"app-b": {APIKeyEnv: "KEY_B"},
		},
	})
	s := serve(t, g)
	code, out := post(t, s.URL+"/passthrough/up/document-digitization?x=1", "secret-a", "PDFBYTES")
	if code != 200 || out["parsed"] != true {
		t.Fatalf("passthrough %d %v", code, out)
	}
	if gotAuth != "Bearer provider-secret" || gotPath != "/v1/document-digitization?x=1" || gotBody != "PDFBYTES" {
		t.Fatalf("중계 내용 auth=%q path=%q body=%q", gotAuth, gotPath, gotBody)
	}
	if g.Stats()["up"].Calls != 1 {
		t.Fatalf("provider 게이트를 거쳐야 함: %+v", g.Stats()["up"])
	}
	if code, _ := post(t, s.URL+"/passthrough/up/x", "secret-b", "x"); code != 403 {
		t.Fatalf("passthrough 미허용 클라이언트 → 403, got %d", code)
	}
}

func TestRequestLogWritten(t *testing.T) {
	f := newFake(t, func(_ int64, w http.ResponseWriter) { okJSON(w, "ok") })
	path := filepath.Join(t.TempDir(), "req.jsonl")
	t.Setenv("KEY_A", "secret-a")
	g := build(t, &Config{
		Providers: map[string]*ProviderConfig{"p": prov(f.srv.URL)},
		Routes:    map[string]*RouteConfig{"r": {Steps: []StepConfig{{Provider: "p"}}}},
		Clients:   map[string]*ClientConfig{"app-a": {APIKeyEnv: "KEY_A"}},
		LogPath:   path,
	})
	req := userReq("r", "비밀 프롬프트")
	req.Client = "app-a"
	if _, err := g.Call(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	g.Close()
	fh, _ := os.Open(path)
	defer fh.Close()
	sc := bufio.NewScanner(fh)
	if !sc.Scan() {
		t.Fatal("로그 비어 있음")
	}
	line := sc.Text()
	var l logLine
	if err := json.Unmarshal([]byte(line), &l); err != nil || l.Client != "app-a" || l.Provider != "p" || !l.OK {
		t.Fatalf("로그 %s", line)
	}
	if strings.Contains(line, "비밀 프롬프트") {
		t.Fatal("로그에 본문이 남으면 안 됨")
	}
}

func TestConfigRejectsUnknownClientRoute(t *testing.T) {
	c := &Config{
		Providers: map[string]*ProviderConfig{"p": {BaseURL: "http://x", Model: "m"}},
		Routes:    map[string]*RouteConfig{"r": {Steps: []StepConfig{{Provider: "p"}}}},
		Clients:   map[string]*ClientConfig{"c": {APIKeyEnv: "K", Routes: []string{"nope"}, Passthrough: []string{"p"}}},
	}
	c.applyDefaults()
	err := c.validate()
	if err == nil || !strings.Contains(err.Error(), "nope") || !strings.Contains(err.Error(), "passthrough") {
		t.Fatalf("err=%v", err)
	}
}

func TestSchemaHintAndEnumCheck(t *testing.T) {
	// a: 스키마 무시(enum 밖 값) → 검증 실패로 b 폴백. a 요청에는 스키마 지시문이 붙어야 한다.
	a := newFake(t, func(_ int64, w http.ResponseWriter) { okJSON(w, `{"label":"부정적"}`) })
	b := newFake(t, func(_ int64, w http.ResponseWriter) { okJSON(w, "```json\n{\"label\":\"bearish\"}\n```") })
	strict := prov(b.srv.URL)
	strict.JSONSchema = true
	g := build(t, &Config{
		Providers: map[string]*ProviderConfig{"a": prov(a.srv.URL), "b": strict},
		Routes:    map[string]*RouteConfig{"r": {Steps: []StepConfig{{Provider: "a"}, {Provider: "b"}}}},
	})
	req := userReq("r", "x")
	req.ResponseFormat = json.RawMessage(`{"type":"json_schema","json_schema":{"name":"s","strict":true,"schema":` +
		`{"type":"object","properties":{"label":{"type":"string","enum":["bullish","bearish","neutral"]}},"required":["label"]}}}`)
	resp, err := g.Call(t.Context(), req)
	if err != nil || resp.Provider != "b" {
		t.Fatalf("enum 밖 값이면 폴백해야 함: resp=%+v err=%v", resp, err)
	}
	if !strings.Contains(resp.Attempts[0].Error, "enum") {
		t.Fatalf("첫 시도 사유 %q", resp.Attempts[0].Error)
	}
	am := a.bodies[0]["messages"].([]any)
	if first := am[0].(map[string]any); first["role"] != "system" || !strings.Contains(first["content"].(string), `"enum"`) {
		t.Fatalf("미지원 provider 요청 앞에 스키마 지시문이 있어야 함: %v", first)
	}
	if bm := b.bodies[0]["messages"].([]any); len(bm) != 1 {
		t.Fatalf("json_schema 지원 provider 엔 지시문을 붙이지 않아야 함: %v", bm)
	}
}

func TestCheckSchemaRequired(t *testing.T) {
	s := json.RawMessage(`{"type":"object","required":["a","b"]}`)
	if err := checkSchema(`{"a":1}`, s); err == nil || !strings.Contains(err.Error(), `"b"`) {
		t.Fatalf("필수 키 누락을 잡아야 함: %v", err)
	}
	if err := checkSchema(`{"a":1,"b":2}`, s); err != nil {
		t.Fatal(err)
	}
}

func TestExternalBlocking(t *testing.T) {
	ext := newFake(t, func(_ int64, w http.ResponseWriter) { okJSON(w, "external") })
	in := newFake(t, func(n int64, w http.ResponseWriter) {
		if n == 1 {
			http.Error(w, "down", 500) // 첫 호출 실패 → 외부 허용이면 external 로 폴백
			return
		}
		okJSON(w, "internal")
	})
	pe := prov(ext.srv.URL)
	pe.External, pe.Passthrough = true, true
	t.Setenv("KEY_OPEN", "k-open")
	t.Setenv("KEY_CLOSED", "k-closed")
	no := false
	g := build(t, &Config{
		Providers: map[string]*ProviderConfig{"in": prov(in.srv.URL), "ext": pe},
		Routes:    map[string]*RouteConfig{"r": {Steps: []StepConfig{{Provider: "in"}, {Provider: "ext"}}}},
		Clients: map[string]*ClientConfig{
			"open":   {APIKeyEnv: "KEY_OPEN", Passthrough: []string{"ext"}},
			"closed": {APIKeyEnv: "KEY_CLOSED", AllowExternal: &no, Passthrough: []string{"ext"}},
		},
	})
	// 외부 허용: 내부 실패 → external 로 폴백
	req := userReq("r", "a")
	req.Client = "open"
	resp, err := g.Call(t.Context(), req)
	if err != nil || resp.Provider != "ext" {
		t.Fatalf("open: resp=%+v err=%v", resp, err)
	}
	// 외부 차단 클라이언트: 내부만. 내부가 성공하면 그대로, external 은 건드리지 않는다.
	req = userReq("r", "b")
	req.Client = "closed"
	resp, err = g.Call(t.Context(), req)
	if err != nil || resp.Provider != "in" {
		t.Fatalf("closed: resp=%+v err=%v", resp, err)
	}
	// 요청 단위 차단 + 내부 실패 → external 은 건너뛰고 전체 실패
	bad := newFake(t, func(_ int64, w http.ResponseWriter) { http.Error(w, "down", 500) })
	g2 := build(t, &Config{
		Providers: map[string]*ProviderConfig{"in": prov(bad.srv.URL), "ext": pe},
		Routes:    map[string]*RouteConfig{"r": {Steps: []StepConfig{{Provider: "in"}, {Provider: "ext"}}}},
	})
	req = userReq("r", "c")
	req.NoExternal = true
	calls := ext.calls.Load()
	_, err = g2.Call(t.Context(), req)
	var ce *ChainError
	if !errors.As(err, &ce) || len(ce.Attempts) != 2 || ce.Attempts[1].Error != ErrExternalBlocked.Error() || ext.calls.Load() != calls {
		t.Fatalf("NoExternal 이면 external 을 호출하지 않아야 함: err=%v", err)
	}
	// 서버: 헤더로 차단, 차단 클라이언트의 external passthrough 는 403
	s := serve(t, g)
	hreq, _ := http.NewRequest(http.MethodPost, s.URL+"/passthrough/ext/x", strings.NewReader("x"))
	hreq.Header.Set("Authorization", "Bearer k-closed")
	hr, _ := http.DefaultClient.Do(hreq)
	hr.Body.Close()
	if hr.StatusCode != 403 {
		t.Fatalf("차단 클라이언트 external passthrough → 403, got %d", hr.StatusCode)
	}
	hreq, _ = http.NewRequest(http.MethodPost, s.URL+"/passthrough/ext/x", strings.NewReader("x"))
	hreq.Header.Set("Authorization", "Bearer k-open")
	hreq.Header.Set("X-LLMGW-No-External", "1")
	hr, _ = http.DefaultClient.Do(hreq)
	hr.Body.Close()
	if hr.StatusCode != 403 {
		t.Fatalf("X-LLMGW-No-External 헤더 → 403, got %d", hr.StatusCode)
	}
}

func TestReasoningInherit(t *testing.T) {
	f := newFake(t, func(_ int64, w http.ResponseWriter) { okJSON(w, "ok") })
	pq := prov(f.srv.URL)
	pq.Reasoning, pq.ReasoningMinTokens = "chat_template", 2048
	pu := prov(f.srv.URL)
	pu.Reasoning = "effort"
	g := build(t, &Config{
		Providers: map[string]*ProviderConfig{"q": pq, "u": pu},
		Routes: map[string]*RouteConfig{
			"q": {Steps: []StepConfig{{Provider: "q", Reasoning: "inherit", MaxTokens: 500}}},
			"u": {Steps: []StepConfig{{Provider: "u", Reasoning: "inherit"}}},
		},
	})
	s := serve(t, g)
	for _, body := range []string{
		`{"model":"q","messages":[{"role":"user","content":"1"}],"chat_template_kwargs":{"enable_thinking":true}}`,
		`{"model":"q","messages":[{"role":"user","content":"2"}]}`,
		`{"model":"u","messages":[{"role":"user","content":"3"}],"reasoning_effort":"low"}`,
		`{"model":"u","messages":[{"role":"user","content":"4"}],"chat_template_kwargs":{"enable_thinking":false}}`,
		`{"model":"q","messages":[{"role":"user","content":"5"}],"reasoning_effort":"none"}`,
		`{"model":"u","messages":[{"role":"user","content":"6"}],"reasoning_effort":"minimal"}`,
	} {
		if code, out := post(t, s.URL+"/v1/chat/completions", "", body); code != 200 {
			t.Fatalf("%d %v", code, out)
		}
	}
	b := f.bodies
	if kw := b[0]["chat_template_kwargs"].(map[string]any); kw["enable_thinking"] != true || b[0]["max_tokens"].(float64) != 2048 {
		t.Fatalf("호출자 enable_thinking=true 를 따라야 함: %v", b[0])
	}
	if kw := b[1]["chat_template_kwargs"].(map[string]any); kw["enable_thinking"] != false {
		t.Fatalf("지정 없으면 off: %v", b[1])
	}
	if b[2]["reasoning_effort"] != "low" {
		t.Fatalf("reasoning_effort 를 effort 방식 provider 로 전달해야 함: %v", b[2])
	}
	if _, ok := b[3]["reasoning_effort"]; ok {
		t.Fatalf("enable_thinking=false 면 effort 를 보내지 않아야 함: %v", b[3])
	}
	if kw := b[4]["chat_template_kwargs"].(map[string]any); kw["enable_thinking"] != false {
		t.Fatalf("reasoning_effort=none 이면 추론을 꺼야 함: %v", b[4])
	}
	if b[5]["reasoning_effort"] != "low" {
		t.Fatalf("reasoning_effort=minimal 은 low 로 보내야 함: %v", b[5])
	}
}

func TestClientKeysMissingFailsClosed(t *testing.T) {
	f := newFake(t, func(_ int64, w http.ResponseWriter) { okJSON(w, "hi") })
	t.Setenv("KEY_EMPTY", "")
	g := build(t, &Config{
		Providers: map[string]*ProviderConfig{"p": prov(f.srv.URL)},
		Routes:    map[string]*RouteConfig{"r": {Steps: []StepConfig{{Provider: "p"}}}},
		Clients:   map[string]*ClientConfig{"app": {APIKeyEnv: "KEY_EMPTY"}},
	})
	if err := g.CheckClientKeys(); err == nil || !strings.Contains(err.Error(), "KEY_EMPTY") {
		t.Fatalf("비어 있는 키를 알려야 함: %v", err)
	}
	s := serve(t, g)
	for _, key := range []string{"", "anything"} {
		if code, _ := post(t, s.URL+"/v1/chat/completions", key, `{"model":"r","messages":[{"role":"user","content":"x"}]}`); code != 401 {
			t.Fatalf("clients 가 있는데 키가 비면 인증 없이 열리면 안 됨(key=%q): %d", key, code)
		}
	}
	if f.calls.Load() != 0 {
		t.Fatalf("upstream 이 호출되면 안 됨: %d", f.calls.Load())
	}
}

func sseFake(t *testing.T, fail bool) *fakeLLM {
	return newFake(t, func(_ int64, w http.ResponseWriter) {
		if fail {
			http.Error(w, "down", 503)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		for _, tok := range []string{"안", "녕"} {
			fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{\"content\":%q}}]}\n\n", tok)
			w.(http.Flusher).Flush()
		}
		fmt.Fprint(w, "data: [DONE]\n\n")
	})
}

func TestStreamFallbackBeforeFirstByte(t *testing.T) {
	bad, good := sseFake(t, true), sseFake(t, false)
	g := build(t, &Config{
		Providers: map[string]*ProviderConfig{"a": prov(bad.srv.URL), "b": prov(good.srv.URL)},
		Routes:    map[string]*RouteConfig{"r": {Steps: []StepConfig{{Provider: "a", Retries: 1}, {Provider: "b"}}}},
	})
	s := serve(t, g)
	resp, err := http.Post(s.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"r","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("status %d ct %q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	if !strings.Contains(string(body), `"안"`) || !strings.Contains(string(body), "[DONE]") {
		t.Fatalf("SSE 본문 %q", body)
	}
	if bad.calls.Load() != 2 || good.bodies[0]["stream"] != true {
		t.Fatalf("a 재시도 후 b 로 스트림: a=%d b.stream=%v", bad.calls.Load(), good.bodies[0]["stream"])
	}
}

func TestStreamAllFailedIsJSONError(t *testing.T) {
	bad := sseFake(t, true)
	g := build(t, &Config{
		Providers: map[string]*ProviderConfig{"a": prov(bad.srv.URL)},
		Routes:    map[string]*RouteConfig{"r": {Steps: []StepConfig{{Provider: "a"}}}},
	})
	s := serve(t, g)
	code, out := post(t, s.URL+"/v1/chat/completions", "", `{"model":"r","stream":true,"messages":[{"role":"user","content":"x"}]}`)
	if code != 502 || out["attempts"] == nil {
		t.Fatalf("스트림 시작 전 전부 실패면 502 JSON: %d %v", code, out)
	}
}

func TestUpstreamErrorNotLeakedToCaller(t *testing.T) {
	f := newFake(t, func(_ int64, w http.ResponseWriter) {
		http.Error(w, `{"error":"internal host gpu-node-07.corp:8000 model /models/secret-path"}`, 500)
	})
	dir := t.TempDir()
	logPath := filepath.Join(dir, "req.jsonl")
	g := build(t, &Config{
		LogPath:   logPath,
		Providers: map[string]*ProviderConfig{"a": prov(f.srv.URL), "dead": prov("http://127.0.0.1:1")},
		Routes:    map[string]*RouteConfig{"r": {Steps: []StepConfig{{Provider: "a"}, {Provider: "dead"}}}},
	})
	s := serve(t, g)
	req, _ := http.NewRequest(http.MethodPost, s.URL+"/v1/chat/completions", strings.NewReader(`{"model":"r","messages":[{"role":"user","content":"x"}]}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	g.Close()
	if resp.StatusCode != 502 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	for _, leak := range []string{"gpu-node-07", "secret-path", "127.0.0.1:1"} {
		if strings.Contains(string(raw), leak) {
			t.Fatalf("호출자 응답에 upstream 원문 %q 가 새어 나감: %s", leak, raw)
		}
	}
	if !strings.Contains(string(raw), "HTTP 500") || !strings.Contains(string(raw), "연결 실패") {
		t.Fatalf("분류는 남아야 함: %s", raw)
	}
	logged, _ := os.ReadFile(logPath)
	if !strings.Contains(string(logged), "gpu-node-07") || !strings.Contains(string(logged), "127.0.0.1:1") {
		t.Fatalf("서버 로그에는 원문이 남아야 함: %s", logged)
	}
}

func TestMetaHeaders(t *testing.T) {
	f := newFake(t, func(_ int64, w http.ResponseWriter) { okJSON(w, "ok") })
	good := sseFake(t, false)
	p := prov(f.srv.URL)
	p.Mask = true
	g := build(t, &Config{
		Providers: map[string]*ProviderConfig{"a": p, "s": prov(good.srv.URL), "down": prov("http://127.0.0.1:1")},
		Routes: map[string]*RouteConfig{
			"r":  {Steps: []StepConfig{{Provider: "a"}}},
			"st": {Steps: []StepConfig{{Provider: "down"}, {Provider: "s"}}},
		},
	})
	s := serve(t, g)
	resp, err := http.Post(s.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"r","messages":[{"role":"user","content":"메일 a@b.com"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if h := resp.Header; h.Get("X-LLMGW-Provider") != "a" || h.Get("X-LLMGW-Model") != "m" || h.Get("X-LLMGW-Attempts") != "1" || h.Get("X-LLMGW-Masked") != "1" {
		t.Fatalf("headers %v", resp.Header)
	}
	resp, err = http.Post(s.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"st","stream":true,"messages":[{"role":"user","content":"x"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	io.ReadAll(resp.Body)
	resp.Body.Close()
	if h := resp.Header; h.Get("X-LLMGW-Provider") != "s" || h.Get("X-LLMGW-Attempts") != "2" {
		t.Fatalf("스트림 응답도 헤더로 처리 provider 를 알려야 함: %v", resp.Header)
	}
}

func TestStreamIdleTimeout(t *testing.T) {
	// 줄 간격이 idle 보다 짧으면 전체 길이가 timeout 을 넘어도 끝까지 받는다.
	steady := newFake(t, func(_ int64, w http.ResponseWriter) {
		w.Header().Set("Content-Type", "text/event-stream")
		for range 6 {
			fmt.Fprint(w, "data: {}\n\n")
			w.(http.Flusher).Flush()
			time.Sleep(50 * time.Millisecond)
		}
		fmt.Fprint(w, "data: [DONE]\n\n")
	})
	// 첫 줄 뒤 멈추면 idle 타임아웃으로 끊는다.
	stall := newFake(t, func(_ int64, w http.ResponseWriter) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {}\n\n")
		w.(http.Flusher).Flush()
		time.Sleep(2 * time.Second)
	})
	mk := func(url string) *ProviderConfig {
		p := prov(url)
		p.Timeout, p.StreamIdleTimeout = Duration{200 * time.Millisecond}, Duration{150 * time.Millisecond}
		return p
	}
	g := build(t, &Config{
		Providers: map[string]*ProviderConfig{"steady": mk(steady.srv.URL), "stall": mk(stall.srv.URL)},
		Routes: map[string]*RouteConfig{
			"steady": {Steps: []StepConfig{{Provider: "steady"}}},
			"stall":  {Steps: []StepConfig{{Provider: "stall"}}},
		},
	})
	var lines int
	if _, err := g.Stream(context.Background(), userReq("steady", "x"), func([]byte) error { lines++; return nil }); err != nil {
		t.Fatalf("전체 300ms+ 스트림이 timeout(200ms)에 잘리면 안 됨: %v", err)
	}
	t0 := time.Now()
	resp, err := g.Stream(context.Background(), userReq("stall", "x"), func([]byte) error { return nil })
	if err == nil || time.Since(t0) > time.Second {
		t.Fatalf("멈춘 스트림은 idle 타임아웃으로 빨리 끊어야 함: err=%v %s", err, time.Since(t0))
	}
	if a := resp.Attempts[len(resp.Attempts)-1]; a.Error != "스트림 idle 타임아웃" {
		t.Fatalf("attempt %+v", a)
	}
	if lines < 7 {
		t.Fatalf("steady 줄 수 %d", lines)
	}
}

func TestStreamErrorAfterStartIsSanitized(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {}\n\n")
		w.(http.Flusher).Flush()
		conn, _, _ := w.(http.Hijacker).Hijack() // 스트림 도중 연결을 끊는다
		conn.Close()
	}))
	t.Cleanup(srv.Close)
	g := build(t, &Config{
		Providers: map[string]*ProviderConfig{"a": prov(srv.URL)},
		Routes:    map[string]*RouteConfig{"r": {Steps: []StepConfig{{Provider: "a"}}}},
	})
	resp, err := g.Stream(context.Background(), userReq("r", "x"), func([]byte) error { return nil })
	if err == nil || err.Error() != "스트림 중단(a): 스트림 읽기 실패" {
		t.Fatalf("시작 뒤 에러도 분류만 돌려줘야 함: %v", err)
	}
	if a := resp.Attempts[0]; a.Detail == "" || !strings.Contains(a.Detail, "스트림 읽기:") {
		t.Fatalf("원문은 Detail 에 남아야 함: %+v", a)
	}
}

func TestPassthroughTimeoutClassified(t *testing.T) {
	slow := newFake(t, func(_ int64, w http.ResponseWriter) { time.Sleep(300 * time.Millisecond) })
	p := prov(slow.srv.URL)
	p.Passthrough, p.Timeout = true, Duration{50 * time.Millisecond}
	g := build(t, &Config{
		Providers: map[string]*ProviderConfig{"a": p},
		Routes:    map[string]*RouteConfig{"r": {Steps: []StepConfig{{Provider: "a"}}}},
	})
	s := serve(t, g)
	code, out := post(t, s.URL+"/passthrough/a/parse", "", `{}`)
	if msg := out["error"].(map[string]any)["message"]; code != 502 || msg != "타임아웃" {
		t.Fatalf("passthrough 타임아웃은 타임아웃으로 분류해야 함: %d %v", code, msg)
	}
}
