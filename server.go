package llmgw

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Handler 는 OpenAI 호환 HTTP 핸들러다. 요청의 model 에 라우트 이름을 넣으면 그 체인으로 호출한다.
//
//	POST /v1/chat/completions              {"model":"<route>","messages":[...]}
//	GET  /v1/models                        호출자가 쓸 수 있는 라우트 목록
//	ANY  /passthrough/{provider}/{path...} provider API 그대로 중계(키·한도·대기열은 게이트웨이가 적용)
//	GET  /stats                            provider·client 별 상태
//	GET  /healthz
//
// clients 가 설정돼 있으면 Authorization: Bearer <client key> 로 호출자를 식별하고,
// 없으면 인증 없이 모든 라우트를 연다(127.0.0.1 바인드 전제). clients 가 있는데 키 환경변수가 비어 있으면
// 그 클라이언트는 인증할 수 없을 뿐 서버가 열리지는 않는다 — 기동 전에 CheckClientKeys 로 확인한다.
func (g *Gateway) Handler() http.Handler {
	auth := newAuthenticator(g.cfg.Clients)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("GET /stats", auth.wrap(func(w http.ResponseWriter, _ *http.Request, _ string) {
		writeJSON(w, http.StatusOK, map[string]any{"providers": g.Stats(), "clients": g.ClientStats()})
	}))
	mux.HandleFunc("GET /v1/models", auth.wrap(func(w http.ResponseWriter, _ *http.Request, client string) {
		names := g.Routes()
		sort.Strings(names)
		data := make([]map[string]any, 0, len(names))
		for _, n := range names {
			if c := g.cfg.Clients[client]; c != nil && !c.allows(n) {
				continue
			}
			data = append(data, map[string]any{"id": n, "object": "model", "owned_by": "llmgw"})
		}
		writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": data})
	}))
	mux.HandleFunc("POST /v1/chat/completions", auth.wrap(g.handleChat))
	mux.HandleFunc("/passthrough/{provider}/{path...}", auth.wrap(g.handlePassthrough))
	return mux
}

// authenticator 는 Bearer 키를 클라이언트 이름으로 바꾼다.
type authenticator struct {
	open bool              // clients 를 하나도 정의하지 않았을 때만 인증 없이 연다
	keys map[string]string // client → key
}

func newAuthenticator(clients map[string]*ClientConfig) *authenticator {
	a := &authenticator{open: len(clients) == 0, keys: map[string]string{}}
	for name, c := range clients {
		if k := os.Getenv(c.APIKeyEnv); k != "" {
			a.keys[name] = k
		}
	}
	return a
}

func (a *authenticator) wrap(h func(http.ResponseWriter, *http.Request, string)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if a.open {
			h(w, r, "")
			return
		}
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		for name, k := range a.keys {
			if subtle.ConstantTimeCompare([]byte(got), []byte(k)) == 1 {
				h(w, r, name)
				return
			}
		}
		writeErr(w, http.StatusUnauthorized, "unauthorized")
	}
}

// CheckClientKeys 는 clients 의 api_key_env 가 모두 채워져 있는지 확인한다. 서버 모드 기동 전에 부른다.
func (g *Gateway) CheckClientKeys() error {
	var missing []string
	for name, c := range g.cfg.Clients {
		if os.Getenv(c.APIKeyEnv) == "" {
			missing = append(missing, name+"("+c.APIKeyEnv+")")
		}
	}
	if len(missing) == 0 {
		return nil
	}
	sort.Strings(missing)
	return errors.New("client 키 환경변수가 비어 있음: " + strings.Join(missing, ", "))
}

func (g *Gateway) handleChat(w http.ResponseWriter, r *http.Request, client string) {
	var in struct {
		Model          string          `json:"model"`
		Messages       []Message       `json:"messages"`
		MaxTokens      int             `json:"max_tokens"`
		Temperature    *float64        `json:"temperature"`
		Stream         bool            `json:"stream"`
		ResponseFormat json.RawMessage `json:"response_format"`
		// 호출자의 추론 설정 — reasoning=inherit step 이 따른다.
		ReasoningEffort    string `json:"reasoning_effort"`
		ChatTemplateKwargs struct {
			EnableThinking *bool `json:"enable_thinking"`
		} `json:"chat_template_kwargs"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 32<<20)).Decode(&in); err != nil {
		writeErr(w, http.StatusBadRequest, "요청 JSON: "+err.Error())
		return
	}
	req := Request{
		Client: client, Route: in.Model, Messages: in.Messages, MaxTokens: in.MaxTokens,
		Temperature: in.Temperature, ResponseFormat: in.ResponseFormat,
		NoExternal: isTrue(r.Header.Get("X-LLMGW-No-External")),
		Reasoning:  normalizeReasoning(in.ReasoningEffort),
	}
	if et := in.ChatTemplateKwargs.EnableThinking; et != nil && req.Reasoning == "" {
		req.Reasoning = map[bool]string{true: "on", false: "off"}[*et]
	}
	if in.Stream {
		g.serveStream(w, r, req)
		return
	}
	resp, err := g.Call(r.Context(), req)
	if err != nil {
		g.writeCallErr(w, err)
		return
	}
	setMetaHeaders(w, resp)
	finish := resp.FinishReason
	if finish == "" {
		finish = "stop"
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id":      "llmgw-" + time.Now().Format("20060102150405.000000"),
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   resp.Model,
		"choices": []map[string]any{{
			"index":         0,
			"finish_reason": finish,
			"message":       map[string]any{"role": "assistant", "content": resp.Content, "reasoning_content": resp.Reasoning},
		}},
		"usage": map[string]any{
			"prompt_tokens": resp.Usage.PromptTokens, "completion_tokens": resp.Usage.CompletionTokens,
			"total_tokens": resp.Usage.PromptTokens + resp.Usage.CompletionTokens,
		},
		"llmgw": map[string]any{"route": in.Model, "provider": resp.Provider, "cached": resp.Cached, "attempts": resp.Attempts},
	})
}

// serveStream 은 provider 의 SSE 를 그대로 흘려보낸다. 스트림이 시작되기 전에 실패하면 일반 JSON 에러를 준다.
func (g *Gateway) serveStream(w http.ResponseWriter, r *http.Request, req Request) {
	fl, _ := w.(http.Flusher)
	headerSent := false
	_, err := g.StreamWithStart(r.Context(), req, func(resp *Response) { setMetaHeaders(w, resp) }, func(line []byte) error {
		if !headerSent {
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("Cache-Control", "no-cache")
			w.WriteHeader(http.StatusOK)
			headerSent = true
		}
		if _, err := w.Write(line); err != nil {
			return err
		}
		if fl != nil {
			fl.Flush()
		}
		return nil
	})
	if headerSent {
		return // 스트림이 시작된 뒤의 에러는 연결을 닫는 것으로 끝난다
	}
	g.writeCallErr(w, err)
}

// writeCallErr 는 Call·Stream 에러를 HTTP 상태로 옮긴다.
func (g *Gateway) writeCallErr(w http.ResponseWriter, err error) {
	var ce *ChainError
	switch {
	case errors.As(err, &ce):
		writeJSON(w, http.StatusBadGateway, map[string]any{
			"error": map[string]any{"message": err.Error(), "type": "all_steps_failed"}, "attempts": ce.Attempts,
		})
	case errors.Is(err, ErrUnknownRoute):
		writeErr(w, http.StatusNotFound, err.Error())
	case errors.Is(err, ErrRouteNotAllowed):
		writeErr(w, http.StatusForbidden, err.Error())
	case errors.Is(err, ErrClientLimited):
		w.Header().Set("Retry-After", "5")
		writeErr(w, http.StatusTooManyRequests, err.Error())
	default:
		writeErr(w, http.StatusGatewayTimeout, err.Error())
	}
}

// handlePassthrough 는 provider API 를 그대로 중계한다. 인증 헤더만 provider 키로 바꾸고,
// provider 의 한도·동시 처리·대기열과 클라이언트 한도를 똑같이 적용한다.
func (g *Gateway) handlePassthrough(w http.ResponseWriter, r *http.Request, client string) {
	name := r.PathValue("provider")
	p, ok := g.cfg.Providers[name]
	if !ok || !p.Passthrough {
		writeErr(w, http.StatusNotFound, "passthrough provider 없음: "+name)
		return
	}
	if c := g.cfg.Clients[client]; c != nil && !c.allowsPassthrough(name) {
		writeErr(w, http.StatusForbidden, "허용되지 않은 passthrough: "+name)
		return
	}
	if p.External && (isTrue(r.Header.Get("X-LLMGW-No-External")) || (g.cfg.Clients[client] != nil && !g.cfg.Clients[client].externalAllowed())) {
		writeErr(w, http.StatusForbidden, "외부 provider 차단: "+name)
		return
	}
	if cg := g.clientGates[client]; cg != nil {
		release, err := cg.acquire(r.Context())
		if err != nil {
			w.Header().Set("Retry-After", "5")
			writeErr(w, http.StatusTooManyRequests, "클라이언트 한도 초과: "+err.Error())
			return
		}
		defer release()
	}
	gt := g.gates[name]
	release, err := gt.acquire(r.Context())
	if err != nil {
		w.Header().Set("Retry-After", "5")
		writeErr(w, http.StatusTooManyRequests, "provider 대기열: "+err.Error())
		return
	}
	defer release()

	target := p.BaseURL + "/" + r.PathValue("path")
	if r.URL.RawQuery != "" {
		target += "?" + r.URL.RawQuery
	}
	ctx, cancel := context.WithTimeout(r.Context(), p.Timeout.Duration)
	defer cancel()
	out, err := http.NewRequestWithContext(ctx, r.Method, target, http.MaxBytesReader(w, r.Body, 128<<20))
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	for _, h := range []string{"Content-Type", "Accept", "Content-Encoding"} {
		if v := r.Header.Get(h); v != "" {
			out.Header.Set(h, v)
		}
	}
	out.ContentLength = r.ContentLength
	if k := g.keys[name]; k != "" {
		out.Header.Set("Authorization", "Bearer "+k)
	}
	resp, err := g.hc.Do(out)
	if err != nil {
		if r.Context().Err() != nil {
			gt.reportNeutral() // 호출자가 끊은 것은 provider 실패가 아니다
		} else {
			gt.report(false)
		}
		writeErr(w, http.StatusBadGateway, transportError(err, false, false).public()) // 내부 주소를 내보내지 않는다
		return
	}
	defer resp.Body.Close()
	gt.report(resp.StatusCode < 500 && resp.StatusCode != http.StatusTooManyRequests)
	if ra := parseRetryAfter(resp.Header.Get("Retry-After")); resp.StatusCode == http.StatusTooManyRequests && ra > 0 {
		gt.deferRate(time.Now().Add(ra))
	}
	for _, h := range []string{"Content-Type", "Retry-After"} {
		if v := resp.Header.Get(h); v != "" {
			w.Header().Set(h, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

// setMetaHeaders 는 실제 처리한 provider·모델·시도 횟수를 응답 헤더로 알린다.
// 스트림 응답에는 본문에 llmgw 필드를 넣을 수 없어서 헤더로만 알 수 있다.
func setMetaHeaders(w http.ResponseWriter, r *Response) {
	h := w.Header()
	h.Set("X-LLMGW-Provider", r.Provider)
	h.Set("X-LLMGW-Model", r.Model)
	h.Set("X-LLMGW-Attempts", strconv.Itoa(len(r.Attempts)))
	if r.Cached {
		h.Set("X-LLMGW-Cached", "1")
	}
	if n := len(r.Attempts); n > 0 && r.Attempts[n-1].Masked > 0 {
		h.Set("X-LLMGW-Masked", strconv.Itoa(r.Attempts[n-1].Masked))
	}
}

func isTrue(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]any{"error": map[string]string{"message": msg}})
}
