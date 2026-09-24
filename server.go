package llmgw

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"
)

// Handler 는 OpenAI 호환 HTTP 핸들러다. 요청의 model 에 라우트 이름을 넣으면 그 체인으로 호출한다.
//
//	POST /v1/chat/completions  {"model":"news-deep","messages":[...]}
//	GET  /v1/models            라우트 목록
//	GET  /stats                프로바이더별 호출·대기열 상태
//	GET  /healthz
func (g *Gateway) Handler() http.Handler {
	token := ""
	if env := g.cfg.Server.APIKeyEnv; env != "" {
		token = os.Getenv(env)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, http.StatusOK, g.Stats()) })
	mux.HandleFunc("GET /v1/models", func(w http.ResponseWriter, _ *http.Request) {
		names := g.Routes()
		sort.Strings(names)
		data := make([]map[string]any, 0, len(names))
		for _, n := range names {
			data = append(data, map[string]any{"id": n, "object": "model", "owned_by": "llmgw"})
		}
		writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": data})
	})
	mux.HandleFunc("POST /v1/chat/completions", g.handleChat)
	if token == "" {
		return mux
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" {
			got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			if subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
				writeErr(w, http.StatusUnauthorized, "unauthorized")
				return
			}
		}
		mux.ServeHTTP(w, r)
	})
}

func (g *Gateway) handleChat(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Model          string    `json:"model"`
		Messages       []Message `json:"messages"`
		MaxTokens      int       `json:"max_tokens"`
		Temperature    *float64  `json:"temperature"`
		Stream         bool      `json:"stream"`
		ResponseFormat *struct {
			Type string `json:"type"`
		} `json:"response_format"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 32<<20)).Decode(&in); err != nil {
		writeErr(w, http.StatusBadRequest, "요청 JSON: "+err.Error())
		return
	}
	if in.Stream {
		writeErr(w, http.StatusBadRequest, "stream 은 지원하지 않는다")
		return
	}
	req := Request{
		Route: in.Model, Messages: in.Messages, MaxTokens: in.MaxTokens, Temperature: in.Temperature,
		JSON: in.ResponseFormat != nil && strings.HasPrefix(in.ResponseFormat.Type, "json"),
	}
	resp, err := g.Call(r.Context(), req)
	if err != nil {
		var ce *ChainError
		switch {
		case errors.As(err, &ce):
			writeJSON(w, http.StatusBadGateway, map[string]any{
				"error": map[string]any{"message": err.Error(), "type": "all_steps_failed"}, "attempts": ce.Attempts,
			})
		case errors.Is(err, ErrUnknownRoute):
			writeErr(w, http.StatusNotFound, err.Error())
		default:
			writeErr(w, http.StatusGatewayTimeout, err.Error())
		}
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id":      "llmgw-" + time.Now().Format("20060102150405.000000"),
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   resp.Model,
		"choices": []map[string]any{{
			"index":         0,
			"finish_reason": "stop",
			"message":       map[string]any{"role": "assistant", "content": resp.Content, "reasoning_content": resp.Reasoning},
		}},
		"usage": map[string]any{
			"prompt_tokens": resp.Usage.PromptTokens, "completion_tokens": resp.Usage.CompletionTokens,
			"total_tokens": resp.Usage.PromptTokens + resp.Usage.CompletionTokens,
		},
		"llmgw": map[string]any{"route": in.Model, "provider": resp.Provider, "cached": resp.Cached, "attempts": resp.Attempts},
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]any{"error": map[string]string{"message": msg}})
}
