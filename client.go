package llmgw

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Message 는 OpenAI chat 메시지다.
type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// Usage 는 응답의 토큰 사용량이다. ReasoningTokens 는 제공하는 프로바이더만 채운다.
type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	ReasoningTokens  int `json:"reasoning_tokens,omitempty"`
}

// callError 는 HTTP 호출 실패다. retryable 이면 같은 step 에서 재시도할 수 있다.
type callError struct {
	status     int
	retryAfter time.Duration
	retryable  bool
	msg        string
}

func (e *callError) Error() string {
	if e.status > 0 {
		return fmt.Sprintf("HTTP %d: %s", e.status, e.msg)
	}
	return e.msg
}

type completion struct {
	content, reasoning, model string
	usage                     Usage
	finish                    string
}

// buildBody 는 step 설정을 프로바이더 방식에 맞는 요청 body 로 바꾼다.
func buildBody(p *ProviderConfig, s *StepConfig, req *Request) map[string]any {
	model := s.Model
	if model == "" {
		model = p.Model
	}
	maxTokens := s.MaxTokens
	if req.MaxTokens > 0 {
		maxTokens = req.MaxTokens
	}
	thinking := s.Reasoning != "off"
	if thinking && maxTokens < p.ReasoningMinTokens {
		maxTokens = p.ReasoningMinTokens
	}

	body := map[string]any{"model": model, "messages": req.Messages, "max_tokens": maxTokens}
	maps.Copy(body, p.Extra)
	switch {
	case req.Temperature != nil:
		body["temperature"] = *req.Temperature
	case s.Temperature != nil:
		body["temperature"] = *s.Temperature
	}
	if req.JSON {
		body["response_format"] = map[string]string{"type": "json_object"}
	}

	switch p.Reasoning {
	case "chat_template":
		kw, _ := body["chat_template_kwargs"].(map[string]any)
		if kw == nil {
			kw = map[string]any{}
		}
		kw["enable_thinking"] = thinking
		body["chat_template_kwargs"] = kw
	case "effort":
		if thinking {
			effort := s.Reasoning
			if effort == "on" {
				effort = "medium"
			}
			body["reasoning_effort"] = effort
		}
	}
	return body
}

// doCall 은 OpenAI 호환 /chat/completions 를 1회 호출한다.
func doCall(ctx context.Context, hc *http.Client, p *ProviderConfig, apiKey string, body map[string]any) (*completion, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return nil, &callError{msg: "요청 직렬화: " + err.Error()}
	}
	ctx, cancel := context.WithTimeout(ctx, p.Timeout.Duration)
	defer cancel()
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.BaseURL+"/chat/completions", bytes.NewReader(b))
	if err != nil {
		return nil, &callError{msg: err.Error()}
	}
	hreq.Header.Set("Content-Type", "application/json")
	if apiKey != "" {
		hreq.Header.Set("Authorization", "Bearer "+apiKey)
	}
	resp, err := hc.Do(hreq)
	if err != nil {
		// 호출자 취소가 아니면 네트워크·타임아웃이므로 재시도 대상이다.
		return nil, &callError{msg: err.Error(), retryable: ctx.Err() == nil || ctx.Err() == context.DeadlineExceeded}
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return nil, &callError{msg: "응답 읽기: " + err.Error(), retryable: true}
	}
	if resp.StatusCode != http.StatusOK {
		return nil, &callError{
			status:     resp.StatusCode,
			retryAfter: parseRetryAfter(resp.Header.Get("Retry-After")),
			retryable:  resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500,
			msg:        truncate(string(raw), 300),
		}
	}

	var r struct {
		Model   string `json:"model"`
		Choices []struct {
			FinishReason string `json:"finish_reason"`
			Message      struct {
				Content          string `json:"content"`
				ReasoningContent string `json:"reasoning_content"`
				Reasoning        string `json:"reasoning"`
			} `json:"message"`
		} `json:"choices"`
		Usage struct {
			PromptTokens            int `json:"prompt_tokens"`
			CompletionTokens        int `json:"completion_tokens"`
			CompletionTokensDetails struct {
				ReasoningTokens int `json:"reasoning_tokens"`
			} `json:"completion_tokens_details"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, &callError{msg: "응답 JSON: " + err.Error()}
	}
	if len(r.Choices) == 0 {
		return nil, &callError{msg: "choices 비어 있음"}
	}
	m := r.Choices[0].Message
	reasoning := m.ReasoningContent
	if reasoning == "" {
		reasoning = m.Reasoning
	}
	content := m.Content
	// 추론 파서가 없는 서버는 <think>…</think> 를 본문에 섞어 보낸다.
	if i := strings.LastIndex(content, "</think>"); i >= 0 {
		if reasoning == "" {
			reasoning = strings.TrimPrefix(content[:i], "<think>")
		}
		content = content[i+len("</think>"):]
	}
	return &completion{
		content:   strings.TrimSpace(content),
		reasoning: strings.TrimSpace(reasoning),
		model:     r.Model,
		finish:    r.Choices[0].FinishReason,
		usage: Usage{
			PromptTokens:     r.Usage.PromptTokens,
			CompletionTokens: r.Usage.CompletionTokens,
			ReasoningTokens:  r.Usage.CompletionTokensDetails.ReasoningTokens,
		},
	}, nil
}

// parseRetryAfter 는 초 단위 또는 HTTP 날짜 형식의 Retry-After 를 해석한다.
func parseRetryAfter(v string) time.Duration {
	if v == "" {
		return 0
	}
	if s, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && s >= 0 {
		return time.Duration(s) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
