package llmgw

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"sync/atomic"
	"time"
)

// Stream 은 라우트를 스트리밍으로 호출한다. 첫 바이트를 받기 전의 실패(연결 오류·429·5xx 등)는 Call 과
// 같은 규칙으로 재시도·폴백하고, 스트림이 시작되면 그 provider 의 SSE 줄을 onLine 으로 그대로 넘긴다.
// 스트림 도중 끊기면 다른 provider 로 이어붙이지 않는다(부분 응답을 섞지 않기 위해). 검증·캐시는 하지 않는다.
func (g *Gateway) Stream(ctx context.Context, req Request, onLine func([]byte) error) (resp *Response, err error) {
	return g.StreamWithStart(ctx, req, nil, onLine)
}

// StreamWithStart 는 Stream 과 같고, 스트림이 시작될 때(첫 줄을 넘기기 직전) onStart 를 한 번 부른다.
// onStart 는 처리할 provider·모델과 그때까지의 시도 기록을 받는다(응답 헤더를 쓰는 데 쓴다).
func (g *Gateway) StreamWithStart(ctx context.Context, req Request, onStart func(*Response), onLine func([]byte) error) (resp *Response, err error) {
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

	var attempts []Attempt
	for i := range route.Steps {
		s := &route.Steps[i]
		p := g.cfg.Providers[s.Provider]
		gt := g.gates[s.Provider]
		body := buildBody(p, s, &req)
		body["stream"] = true
		if p.StreamUsage {
			body["stream_options"] = map[string]any{"include_usage": true}
		}
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
			release, err := gt.acquireWithin(ctx, g.stepMaxWait(route, i, req.NoExternal))
			a.WaitedMS = time.Since(t0).Milliseconds()
			if err != nil {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				a.Error = err.Error()
				attempts = append(attempts, a)
				break
			}
			t1 := time.Now()
			first := true
			var usage Usage
			started, err := doStream(ctx, g.hc, p, g.keys[s.Provider], body, func(line []byte) error {
				if u, ok := usageFromSSE(line); ok {
					usage = u
				}
				if first {
					first = false
					if onStart != nil {
						onStart(&Response{Provider: s.Provider, Model: model, Attempts: append(slices.Clone(attempts), a)})
					}
				}
				return onLine(line)
			})
			release()
			a.LatencyMS = time.Since(t1).Milliseconds()
			if started {
				// 스트림이 시작된 뒤의 에러는 폴백하지 않고 그대로 돌려준다.
				// 호출자 취소·전달 실패(onLine 에러)는 provider 실패로 세지 않는다.
				var ce *callError
				errors.As(err, &ce)
				switch {
				case err == nil:
					gt.report(true)
					gt.lat.observe(time.Since(t1))
				case ctx.Err() == nil && ce != nil:
					gt.report(false)
				default:
					gt.reportNeutral()
				}
				if err != nil {
					a.setError(err)
				}
				attempts = append(attempts, a)
				r := &Response{Provider: s.Provider, Model: model, Usage: usage, Attempts: attempts, Latency: Duration{time.Since(start)}}
				if err != nil {
					// 호출자에게는 분류만 준다. upstream 원문은 Attempt.Detail 에 남아 있다.
					if ce != nil {
						return r, fmt.Errorf("스트림 중단(%s): %s", s.Provider, ce.public())
					}
					return r, fmt.Errorf("스트림 중단(%s): %w", s.Provider, err) // onLine 이 돌려준 호출자 쪽 에러
				}
				return r, nil
			}
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			var ce *callError
			errors.As(err, &ce)
			if ce != nil && !ce.providerFault() {
				gt.reportNeutral()
			} else {
				gt.report(false)
			}
			a.setError(err)
			attempts = append(attempts, a)
			if ce == nil || !ce.retryable || try >= s.Retries {
				break
			}
			wait := backoff(try)
			if ce.retryAfter > 0 {
				gt.deferRate(time.Now().Add(ce.retryAfter))
				wait = ce.retryAfter
			}
			if wait > s.MaxRetryWait.Duration {
				break
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

var errLineTooLong = errors.New("줄이 너무 김")

// readLine 은 줄바꿈까지 읽되 limit 바이트를 넘으면 errLineTooLong 을 돌려준다.
// bufio.ReadBytes 는 줄바꿈이 올 때까지 끝없이 버퍼를 늘리므로, 줄바꿈 없이 큰 데이터가 오면 메모리를 다 쓴다.
func readLine(rd *bufio.Reader, limit int) ([]byte, error) {
	var line []byte
	for {
		chunk, err := rd.ReadSlice('\n')
		if len(line)+len(chunk) > limit {
			return nil, errLineTooLong
		}
		line = append(line, chunk...)
		if err != bufio.ErrBufferFull {
			return line, err
		}
	}
}

// usageFromSSE 는 SSE 한 줄에 usage 가 실려 있으면 꺼낸다(stream_options.include_usage 의 마지막 청크 등).
func usageFromSSE(line []byte) (Usage, bool) {
	data, ok := bytes.CutPrefix(bytes.TrimSpace(line), []byte("data:"))
	if !ok || !bytes.Contains(data, []byte(`"usage"`)) {
		return Usage{}, false
	}
	var c struct {
		Usage *struct {
			PromptTokens            int `json:"prompt_tokens"`
			CompletionTokens        int `json:"completion_tokens"`
			CompletionTokensDetails struct {
				ReasoningTokens int `json:"reasoning_tokens"`
			} `json:"completion_tokens_details"`
		} `json:"usage"`
	}
	if json.Unmarshal(bytes.TrimSpace(data), &c) != nil || c.Usage == nil {
		return Usage{}, false
	}
	return Usage{PromptTokens: c.Usage.PromptTokens, CompletionTokens: c.Usage.CompletionTokens,
		ReasoningTokens: c.Usage.CompletionTokensDetails.ReasoningTokens}, true
}

// doStream 은 stream:true 로 호출한다. 200 을 받아 첫 줄을 넘기기 시작하면 started=true.
// 응답 헤더를 받을 때까지는 provider timeout, 그 뒤로는 줄 사이 간격에 stream_idle_timeout 을 적용한다.
// 스트림 전체 길이에는 제한을 두지 않는다(긴 추론 응답이 중간에 잘리지 않게).
func doStream(parent context.Context, hc *http.Client, p *ProviderConfig, apiKey string, body map[string]any, onLine func([]byte) error) (started bool, err error) {
	b, err := json.Marshal(body)
	if err != nil {
		return false, &callError{msg: "요청 직렬화: " + err.Error(), kind: "요청 직렬화 실패"}
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	var timedOut atomic.Bool
	timer := time.AfterFunc(p.Timeout.Duration, func() { timedOut.Store(true); cancel() })
	defer timer.Stop()

	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.BaseURL+"/chat/completions", bytes.NewReader(b))
	if err != nil {
		return false, &callError{msg: err.Error(), kind: "요청 생성 실패"}
	}
	hreq.Header.Set("Content-Type", "application/json")
	hreq.Header.Set("Accept", "text/event-stream")
	if apiKey != "" {
		hreq.Header.Set("Authorization", "Bearer "+apiKey)
	}
	resp, err := hc.Do(hreq)
	if err != nil {
		return false, transportError(err, timedOut.Load(), parent.Err() == nil)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return false, &callError{
			status:     resp.StatusCode,
			retryAfter: parseRetryAfter(resp.Header.Get("Retry-After")),
			retryable:  resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500,
			msg:        truncate(string(raw), 300),
		}
	}
	idle := p.StreamIdleTimeout.Duration
	timer.Reset(idle)
	rd := bufio.NewReaderSize(resp.Body, 64<<10)
	for {
		line, rerr := readLine(rd, int(p.MaxResponseBytes))
		if len(line) > 0 {
			started = true
			timer.Stop() // 호출자에게 넘기는 동안은 idle 로 세지 않는다(느린 호출자 탓을 provider 에 돌리지 않게)
			if err := onLine(line); err != nil {
				return true, err
			}
			timer.Reset(idle)
		}
		if rerr == io.EOF {
			if !started {
				return false, &callError{msg: "빈 스트림", kind: "빈 스트림", retryable: true}
			}
			return true, nil
		}
		if rerr == errLineTooLong {
			return started, &callError{msg: fmt.Sprintf("스트림 한 줄이 max_response_bytes(%d) 초과", p.MaxResponseBytes), kind: "스트림 줄이 너무 김", retryable: !started}
		}
		if rerr != nil {
			if timedOut.Load() {
				return started, &callError{msg: fmt.Sprintf("스트림 %s 동안 응답 없음", idle), kind: "스트림 idle 타임아웃", retryable: !started}
			}
			return started, &callError{msg: "스트림 읽기: " + rerr.Error(), kind: "스트림 읽기 실패", retryable: !started}
		}
	}
}
