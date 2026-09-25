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
	"time"
)

// Stream 은 라우트를 스트리밍으로 호출한다. 첫 바이트를 받기 전의 실패(연결 오류·429·5xx 등)는 Call 과
// 같은 규칙으로 재시도·폴백하고, 스트림이 시작되면 그 provider 의 SSE 줄을 onLine 으로 그대로 넘긴다.
// 스트림 도중 끊기면 다른 provider 로 이어붙이지 않는다(부분 응답을 섞지 않기 위해). 검증·캐시는 하지 않는다.
func (g *Gateway) Stream(ctx context.Context, req Request, onLine func([]byte) error) (resp *Response, err error) {
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
		model, _ := body["model"].(string)
		if p.External && req.NoExternal {
			attempts = append(attempts, Attempt{Step: i, Provider: s.Provider, Model: model, Reasoning: s.Reasoning, Error: ErrExternalBlocked.Error()})
			continue
		}
		for try := 0; ; try++ {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			a := Attempt{Step: i, Provider: s.Provider, Model: model, Reasoning: s.Reasoning}
			t0 := time.Now()
			release, err := gt.acquire(ctx)
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
			started, err := doStream(ctx, g.hc, p, g.keys[s.Provider], body, onLine)
			release()
			a.LatencyMS = time.Since(t1).Milliseconds()
			if started {
				// 스트림이 시작된 뒤의 에러는 폴백하지 않고 그대로 돌려준다.
				// 호출자 취소·전달 실패(onLine 에러)는 provider 실패로 세지 않는다.
				var ce *callError
				switch {
				case err == nil:
					gt.report(true)
				case ctx.Err() == nil && errors.As(err, &ce):
					gt.report(false)
				default:
					gt.reportNeutral()
				}
				if err != nil {
					a.Error = err.Error()
				}
				attempts = append(attempts, a)
				r := &Response{Provider: s.Provider, Model: model, Attempts: attempts, Latency: Duration{time.Since(start)}}
				if err != nil {
					return r, fmt.Errorf("스트림 중단(%s): %w", s.Provider, err)
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
			a.Error = err.Error()
			if ce != nil {
				a.Status = ce.status
			}
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

// doStream 은 stream:true 로 호출한다. 200 을 받아 첫 줄을 넘기기 시작하면 started=true.
func doStream(ctx context.Context, hc *http.Client, p *ProviderConfig, apiKey string, body map[string]any, onLine func([]byte) error) (started bool, err error) {
	b, err := json.Marshal(body)
	if err != nil {
		return false, &callError{msg: "요청 직렬화: " + err.Error()}
	}
	ctx, cancel := context.WithTimeout(ctx, p.Timeout.Duration)
	defer cancel()
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.BaseURL+"/chat/completions", bytes.NewReader(b))
	if err != nil {
		return false, &callError{msg: err.Error()}
	}
	hreq.Header.Set("Content-Type", "application/json")
	hreq.Header.Set("Accept", "text/event-stream")
	if apiKey != "" {
		hreq.Header.Set("Authorization", "Bearer "+apiKey)
	}
	resp, err := hc.Do(hreq)
	if err != nil {
		return false, &callError{msg: err.Error(), retryable: !errors.Is(ctx.Err(), context.Canceled)}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return false, &callError{
			status:     resp.StatusCode,
			retryAfter: parseRetryAfter(resp.Header.Get("Retry-After")),
			retryable:  resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500,
			msg:        truncate(string(raw), 300),
		}
	}
	rd := bufio.NewReaderSize(resp.Body, 64<<10)
	for {
		line, rerr := rd.ReadBytes('\n')
		if len(line) > 0 {
			started = true
			if err := onLine(line); err != nil {
				return true, err
			}
		}
		if rerr == io.EOF {
			if !started {
				return false, &callError{msg: "빈 스트림", retryable: true}
			}
			return true, nil
		}
		if rerr != nil {
			return started, &callError{msg: "스트림 읽기: " + rerr.Error(), retryable: !started}
		}
	}
}
