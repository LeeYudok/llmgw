package llmgw

import (
	"encoding/json"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type clientStats struct {
	Calls, OK, Fail, Rejected, Cached, PromptTokens, CompletionTokens atomic.Int64
}

// ClientStat 은 클라이언트별 누적 사용량이다.
type ClientStat struct {
	Calls, OK, Fail, Rejected, Cached int64
	PromptTokens, CompletionTokens    int64
	InFlight, Waiting                 int64
}

// ClientStats 는 클라이언트별 사용량 스냅샷을 돌려준다.
func (g *Gateway) ClientStats() map[string]ClientStat {
	out := make(map[string]ClientStat, len(g.clientStats))
	for name, s := range g.clientStats {
		gt := g.clientGates[name]
		out[name] = ClientStat{
			Calls: s.Calls.Load(), OK: s.OK.Load(), Fail: s.Fail.Load(), Rejected: s.Rejected.Load(),
			Cached: s.Cached.Load(), PromptTokens: s.PromptTokens.Load(), CompletionTokens: s.CompletionTokens.Load(),
			InFlight: gt.inFlight.Load(), Waiting: gt.waiting.Load(),
		}
	}
	return out
}

// requestLog 는 요청 1건당 JSONL 한 줄을 남긴다. 본문(프롬프트·응답)은 남기지 않는다.
type requestLog struct {
	mu sync.Mutex
	f  *os.File
}

func openRequestLog(path string) (*requestLog, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return nil, err
	}
	return &requestLog{f: f}, nil
}

type logLine struct {
	TS               string `json:"ts"`
	Client           string `json:"client,omitempty"`
	Route            string `json:"route"`
	Provider         string `json:"provider,omitempty"`
	Model            string `json:"model,omitempty"`
	OK               bool   `json:"ok"`
	Cached           bool   `json:"cached,omitempty"`
	LatencyMS        int64  `json:"latency_ms"`
	Steps            int    `json:"steps,omitempty"`
	PromptTokens     int    `json:"prompt_tokens,omitempty"`
	CompletionTokens int    `json:"completion_tokens,omitempty"`
	ReasoningTokens  int    `json:"reasoning_tokens,omitempty"`
	Error            string `json:"error,omitempty"`
}

// record 는 호출 1건을 클라이언트 통계와 요청 로그에 반영한다.
func (g *Gateway) record(req Request, resp *Response, err error, d time.Duration) {
	if s := g.clientStats[req.Client]; s != nil {
		s.Calls.Add(1)
		switch {
		case err == nil:
			s.OK.Add(1)
			if resp.Cached {
				s.Cached.Add(1)
			}
			s.PromptTokens.Add(int64(resp.Usage.PromptTokens))
			s.CompletionTokens.Add(int64(resp.Usage.CompletionTokens))
		case errors.Is(err, ErrClientLimited) || errors.Is(err, ErrRouteNotAllowed):
			s.Rejected.Add(1)
		default:
			s.Fail.Add(1)
		}
	}
	if g.log == nil {
		return
	}
	l := logLine{
		TS: time.Now().Format("2006-01-02 15:04:05.000"), Client: req.Client, Route: req.Route,
		OK: err == nil, LatencyMS: d.Milliseconds(),
	}
	if resp != nil {
		l.Provider, l.Model, l.Cached, l.Steps = resp.Provider, resp.Model, resp.Cached, len(resp.Attempts)
		l.PromptTokens, l.CompletionTokens, l.ReasoningTokens = resp.Usage.PromptTokens, resp.Usage.CompletionTokens, resp.Usage.ReasoningTokens
	}
	if err != nil {
		l.Error = err.Error()
		var ce *ChainError
		if errors.As(err, &ce) {
			l.Steps = len(ce.Attempts)
		}
	}
	b, _ := json.Marshal(l)
	g.log.mu.Lock()
	g.log.f.Write(append(b, '\n'))
	g.log.mu.Unlock()
}

// Close 는 요청 로그 파일을 닫는다.
func (g *Gateway) Close() error {
	if g.log == nil {
		return nil
	}
	return g.log.f.Close()
}
