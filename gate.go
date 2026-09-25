package llmgw

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// 게이트가 요청을 받지 않은 이유. 게이트웨이는 이 경우 재시도 없이 다음 step 으로 넘긴다.
var (
	ErrQueueFull    = errors.New("대기열 가득 참")
	ErrQueueTimeout = errors.New("대기열 대기 시간 초과")
	ErrBreakerOpen  = errors.New("서킷 브레이커 열림")
)

// gate 는 프로바이더 하나의 입구다. 들어오는 순서대로 ①대기열 자리 ②동시 처리 슬롯 ③분당 한도 토큰을
// 얻어야 호출할 수 있다. 셋 다 대기열 제한 시간 안에 얻지 못하면 포기한다.
type gate struct {
	maxQueue     int
	queueTimeout time.Duration

	slots chan struct{} // 동시 처리 세마포어(nil = 무제한)

	mu       sync.Mutex
	interval time.Duration // 요청 간 최소 간격 = 1분 / rpm (0 = 무제한)
	next     time.Time     // 다음 요청이 나갈 수 있는 시각

	breakerFailures int
	breakerCooldown time.Duration
	consecFail      int
	openUntil       time.Time

	waiting  atomic.Int64
	inFlight atomic.Int64
	stats    providerStats
}

type providerStats struct {
	Calls, OK, Fail, QueueRejects, QueueTimeouts, BreakerSkips atomic.Int64
}

func newGate(p *ProviderConfig) *gate {
	g := &gate{
		maxQueue:        p.MaxQueue,
		queueTimeout:    p.QueueTimeout.Duration,
		breakerFailures: p.BreakerFailures,
		breakerCooldown: p.BreakerCooldown.Duration,
	}
	if p.MaxConcurrency > 0 {
		g.slots = make(chan struct{}, p.MaxConcurrency)
	}
	if p.RPM > 0 {
		g.interval = time.Minute / time.Duration(p.RPM)
	}
	return g
}

// acquire 는 호출 자격을 얻을 때까지 기다린다. 성공하면 release 를 반드시 호출해야 한다.
func (g *gate) acquire(ctx context.Context) (release func(), err error) {
	if g.breakerOpen() {
		g.stats.BreakerSkips.Add(1)
		return nil, ErrBreakerOpen
	}
	if g.maxQueue > 0 && g.waiting.Load() >= int64(g.maxQueue) {
		g.stats.QueueRejects.Add(1)
		return nil, ErrQueueFull
	}
	g.waiting.Add(1)
	defer g.waiting.Add(-1)

	wctx, cancel := context.WithTimeout(ctx, g.queueTimeout)
	defer cancel()

	if g.slots != nil {
		select {
		case g.slots <- struct{}{}:
		case <-wctx.Done():
			return nil, g.waitErr(ctx)
		}
	}
	releaseSlot := func() {
		if g.slots != nil {
			<-g.slots
		}
	}
	if err := g.waitRate(wctx); err != nil {
		releaseSlot()
		return nil, g.waitErr(ctx)
	}
	g.inFlight.Add(1)
	g.stats.Calls.Add(1)
	var once sync.Once
	return func() {
		once.Do(func() {
			g.inFlight.Add(-1)
			releaseSlot()
		})
	}, nil
}

// waitErr 는 대기 중단 사유를 가른다: 호출자 ctx 가 끝났으면 그 에러, 아니면 대기열 시간 초과.
func (g *gate) waitErr(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	g.stats.QueueTimeouts.Add(1)
	return ErrQueueTimeout
}

// waitRate 는 분당 한도를 요청 간 고정 간격으로 지킨다(버스트 없음). 자리를 먼저 예약하고 그 시각까지 잔다.
func (g *gate) waitRate(ctx context.Context) error {
	if g.interval == 0 {
		return nil
	}
	g.mu.Lock()
	now := time.Now()
	at := g.next
	if at.Before(now) {
		at = now
	}
	g.next = at.Add(g.interval)
	g.mu.Unlock()

	d := time.Until(at)
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		// 예약한 자리를 반납한다(뒤에 예약한 사람이 없을 때만 안전하게 되돌릴 수 있다).
		g.mu.Lock()
		if g.next.Equal(at.Add(g.interval)) {
			g.next = at
		}
		g.mu.Unlock()
		return ctx.Err()
	}
}

// deferRate 는 429 의 Retry-After 를 받았을 때 이후 요청 전체를 그 시각 뒤로 민다.
func (g *gate) deferRate(until time.Time) {
	g.mu.Lock()
	if until.After(g.next) {
		g.next = until
	}
	g.mu.Unlock()
}

func (g *gate) breakerOpen() bool {
	if g.breakerFailures <= 0 {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return time.Now().Before(g.openUntil)
}

// reportNeutral 은 provider 탓이 아닌 실패(호출자 요청 오류·응답 검증 실패)를 통계에만 반영한다.
// 서킷 브레이커의 연속 실패 횟수는 건드리지 않는다.
func (g *gate) reportNeutral() { g.stats.Fail.Add(1) }

// report 는 호출 결과를 서킷 브레이커와 통계에 반영한다.
func (g *gate) report(ok bool) {
	if ok {
		g.stats.OK.Add(1)
	} else {
		g.stats.Fail.Add(1)
	}
	if g.breakerFailures <= 0 {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if ok {
		g.consecFail = 0
		return
	}
	g.consecFail++
	if g.consecFail >= g.breakerFailures {
		g.openUntil = time.Now().Add(g.breakerCooldown)
		g.consecFail = 0
	}
}
