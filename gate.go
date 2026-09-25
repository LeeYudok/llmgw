package llmgw

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// 게이트가 요청을 받지 않은 이유. 게이트웨이는 이 경우 재시도 없이 다음 step 으로 넘긴다.
var (
	ErrQueueFull    = errors.New("대기열 가득 참")
	ErrQueueTimeout = errors.New("대기열 대기 시간 초과")
	ErrBreakerOpen  = errors.New("서킷 브레이커 열림")
	// ErrWaitTooLong 은 예상 대기 시간이 step 의 max_wait 를 넘어 기다리지 않고 넘긴 경우다.
	ErrWaitTooLong = errors.New("예상 대기 초과")
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
	tripped         bool      // 브레이커가 열린 적이 있고 아직 성공으로 닫히지 않았다
	openUntil       time.Time // 이 시각까지는 모두 막는다
	probeUntil      time.Time // half-open 시험 요청이 진행 중인 동안 다른 요청을 막는다

	waiting  atomic.Int64
	blocked  atomic.Int64 // 동시 처리 슬롯을 기다리는 요청 수(waiting 의 일부)
	inFlight atomic.Int64
	stats    providerStats
	lat      latencyWindow
}

type providerStats struct {
	Calls, OK, Fail, QueueRejects, QueueTimeouts, BreakerSkips, EarlySpills atomic.Int64
}

// latencyWindow 는 최근 호출 소요 시간을 고정 크기 링 버퍼에 담는다(대기 예측·통계용).
type latencyWindow struct {
	mu   sync.Mutex
	buf  [256]time.Duration
	n    int // 채워진 칸 수
	next int
	sum  time.Duration
}

func (w *latencyWindow) observe(d time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.n == len(w.buf) {
		w.sum -= w.buf[w.next]
	} else {
		w.n++
	}
	w.buf[w.next] = d
	w.sum += d
	w.next = (w.next + 1) % len(w.buf)
}

func (w *latencyWindow) mean() time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.n == 0 {
		return 0
	}
	return w.sum / time.Duration(w.n)
}

// percentiles 는 최근 호출의 p50·p95 다. 기록이 없으면 0.
func (w *latencyWindow) percentiles() (p50, p95 time.Duration) {
	w.mu.Lock()
	s := slices.Clone(w.buf[:w.n])
	w.mu.Unlock()
	if len(s) == 0 {
		return 0, 0
	}
	slices.Sort(s)
	return s[(len(s)-1)*50/100], s[(len(s)-1)*95/100]
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

// permit 은 게이트를 통과한 요청 한 건의 자격이다. 호출이 끝나면 release 하고, 결과는 report 로 알린다.
// probe 는 half-open 에서 시험 요청으로 들어온 요청인지다 — 그 결과만 브레이커를 닫거나 다시 연다.
type permit struct {
	g     *gate
	probe bool
	once  sync.Once
}

func (p *permit) release() {
	p.once.Do(func() {
		p.g.inFlight.Add(-1)
		if p.g.slots != nil {
			<-p.g.slots
		}
	})
}

func (p *permit) report(ok bool) { p.g.report(ok, p.probe) }
func (p *permit) reportNeutral() { p.g.reportNeutral(p.probe) }

// acquire 는 호출 자격을 얻을 때까지 기다린다. 성공하면 release 를 반드시 호출해야 한다.
// 결과를 브레이커에 알리지 않는 곳(client 게이트)에서 쓴다.
func (g *gate) acquire(ctx context.Context) (release func(), err error) {
	pm, err := g.acquireWithin(ctx, 0)
	if err != nil {
		return nil, err
	}
	return pm.release, nil
}

// acquireWithin 은 acquire 와 같되, maxWait(>0)이 있으면 예상 대기가 그보다 길 때 기다리지 않고 바로
// ErrWaitTooLong 을 돌려주고, 실제 대기도 maxWait 로 자른다(queue_timeout 이 더 짧으면 그쪽).
func (g *gate) acquireWithin(ctx context.Context, maxWait time.Duration) (pm *permit, err error) {
	admit, probe := g.breakerAdmit()
	if !admit {
		g.stats.BreakerSkips.Add(1)
		return nil, ErrBreakerOpen
	}
	if probe {
		// 시험 자리를 얻었는데 호출까지 못 가면(대기열·max_wait·시간 초과) 자리를 돌려준다.
		defer func() {
			if err != nil {
				g.releaseProbe()
			}
		}()
	}
	if g.maxQueue > 0 && g.waiting.Load() >= int64(g.maxQueue) {
		g.stats.QueueRejects.Add(1)
		return nil, ErrQueueFull
	}
	if maxWait > 0 {
		if est := g.estimateWait(); est > maxWait {
			g.stats.EarlySpills.Add(1)
			return nil, fmt.Errorf("%w: 예상 %s > max_wait %s", ErrWaitTooLong, est.Round(time.Millisecond), maxWait)
		}
	}
	g.waiting.Add(1)
	defer g.waiting.Add(-1)

	timeout := g.queueTimeout
	capped := maxWait > 0 && maxWait < timeout // max_wait 가 대기 시간을 줄였는가
	if capped {
		timeout = maxWait
	}
	stop := func() error {
		if capped && ctx.Err() == nil {
			g.stats.EarlySpills.Add(1)
			return fmt.Errorf("%w: max_wait %s 동안 자리 없음", ErrWaitTooLong, maxWait)
		}
		return g.waitErr(ctx)
	}
	wctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if g.slots != nil {
		g.blocked.Add(1)
		select {
		case g.slots <- struct{}{}:
			g.blocked.Add(-1)
		case <-wctx.Done():
			g.blocked.Add(-1)
			return nil, stop()
		}
	}
	if err := g.waitRate(wctx); err != nil {
		if g.slots != nil {
			<-g.slots
		}
		return nil, stop()
	}
	g.inFlight.Add(1)
	g.stats.Calls.Add(1)
	return &permit{g: g, probe: probe}, nil
}

// estimateWait 는 지금 들어온 요청이 호출을 시작하기까지 기다릴 시간을 어림한다.
//   - 분당 한도: 이미 예약된 다음 발송 시각까지 + 슬롯을 기다리는 요청 수 × 요청 간격
//   - 동시 처리: 앞에 밀린 요청 수 ÷ 동시 처리 수 × 최근 평균 소요 시간(기록이 없으면 0 으로 본다)
//
// 둘 중 긴 쪽을 쓴다. 정확한 값이 아니라 "기다려 봐야 소용없다"를 미리 알아채는 용도다.
func (g *gate) estimateWait() time.Duration {
	var d time.Duration
	blocked := g.blocked.Load()
	g.mu.Lock()
	r := time.Until(g.next) // 분당 한도 예약, 또는 429 Retry-After 로 밀린 시각
	g.mu.Unlock()
	d = max(r, 0) + time.Duration(blocked)*g.interval
	if g.slots != nil {
		c := int64(cap(g.slots))
		if ahead := int64(len(g.slots)) + blocked + 1 - c; ahead > 0 {
			if avg := g.lat.mean(); avg > 0 {
				d = max(d, time.Duration(ahead)*avg/time.Duration(c))
			}
		}
	}
	return d
}

// admits 는 지금 요청을 받을 수 있는 상태인지다(브레이커가 닫혀 있고 대기열에 자리가 있다).
func (g *gate) admits() bool {
	return !g.breakerBlocked() && (g.maxQueue == 0 || g.waiting.Load() < int64(g.maxQueue))
}

// expectedFinish 는 지금 들어온 요청이 끝날 때까지의 예상 시간(대기 + 최근 평균 소요)이다.
// 소요 시간 기록이 없으면 ok=false.
func (g *gate) expectedFinish() (d time.Duration, ok bool) {
	avg := g.lat.mean()
	if avg == 0 {
		return 0, false
	}
	return g.estimateWait() + avg, true
}

// waitErr 는 대기 중단 사유를 가른다: 호출자 ctx 가 끝났으면 그 에러, 아니면 대기열 시간 초과.
func (g *gate) waitErr(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	g.stats.QueueTimeouts.Add(1)
	return ErrQueueTimeout
}

// errRateBeyondDeadline 은 분당 한도 자리가 대기 마감보다 뒤라서 예약하지 않았다는 뜻이다.
var errRateBeyondDeadline = errors.New("분당 한도 자리가 대기 마감 뒤")

// waitRate 는 분당 한도를 요청 간 고정 간격으로 지킨다(버스트 없음). 자리를 먼저 예약하고 그 시각까지 잔다.
// rpm 이 없어도 429 Retry-After 로 밀린 시각(next)까지는 기다린다. 예약할 자리가 대기 마감(ctx deadline)보다
// 뒤면 예약하지 않고 바로 돌려준다 — 기다려도 못 쓸 자리를 붙잡아 뒤 요청을 막지 않기 위해서다.
func (g *gate) waitRate(ctx context.Context) error {
	g.mu.Lock()
	now := time.Now()
	at := g.next
	if at.Before(now) {
		at = now
	}
	if dl, ok := ctx.Deadline(); ok && at.After(dl) {
		g.mu.Unlock()
		return errRateBeyondDeadline
	}
	if g.interval > 0 {
		g.next = at.Add(g.interval)
	}
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
		if g.interval > 0 {
			g.mu.Lock()
			if g.next.Equal(at.Add(g.interval)) {
				g.next = at
			}
			g.mu.Unlock()
		}
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

// breakerAdmit 은 브레이커가 요청을 들여보내는지다. 열린 뒤 쿨다운이 지나면(half-open) 시험 요청 한 건만
// 들여보내고, 그 결과가 나올 때까지(최대 쿨다운 한 번) 나머지는 계속 막는다.
func (g *gate) breakerAdmit() (admit, probe bool) {
	if g.breakerFailures <= 0 {
		return true, false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.tripped {
		return true, false
	}
	now := time.Now()
	if now.Before(g.openUntil) || now.Before(g.probeUntil) {
		return false, false
	}
	g.probeUntil = now.Add(g.breakerCooldown) // 시험 요청 자리. 결과가 안 오면 이 시각 뒤 다른 요청이 시험한다
	return true, true
}

// releaseProbe 는 시험 요청 자리를 돌려준다(시험 요청이 판정 없이 끝났을 때). 다음 요청이 다시 시험한다.
func (g *gate) releaseProbe() {
	g.mu.Lock()
	g.probeUntil = time.Time{}
	g.mu.Unlock()
}

// breakerBlocked 는 지금 요청이 브레이커에 막힐지다(시험 자리를 쓰지 않는다).
func (g *gate) breakerBlocked() bool {
	if g.breakerFailures <= 0 {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	now := time.Now()
	return g.tripped && (now.Before(g.openUntil) || now.Before(g.probeUntil))
}

// reportNeutral 은 provider 탓이 아닌 실패(호출자 요청 오류·응답 검증 실패)를 통계에만 반영한다.
// 서킷 브레이커의 연속 실패 횟수는 건드리지 않는다. 시험 요청이었다면 판정 없이 끝난 것이므로 자리를 돌려준다.
func (g *gate) reportNeutral(probe bool) {
	g.stats.Fail.Add(1)
	if probe {
		g.releaseProbe()
	}
}

// report 는 호출 결과를 서킷 브레이커와 통계에 반영한다. 브레이커가 열린 동안에는 시험 요청(probe)의
// 결과만 센다 — 열리기 전에 들어가 있던 요청이 늦게 성공해도 쿨다운 중에 닫히지 않는다.
func (g *gate) report(ok, probe bool) {
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
	if g.tripped && !probe {
		return
	}
	if ok {
		g.consecFail = 0
		g.tripped = false
		g.probeUntil = time.Time{}
		return
	}
	g.consecFail++
	// half-open 시험 요청이 실패하면 기다리지 않고 바로 다시 연다.
	if probe || g.consecFail >= g.breakerFailures {
		g.tripped = true
		g.openUntil = time.Now().Add(g.breakerCooldown)
		g.probeUntil = time.Time{}
		g.consecFail = 0
	}
}
