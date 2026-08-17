package worker

import (
	"context"
	"sync"

	"golang.org/x/time/rate"
)

// actionLimiter enforces the requests_per_second and max_concurrency caps
// configured per provider_code+provider_action (DESIGN.md §10). A limiter
// with rps<=0 or maxConcurrency<=0 skips that dimension entirely.
type actionLimiter struct {
	limiter *rate.Limiter
	sem     chan struct{}
}

// Limiters holds one actionLimiter per provider action, shared by every
// goroutine in this worker process. It only bounds this process's own
// concurrency/rate — see DESIGN.md §10 on using Redis for a strict
// cross-process global QPS cap if that is ever required.
type Limiters struct {
	mu sync.RWMutex
	m  map[string]*actionLimiter
}

func NewLimiters() *Limiters {
	return &Limiters{m: make(map[string]*actionLimiter)}
}

func (l *Limiters) Register(key string, requestsPerSecond float64, maxConcurrency int) {
	al := &actionLimiter{}
	if requestsPerSecond > 0 {
		burst := int(requestsPerSecond)
		if burst < 1 {
			burst = 1
		}
		al.limiter = rate.NewLimiter(rate.Limit(requestsPerSecond), burst)
	}
	if maxConcurrency > 0 {
		al.sem = make(chan struct{}, maxConcurrency)
	}
	l.mu.Lock()
	l.m[key] = al
	l.mu.Unlock()
}

// Acquire blocks until both the rate limit and the concurrency slot allow
// proceeding for key, or ctx is canceled. The returned func releases the
// concurrency slot and must always be called.
func (l *Limiters) Acquire(ctx context.Context, key string) (func(), error) {
	l.mu.RLock()
	al, ok := l.m[key]
	l.mu.RUnlock()
	if !ok {
		return func() {}, nil
	}
	if al.limiter != nil {
		if err := al.limiter.Wait(ctx); err != nil {
			return nil, err
		}
	}
	if al.sem != nil {
		select {
		case al.sem <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return func() { <-al.sem }, nil
	}
	return func() {}, nil
}
