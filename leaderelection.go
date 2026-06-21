package leaderelection

import (
	"context"
	"errors"
	"sync"
	"time"
)

type LeaderElector struct {
	config      Config
	lockFactory resourceLockFactory

	mu       sync.RWMutex
	leader   string
	isLeader bool
}

func newLeaderElector(config Config, factory resourceLockFactory) (*LeaderElector, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	if factory == nil {
		return nil, errors.New("resource lock factory is required")
	}
	return &LeaderElector{
		config:      config,
		lockFactory: factory,
	}, nil
}

func (e *LeaderElector) Run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		e.observeLeader(ctx)

		lock, err := e.lockFactory.NewResourceLock(e.config.LockName, e.config.Identity, e.config.LeaseDuration)
		if err != nil {
			if !sleepContext(ctx, e.config.RetryPeriod) {
				return
			}
			continue
		}

		if err := lock.Acquire(ctx); err != nil {
			if !sleepContext(ctx, e.config.RetryPeriod) {
				return
			}
			continue
		}

		e.setObservedLeader(e.config.Identity)
		e.setLeaderStatus(true)
		e.runLeading(ctx, lock)
		return
	}
}

func (e *LeaderElector) IsLeader() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.isLeader
}

func (e *LeaderElector) GetLeader() string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.leader
}

func (e *LeaderElector) observeLeader(ctx context.Context) {
	leader, err := e.lockFactory.CurrentLeader(ctx, e.config.LockName)
	if err != nil || leader == "" {
		return
	}
	e.setObservedLeader(leader)
}

func (e *LeaderElector) setObservedLeader(identity string) {
	if identity == "" {
		return
	}

	var callback func(string)
	changed := false

	e.mu.Lock()
	if e.leader != identity {
		e.leader = identity
		changed = true
		callback = e.config.Callbacks.OnNewLeader
	}
	e.mu.Unlock()

	if changed && callback != nil {
		callback(identity)
	}
}

func (e *LeaderElector) setLeaderStatus(isLeader bool) {
	e.mu.Lock()
	e.isLeader = isLeader
	e.mu.Unlock()
}

func (e *LeaderElector) runLeading(ctx context.Context, lock resourceLock) {
	leadingCtx, cancelLeading := context.WithCancel(ctx)
	stopped := false

	defer func() {
		cancelLeading()
		e.setLeaderStatus(false)
		if e.config.ReleaseOnCancel {
			_, _ = lock.Release(context.Background())
		}
		if callback := e.config.Callbacks.OnStoppedLeading; callback != nil {
			callback()
		}
	}()

	if callback := e.config.Callbacks.OnStartedLeading; callback != nil {
		go callback(leadingCtx)
	}

	ticker := time.NewTicker(e.config.RetryPeriod)
	defer ticker.Stop()

	deadline := time.NewTimer(e.config.RenewDeadline)
	defer deadline.Stop()

	for !stopped {
		select {
		case <-ctx.Done():
			stopped = true
		case <-deadline.C:
			stopped = true
		case <-ticker.C:
			ok, err := lock.Renew(ctx)
			if ok && err == nil {
				resetTimer(deadline, e.config.RenewDeadline)
			}
		}
	}
}

func sleepContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func resetTimer(timer *time.Timer, duration time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(duration)
}
