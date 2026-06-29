package leaderelection

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/hq0101/go-leaderelection/resourcelock"
)

// LeaderElectionConfig is the config used to create a LeaderElector.
type LeaderElectionConfig struct {
	// Lock is the resource lock implementation used for leader election.
	Lock resourcelock.Interface

	// LeaseDuration is the duration that non-leader candidates will wait to force
	// acquire leadership. Core clients default this value to 15 seconds.
	LeaseDuration time.Duration

	// RenewDeadline is the duration that the acting leader will retry refreshing
	// leadership before giving up. Core clients default this value to 10 seconds.
	RenewDeadline time.Duration

	// RetryPeriod is the duration the LeaderElector clients should wait between
	// tries of actions. Core clients default this value to 2 seconds.
	RetryPeriod time.Duration

	// ReleaseOnCancel specifies whether the lock should be released when the run
	// context is cancelled. If set to true, ensure all leader-only work has
	// completed before cancelling the context to avoid split-brain.
	ReleaseOnCancel bool

	// Callbacks are callbacks triggered during certain lifecycle events.
	Callbacks LeaderCallbacks

	// Name is the name of the resource lock for debugging purposes.
	Name string
}

// LeaderCallbacks are callbacks triggered during certain lifecycle events of
// the LeaderElector.
type LeaderCallbacks struct {
	// OnStartedLeading is called when this client starts leading.
	OnStartedLeading func(context.Context)
	// OnStoppedLeading is called when this client stops leading.
	OnStoppedLeading func()
	// OnNewLeader is called when a new leader is observed. It runs synchronously
	// on the elector control path and must return promptly.
	OnNewLeader func(identity string)
}

// LeaderElector is a leader election client.
type LeaderElector struct {
	config LeaderElectionConfig

	mu       sync.RWMutex
	leader   string
	isLeader bool
}

// NewLeaderElector creates a LeaderElector from a LeaderElectionConfig.
func NewLeaderElector(lec LeaderElectionConfig) (*LeaderElector, error) {
	if lec.Lock == nil {
		return nil, errors.New("Lock must not be nil")
	}
	if lec.LeaseDuration <= 0 {
		return nil, errors.New("leaseDuration must be greater than zero")
	}
	if lec.RenewDeadline <= 0 {
		return nil, errors.New("renewDeadline must be greater than zero")
	}
	if lec.LeaseDuration <= lec.RenewDeadline {
		return nil, errors.New("leaseDuration must be greater than renewDeadline")
	}
	if lec.RetryPeriod <= 0 {
		return nil, errors.New("retryPeriod must be greater than zero")
	}
	if lec.RetryPeriod >= lec.RenewDeadline {
		return nil, errors.New("retryPeriod must be less than renewDeadline")
	}
	return &LeaderElector{config: lec}, nil
}

type renewResult struct {
	ok  bool
	err error
}

// Run starts the leader election loop. Run will not return before the leader
// election loop is stopped by ctx or it has stopped holding the leader lease.
func (le *LeaderElector) Run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		if leader, err := le.config.Lock.CurrentLeader(ctx); err == nil && leader != "" {
			le.setObservedLeader(leader)
		}
		if ctx.Err() != nil {
			return
		}

		if err := le.config.Lock.Acquire(ctx); err != nil {
			if !sleepContext(ctx, le.config.RetryPeriod) {
				return
			}
			continue
		}

		le.setObservedLeader(le.config.Lock.Identity())
		le.setLeaderStatus(true)
		le.runLeading(ctx, le.config.Lock)
		return
	}
}

// GetLeader returns the last observed non-empty leader identity. The value is
// not cleared on shutdown or leadership loss, so it remains readable after
// Run returns.
func (le *LeaderElector) GetLeader() string {
	le.mu.RLock()
	defer le.mu.RUnlock()
	return le.leader
}

// IsLeader returns true if this client is currently the leader.
func (le *LeaderElector) IsLeader() bool {
	le.mu.RLock()
	defer le.mu.RUnlock()
	return le.isLeader
}

func (le *LeaderElector) setObservedLeader(identity string) {
	if identity == "" {
		return
	}
	var callback func(string)
	changed := false

	le.mu.Lock()
	if le.leader != identity {
		le.leader = identity
		changed = true
		callback = le.config.Callbacks.OnNewLeader
	}
	le.mu.Unlock()

	if changed && callback != nil {
		callback(identity)
	}
}

func (le *LeaderElector) setLeaderStatus(isLeader bool) {
	le.mu.Lock()
	le.isLeader = isLeader
	le.mu.Unlock()
}

func (le *LeaderElector) runLeading(ctx context.Context, lock resourcelock.Interface) {
	leadingCtx, cancelLeading := context.WithCancel(ctx)
	stopped := false
	renewResults := make(chan renewResult, 1)
	renewInFlight := false

	defer func() {
		cancelLeading()
		le.setLeaderStatus(false)
		if le.config.ReleaseOnCancel && ctx.Err() != nil {
			le.releaseOnCancel(lock)
		}
		if callback := le.config.Callbacks.OnStoppedLeading; callback != nil {
			callback()
		}
	}()

	if callback := le.config.Callbacks.OnStartedLeading; callback != nil {
		go callback(leadingCtx)
	}

	ticker := time.NewTicker(le.config.RetryPeriod)
	defer ticker.Stop()

	deadline := time.NewTimer(le.config.RenewDeadline)
	defer deadline.Stop()

	for !stopped {
		select {
		case <-ctx.Done():
			stopped = true
		case <-deadline.C:
			stopped = true
		case result := <-renewResults:
			renewInFlight = false
			if result.ok && result.err == nil {
				resetTimer(deadline, le.config.RenewDeadline)
			} else if !result.ok && result.err == nil {
				// Lock definitively lost (lease not found, TTL expired, or ownership lost).
				// Stop immediately rather than waiting for the deadline to avoid split-brain.
				stopped = true
			}
		case <-ticker.C:
			if renewInFlight {
				continue
			}
			renewInFlight = true
			go func() {
				ok, err := lock.Renew(leadingCtx)
				select {
				case renewResults <- renewResult{ok: ok, err: err}:
				case <-leadingCtx.Done():
				}
			}()
		}
	}
}

func (le *LeaderElector) releaseOnCancel(lock resourcelock.Interface) {
	timeout := le.config.RetryPeriod
	if le.config.RenewDeadline > 0 && le.config.RenewDeadline < timeout {
		timeout = le.config.RenewDeadline
	}
	releaseCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = lock.Release(releaseCtx)
	}()

	select {
	case <-done:
	case <-releaseCtx.Done():
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
