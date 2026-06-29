package leaderelection

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNewLeaderElectorValidatesConfig(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*LeaderElectionConfig)
		wantErr string
	}{
		{
			name: "nil lock",
			mutate: func(c *LeaderElectionConfig) {
				c.Lock = nil
			},
			wantErr: "Lock must not be nil",
		},
		{
			name: "lease duration is zero",
			mutate: func(c *LeaderElectionConfig) {
				c.LeaseDuration = 0
			},
			wantErr: "leaseDuration must be greater than zero",
		},
		{
			name: "renew deadline is zero",
			mutate: func(c *LeaderElectionConfig) {
				c.RenewDeadline = 0
			},
			wantErr: "renewDeadline must be greater than zero",
		},
		{
			name: "renew deadline equals lease duration",
			mutate: func(c *LeaderElectionConfig) {
				c.RenewDeadline = c.LeaseDuration
			},
			wantErr: "leaseDuration must be greater than renewDeadline",
		},
		{
			name: "retry period is zero",
			mutate: func(c *LeaderElectionConfig) {
				c.RetryPeriod = 0
			},
			wantErr: "retryPeriod must be greater than zero",
		},
		{
			name: "retry period exceeds renew deadline",
			mutate: func(c *LeaderElectionConfig) {
				c.RetryPeriod = c.RenewDeadline + time.Second
			},
			wantErr: "retryPeriod must be less than renewDeadline",
		},
		{
			name: "retry period equals renew deadline",
			mutate: func(c *LeaderElectionConfig) {
				c.RetryPeriod = c.RenewDeadline
			},
			wantErr: "retryPeriod must be less than renewDeadline",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validTestConfig()
			tt.mutate(&cfg)

			_, err := NewLeaderElector(cfg)
			if err == nil {
				t.Fatalf("expected error containing %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got %q", tt.wantErr, err.Error())
			}
		})
	}
}

func TestNewLeaderElectorAcceptsValidConfig(t *testing.T) {
	elector, err := NewLeaderElector(validTestConfig())
	if err != nil {
		t.Fatalf("expected valid config, got error: %v", err)
	}
	if elector.IsLeader() {
		t.Fatal("new elector should not start as leader")
	}
	if got := elector.GetLeader(); got != "" {
		t.Fatalf("new elector should not have observed leader, got %q", got)
	}
}

func validTestConfig() LeaderElectionConfig {
	return LeaderElectionConfig{
		Lock:          &fakeLock{identity: "node-a", renewResult: true, releaseResult: true},
		LeaseDuration: 30 * time.Second,
		RenewDeadline: 20 * time.Second,
		RetryPeriod:   5 * time.Second,
	}
}

type fakeLock struct {
	mu             sync.Mutex
	identity       string
	leader         string
	leaderWait     <-chan struct{}
	leaderStarted  chan<- struct{}
	acquireErrors  []error
	acquireCalls   int
	acquireWait    <-chan struct{}
	acquireStarted chan<- struct{}
	renewResult    bool
	renewErr       error
	renewWait      <-chan struct{}
	renewStarted   chan<- struct{}
	releaseResult  bool
	releaseErr     error
	releaseCalls   int
	releaseWait    <-chan struct{}
	releaseStarted chan<- struct{}
}

func (l *fakeLock) Identity() string { return l.identity }

func (l *fakeLock) CurrentLeader(ctx context.Context) (string, error) {
	l.mu.Lock()
	leader := l.leader
	leaderWait := l.leaderWait
	leaderStarted := l.leaderStarted
	l.mu.Unlock()

	if leaderStarted != nil {
		select {
		case leaderStarted <- struct{}{}:
		default:
		}
	}
	if leaderWait != nil {
		select {
		case <-leaderWait:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	return leader, nil
}

func (l *fakeLock) Acquire(ctx context.Context) error {
	l.mu.Lock()
	acquireStarted := l.acquireStarted
	acquireWait := l.acquireWait
	l.mu.Unlock()

	if acquireStarted != nil {
		select {
		case acquireStarted <- struct{}{}:
		default:
		}
	}
	if acquireWait != nil {
		select {
		case <-acquireWait:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	l.acquireCalls++
	if len(l.acquireErrors) == 0 {
		return nil
	}
	err := l.acquireErrors[0]
	l.acquireErrors = l.acquireErrors[1:]
	return err
}

func (l *fakeLock) Renew(ctx context.Context) (bool, error) {
	l.mu.Lock()
	renewStarted := l.renewStarted
	renewWait := l.renewWait
	l.mu.Unlock()

	if renewStarted != nil {
		select {
		case renewStarted <- struct{}{}:
		default:
		}
	}
	if renewWait != nil {
		select {
		case <-renewWait:
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	return l.renewResult, l.renewErr
}

func (l *fakeLock) Release(ctx context.Context) (bool, error) {
	l.mu.Lock()
	releaseStarted := l.releaseStarted
	releaseWait := l.releaseWait
	l.mu.Unlock()

	if releaseStarted != nil {
		select {
		case releaseStarted <- struct{}{}:
		default:
		}
	}
	if releaseWait != nil {
		select {
		case <-releaseWait:
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	l.releaseCalls++
	return l.releaseResult, l.releaseErr
}

func (l *fakeLock) acquireCallCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.acquireCalls
}

func (l *fakeLock) releaseCallCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.releaseCalls
}

func (l *fakeLock) setCurrentLeaderBlock(wait <-chan struct{}, started chan<- struct{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.leaderWait = wait
	l.leaderStarted = started
}

func (l *fakeLock) setAcquireBlock(wait <-chan struct{}, started chan<- struct{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.acquireWait = wait
	l.acquireStarted = started
}

func (l *fakeLock) setRenewBlock(wait <-chan struct{}, started chan<- struct{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.renewWait = wait
	l.renewStarted = started
}

func (l *fakeLock) setReleaseBlock(wait <-chan struct{}, started chan<- struct{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.releaseWait = wait
	l.releaseStarted = started
}

func TestRunAcquiresLeadershipAndStartsCallback(t *testing.T) {
	started := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := validTestConfig()
	cfg.RetryPeriod = time.Millisecond
	cfg.Callbacks.OnStartedLeading = func(context.Context) {
		close(started)
	}

	elector, err := NewLeaderElector(cfg)
	if err != nil {
		t.Fatalf("NewLeaderElector returned error: %v", err)
	}

	go elector.Run(ctx)

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for OnStartedLeading")
	}

	if !elector.IsLeader() {
		t.Fatal("elector should be leader after acquire succeeds")
	}
	if got := elector.GetLeader(); got != "node-a" {
		t.Fatalf("expected observed leader %q, got %q", "node-a", got)
	}
}

func TestRunRetriesAcquireUntilItSucceeds(t *testing.T) {
	started := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := validTestConfig()
	cfg.RetryPeriod = time.Millisecond
	lock := cfg.Lock.(*fakeLock)
	lock.acquireErrors = []error{errors.New("lock is held")}
	cfg.Callbacks.OnStartedLeading = func(context.Context) {
		close(started)
	}

	elector, err := NewLeaderElector(cfg)
	if err != nil {
		t.Fatalf("NewLeaderElector returned error: %v", err)
	}

	go elector.Run(ctx)

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for OnStartedLeading after retry")
	}

	if got := lock.acquireCallCount(); got < 2 {
		t.Fatalf("expected at least two acquire attempts, got %d", got)
	}
}

func TestRunReturnsWhenAcquireBlocksAndContextIsCanceled(t *testing.T) {
	acquireStarted := make(chan struct{}, 1)
	acquireWait := make(chan struct{})
	startedLeading := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())

	cfg := validTestConfig()
	cfg.RetryPeriod = time.Millisecond
	lock := cfg.Lock.(*fakeLock)
	lock.setAcquireBlock(acquireWait, acquireStarted)
	cfg.Callbacks.OnStartedLeading = func(context.Context) {
		startedLeading <- struct{}{}
	}

	elector, err := NewLeaderElector(cfg)
	if err != nil {
		t.Fatalf("NewLeaderElector returned error: %v", err)
	}

	done := make(chan struct{})
	go func() {
		elector.Run(ctx)
		close(done)
	}()

	select {
	case <-acquireStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Acquire to start")
	}

	cancel()

	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("Run did not return after cancel while Acquire was blocked")
	}

	if elector.IsLeader() {
		t.Fatal("elector should not enter leadership when Acquire is canceled")
	}

	select {
	case <-startedLeading:
		t.Fatal("OnStartedLeading should not run when Acquire is canceled")
	default:
	}
}

func TestRunReportsObservedLeaderBeforeSelfLeadership(t *testing.T) {
	leaders := make(chan string, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := validTestConfig()
	cfg.RetryPeriod = time.Millisecond
	cfg.Lock.(*fakeLock).leader = "node-b"
	cfg.Callbacks.OnNewLeader = func(identity string) {
		leaders <- identity
	}

	elector, err := NewLeaderElector(cfg)
	if err != nil {
		t.Fatalf("NewLeaderElector returned error: %v", err)
	}

	go elector.Run(ctx)

	select {
	case got := <-leaders:
		if got != "node-b" {
			t.Fatalf("expected first observed leader node-b, got %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for OnNewLeader")
	}
}

func TestRunReturnsWhenCurrentLeaderBlocksAndContextIsCanceled(t *testing.T) {
	currentLeaderStarted := make(chan struct{}, 1)
	currentLeaderWait := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())

	cfg := validTestConfig()
	cfg.RetryPeriod = time.Millisecond
	cfg.Lock.(*fakeLock).setCurrentLeaderBlock(currentLeaderWait, currentLeaderStarted)

	elector, err := NewLeaderElector(cfg)
	if err != nil {
		t.Fatalf("NewLeaderElector returned error: %v", err)
	}

	done := make(chan struct{})
	go func() {
		elector.Run(ctx)
		close(done)
	}()

	select {
	case <-currentLeaderStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for CurrentLeader to start")
	}

	cancel()

	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("Run did not return after cancel while CurrentLeader was blocked")
	}
}

func TestRunCancelDuringCurrentLeaderDoesNotAcquireLock(t *testing.T) {
	currentLeaderStarted := make(chan struct{}, 1)
	currentLeaderWait := make(chan struct{})
	startedLeading := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())

	cfg := validTestConfig()
	cfg.RetryPeriod = time.Millisecond
	lock := cfg.Lock.(*fakeLock)
	lock.setCurrentLeaderBlock(currentLeaderWait, currentLeaderStarted)
	cfg.Callbacks.OnStartedLeading = func(context.Context) {
		startedLeading <- struct{}{}
	}

	elector, err := NewLeaderElector(cfg)
	if err != nil {
		t.Fatalf("NewLeaderElector returned error: %v", err)
	}

	done := make(chan struct{})
	go func() {
		elector.Run(ctx)
		close(done)
	}()

	select {
	case <-currentLeaderStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for CurrentLeader to start")
	}

	cancel()

	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("Run did not return after cancel while CurrentLeader was blocked")
	}

	if got := lock.acquireCallCount(); got != 0 {
		t.Fatalf("expected no acquire attempts after cancel during CurrentLeader, got %d", got)
	}

	select {
	case <-startedLeading:
		t.Fatal("OnStartedLeading should not run after cancel during CurrentLeader")
	default:
	}
}

func TestGetLeaderRetainsLastObservedLeaderAfterShutdown(t *testing.T) {
	started := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())

	cfg := validTestConfig()
	cfg.RetryPeriod = time.Millisecond
	cfg.Callbacks.OnStartedLeading = func(context.Context) {
		close(started)
	}

	elector, err := NewLeaderElector(cfg)
	if err != nil {
		t.Fatalf("NewLeaderElector returned error: %v", err)
	}

	done := make(chan struct{})
	go func() {
		elector.Run(ctx)
		close(done)
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for started callback")
	}

	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Run to return")
	}

	if got := elector.GetLeader(); got != "node-a" {
		t.Fatalf("expected last observed leader %q after shutdown, got %q", "node-a", got)
	}
}

func TestRunCancelsLeadingContextAndStopsOnParentCancel(t *testing.T) {
	started := make(chan context.Context, 1)
	stopped := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())

	cfg := validTestConfig()
	cfg.RetryPeriod = time.Millisecond
	cfg.Callbacks.OnStartedLeading = func(ctx context.Context) {
		started <- ctx
	}
	cfg.Callbacks.OnStoppedLeading = func() {
		close(stopped)
	}

	elector, err := NewLeaderElector(cfg)
	if err != nil {
		t.Fatalf("NewLeaderElector returned error: %v", err)
	}

	go elector.Run(ctx)

	var leadingCtx context.Context
	select {
	case leadingCtx = <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for started callback")
	}

	cancel()

	select {
	case <-leadingCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for leading context cancellation")
	}

	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for stopped callback")
	}

	if elector.IsLeader() {
		t.Fatal("elector should not be leader after parent context cancel")
	}
}

// TestRunStopsImmediatelyWhenRenewReturnsDefinitiveLoss verifies that when Renew
// returns (false, nil) — meaning the lock is definitively gone (e.g. lease not
// found after etcd restart) — runLeading stops without waiting for the full
// RenewDeadline.  This prevents a split-brain window where a new leader has
// already acquired the lock but the old leader is still considered "leading".
func TestRunStopsImmediatelyWhenRenewReturnsDefinitiveLoss(t *testing.T) {
	started := make(chan struct{})
	stopped := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := validTestConfig()
	cfg.LeaseDuration = 5 * time.Second
	cfg.RenewDeadline = 2 * time.Second // long deadline — must NOT be reached
	cfg.RetryPeriod = 10 * time.Millisecond
	lock := cfg.Lock.(*fakeLock)
	cfg.Callbacks.OnStartedLeading = func(context.Context) {
		close(started)
	}
	cfg.Callbacks.OnStoppedLeading = func() {
		close(stopped)
	}

	elector, err := NewLeaderElector(cfg)
	if err != nil {
		t.Fatalf("NewLeaderElector returned error: %v", err)
	}

	go elector.Run(ctx)

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for started callback")
	}

	// Simulate etcd restart: Renew returns (false, nil) — definitive loss, no error.
	lock.mu.Lock()
	lock.renewResult = false
	lock.renewErr = nil
	lock.mu.Unlock()

	// Must stop well within RenewDeadline (2s); allow 500ms for scheduling.
	select {
	case <-stopped:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Run did not stop promptly on definitive lock loss (Renew returned false, nil)")
	}

	if elector.IsLeader() {
		t.Fatal("elector should not be leader after definitive lock loss")
	}
}

func TestRunStopsWhenRenewDeadlineExpires(t *testing.T) {
	started := make(chan struct{})
	stopped := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := validTestConfig()
	cfg.LeaseDuration = 50 * time.Millisecond
	cfg.RenewDeadline = 15 * time.Millisecond
	cfg.RetryPeriod = 5 * time.Millisecond
	lock := cfg.Lock.(*fakeLock)
	cfg.Callbacks.OnStartedLeading = func(context.Context) {
		close(started)
	}
	cfg.Callbacks.OnStoppedLeading = func() {
		close(stopped)
	}

	elector, err := NewLeaderElector(cfg)
	if err != nil {
		t.Fatalf("NewLeaderElector returned error: %v", err)
	}

	go elector.Run(ctx)

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for started callback")
	}

	lock.mu.Lock()
	lock.renewResult = false
	lock.renewErr = errors.New("renew failed")
	lock.mu.Unlock()

	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for stopped callback after renew deadline")
	}

	if elector.IsLeader() {
		t.Fatal("elector should not be leader after renew deadline expires")
	}
	if got := elector.GetLeader(); got != "node-a" {
		t.Fatalf("expected last observed leader %q after renew deadline, got %q", "node-a", got)
	}
}

func TestRunDoesNotReleaseLockOnRenewDeadlineEvenWhenConfigured(t *testing.T) {
	started := make(chan struct{})
	stopped := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := validTestConfig()
	cfg.LeaseDuration = 50 * time.Millisecond
	cfg.RenewDeadline = 15 * time.Millisecond
	cfg.RetryPeriod = 5 * time.Millisecond
	cfg.ReleaseOnCancel = true
	lock := cfg.Lock.(*fakeLock)
	cfg.Callbacks.OnStartedLeading = func(context.Context) {
		close(started)
	}
	cfg.Callbacks.OnStoppedLeading = func() {
		close(stopped)
	}

	elector, err := NewLeaderElector(cfg)
	if err != nil {
		t.Fatalf("NewLeaderElector returned error: %v", err)
	}

	go elector.Run(ctx)

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for started callback")
	}

	lock.mu.Lock()
	lock.renewResult = false
	lock.renewErr = errors.New("renew failed")
	lock.mu.Unlock()

	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for stopped callback after renew deadline")
	}

	if got := lock.releaseCallCount(); got != 0 {
		t.Fatalf("expected no release call on renew-deadline exit, got %d", got)
	}
}

func TestRunStopsWhenRenewBlocksPastDeadline(t *testing.T) {
	started := make(chan struct{})
	stopped := make(chan struct{})
	renewStarted := make(chan struct{}, 1)
	renewWait := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := validTestConfig()
	cfg.LeaseDuration = 50 * time.Millisecond
	cfg.RenewDeadline = 15 * time.Millisecond
	cfg.RetryPeriod = 5 * time.Millisecond
	lock := cfg.Lock.(*fakeLock)
	cfg.Callbacks.OnStartedLeading = func(context.Context) {
		close(started)
	}
	cfg.Callbacks.OnStoppedLeading = func() {
		close(stopped)
	}

	elector, err := NewLeaderElector(cfg)
	if err != nil {
		t.Fatalf("NewLeaderElector returned error: %v", err)
	}

	go elector.Run(ctx)

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for started callback")
	}

	lock.setRenewBlock(renewWait, renewStarted)

	select {
	case <-renewStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for renew to start")
	}

	select {
	case <-stopped:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("timed out waiting for stopped callback after renew deadline")
	}

	if elector.IsLeader() {
		t.Fatal("elector should not be leader after renew deadline expires")
	}
}

func TestRunReleasesLockOnCancelWhenConfigured(t *testing.T) {
	started := make(chan struct{})
	stopped := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())

	cfg := validTestConfig()
	cfg.RetryPeriod = time.Millisecond
	cfg.ReleaseOnCancel = true
	lock := cfg.Lock.(*fakeLock)
	cfg.Callbacks.OnStartedLeading = func(context.Context) {
		close(started)
	}
	cfg.Callbacks.OnStoppedLeading = func() {
		close(stopped)
	}

	elector, err := NewLeaderElector(cfg)
	if err != nil {
		t.Fatalf("NewLeaderElector returned error: %v", err)
	}

	go elector.Run(ctx)

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for started callback")
	}

	cancel()

	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for stopped callback")
	}

	if got := lock.releaseCallCount(); got != 1 {
		t.Fatalf("expected one release call, got %d", got)
	}
}

func TestRunReturnsWhenReleaseBlocksAfterCancel(t *testing.T) {
	started := make(chan struct{})
	releaseStarted := make(chan struct{}, 1)
	releaseWait := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())

	cfg := validTestConfig()
	cfg.RetryPeriod = 10 * time.Millisecond
	cfg.ReleaseOnCancel = true
	lock := cfg.Lock.(*fakeLock)
	cfg.Callbacks.OnStartedLeading = func(context.Context) {
		close(started)
	}

	elector, err := NewLeaderElector(cfg)
	if err != nil {
		t.Fatalf("NewLeaderElector returned error: %v", err)
	}

	done := make(chan struct{})
	go func() {
		elector.Run(ctx)
		close(done)
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for started callback")
	}

	lock.setReleaseBlock(releaseWait, releaseStarted)

	cancel()

	select {
	case <-releaseStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for release to start")
	}

	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("Run did not return after cancel while release was blocked")
	}
}

func TestCallbacksCanQueryElectorState(t *testing.T) {
	checked := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := validTestConfig()
	cfg.RetryPeriod = time.Millisecond

	var elector *LeaderElector
	cfg.Callbacks.OnStartedLeading = func(context.Context) {
		if !elector.IsLeader() {
			t.Error("callback should see elector as leader")
		}
		if got := elector.GetLeader(); got != "node-a" {
			t.Errorf("callback expected leader %q, got %q", "node-a", got)
		}
		close(checked)
	}

	var err error
	elector, err = NewLeaderElector(cfg)
	if err != nil {
		t.Fatalf("NewLeaderElector returned error: %v", err)
	}

	go elector.Run(ctx)

	select {
	case <-checked:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for callback state check")
	}
}

func TestBlockingOnNewLeaderDelaysLeadingTransition(t *testing.T) {
	onNewLeaderStarted := make(chan struct{})
	releaseOnNewLeader := make(chan struct{})
	startedLeading := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := validTestConfig()
	cfg.RetryPeriod = time.Millisecond
	cfg.Lock.(*fakeLock).leader = "node-b"
	firstCallback := true
	cfg.Callbacks.OnNewLeader = func(identity string) {
		if !firstCallback {
			return
		}
		firstCallback = false
		close(onNewLeaderStarted)
		<-releaseOnNewLeader
	}
	cfg.Callbacks.OnStartedLeading = func(context.Context) {
		close(startedLeading)
	}

	elector, err := NewLeaderElector(cfg)
	if err != nil {
		t.Fatalf("NewLeaderElector returned error: %v", err)
	}

	go elector.Run(ctx)

	select {
	case <-onNewLeaderStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for OnNewLeader to start")
	}

	select {
	case <-startedLeading:
		t.Fatal("OnStartedLeading should not run until blocking OnNewLeader returns")
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseOnNewLeader)

	select {
	case <-startedLeading:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for OnStartedLeading after OnNewLeader returned")
	}
}
