package resourcelock

import "context"

// Interface is the backend contract for leader election locks.
//
// Each instance is stateful: Acquire stores the acquired session so that
// subsequent Renew and Release calls operate on it. All methods must honor
// ctx.Done() promptly.
type Interface interface {
	// Acquire attempts to acquire leadership. On success the instance holds
	// the session for Renew and Release.
	Acquire(ctx context.Context) error

	// Renew attempts to extend the acquired session. Returns (false, nil)
	// if the session is no longer held.
	Renew(ctx context.Context) (bool, error)

	// Release relinquishes the held session. Returns (false, nil) if not held.
	Release(ctx context.Context) (bool, error)

	// Identity returns the holder identity this lock was configured with.
	Identity() string

	// CurrentLeader returns the currently observable leader identity, or an
	// empty string if no leader is currently visible.
	CurrentLeader(ctx context.Context) (string, error)
}
