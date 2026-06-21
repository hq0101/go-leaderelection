package leaderelection

import (
	"context"
	"time"
)

type resourceLock interface {
	Acquire(context.Context) error
	Renew(context.Context) (bool, error)
	Release(context.Context) (bool, error)
}

type resourceLockFactory interface {
	NewResourceLock(name string, identity string, leaseDuration time.Duration) (resourceLock, error)
	CurrentLeader(ctx context.Context, name string) (string, error)
}
