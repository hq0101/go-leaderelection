package leaderelection

import (
	"context"
	"errors"
	"time"

	redis "github.com/redis/go-redis/v9"
)

type Config struct {
	LockName        string
	Identity        string
	LeaseDuration   time.Duration
	RenewDeadline   time.Duration
	RetryPeriod     time.Duration
	ReleaseOnCancel bool
	RedisClient     redis.UniversalClient
	Callbacks       Callbacks
}

type Callbacks struct {
	OnStartedLeading func(context.Context)
	OnStoppedLeading func()
	OnNewLeader      func(identity string)
}

func (c Config) validate() error {
	if c.LockName == "" {
		return errors.New("lock name is required")
	}
	if c.Identity == "" {
		return errors.New("identity is required")
	}
	if c.RedisClient == nil {
		return errors.New("redis client is required")
	}
	if c.LeaseDuration <= 0 {
		return errors.New("lease duration must be greater than zero")
	}
	if c.RenewDeadline <= 0 {
		return errors.New("renew deadline must be greater than zero")
	}
	if c.RenewDeadline >= c.LeaseDuration {
		return errors.New("renew deadline must be less than lease duration")
	}
	if c.RetryPeriod <= 0 {
		return errors.New("retry period must be greater than zero")
	}
	if c.RetryPeriod > c.RenewDeadline {
		return errors.New("retry period must be less than or equal to renew deadline")
	}
	return nil
}
