package leaderelection

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	redsync "github.com/go-redsync/redsync/v4"
	goredis "github.com/go-redsync/redsync/v4/redis/goredis/v9"
	redis "github.com/redis/go-redis/v9"
)

type redisResourceLockFactory struct {
	client redis.UniversalClient
	rs     *redsync.Redsync
}

type redisResourceLock struct {
	mutex *redsync.Mutex
}

type redisLockValue struct {
	Identity string `json:"identity"`
	Token    string `json:"token"`
}

func NewLeaderElector(config Config) (*LeaderElector, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	return newLeaderElector(config, newRedisResourceLockFactory(config.RedisClient))
}

func newRedisResourceLockFactory(client redis.UniversalClient) *redisResourceLockFactory {
	pool := goredis.NewPool(client)
	return &redisResourceLockFactory{
		client: client,
		rs:     redsync.New(pool),
	}
}

func (f *redisResourceLockFactory) NewResourceLock(name string, identity string, leaseDuration time.Duration) (resourceLock, error) {
	value, err := newRedisLockValue(identity)
	if err != nil {
		return nil, err
	}

	mutex := f.rs.NewMutex(
		name,
		redsync.WithExpiry(leaseDuration),
		redsync.WithTries(1),
		redsync.WithGenValueFunc(func() (string, error) {
			return value, nil
		}),
	)
	return &redisResourceLock{mutex: mutex}, nil
}

func (f *redisResourceLockFactory) CurrentLeader(ctx context.Context, name string) (string, error) {
	value, err := f.client.Get(ctx, name).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return parseRedisLockValue(value), nil
}

func (l *redisResourceLock) Acquire(ctx context.Context) error {
	return l.mutex.TryLockContext(ctx)
}

func (l *redisResourceLock) Renew(ctx context.Context) (bool, error) {
	return l.mutex.ExtendContext(ctx)
}

func (l *redisResourceLock) Release(ctx context.Context) (bool, error) {
	return l.mutex.UnlockContext(ctx)
}

func newRedisLockValue(identity string) (string, error) {
	tokenBytes := make([]byte, 16)
	if _, err := rand.Read(tokenBytes); err != nil {
		return "", err
	}

	value := redisLockValue{
		Identity: identity,
		Token:    hex.EncodeToString(tokenBytes),
	}
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func parseRedisLockValue(value string) string {
	var decoded redisLockValue
	if err := json.Unmarshal([]byte(value), &decoded); err != nil {
		return value
	}
	if decoded.Identity == "" {
		return value
	}
	return decoded.Identity
}
