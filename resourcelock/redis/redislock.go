package redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/hq0101/go-leaderelection/resourcelock"

	redsync "github.com/go-redsync/redsync/v4"
	goredis "github.com/go-redsync/redsync/v4/redis/goredis/v9"
	redis "github.com/redis/go-redis/v9"
)

var _ resourcelock.Interface = (*RedisLock)(nil)

type RedisLock struct {
	client        redis.UniversalClient
	rs            *redsync.Redsync
	name          string
	identity      string
	leaseDuration time.Duration

	mu    sync.Mutex
	mutex *redsync.Mutex
}

type lockValue struct {
	Identity string `json:"identity"`
	Token    string `json:"token"`
}

// New returns a RedisLock backed by go-redis and redsync.
// Returns nil if client is nil.
func New(client redis.UniversalClient, name, identity string, leaseDuration time.Duration) *RedisLock {
	if client == nil {
		return nil
	}
	pool := goredis.NewPool(client)
	return &RedisLock{
		client:        client,
		rs:            redsync.New(pool),
		name:          name,
		identity:      identity,
		leaseDuration: leaseDuration,
	}
}

func (l *RedisLock) Identity() string { return l.identity }

func (l *RedisLock) Acquire(ctx context.Context) error {
	value, err := newLockValue(l.identity)
	if err != nil {
		return err
	}
	mutex := l.rs.NewMutex(
		l.name,
		redsync.WithExpiry(l.leaseDuration),
		redsync.WithTries(1),
		redsync.WithGenValueFunc(func() (string, error) { return value, nil }),
	)
	if err := mutex.TryLockContext(ctx); err != nil {
		return err
	}
	l.mu.Lock()
	l.mutex = mutex
	l.mu.Unlock()
	return nil
}

func (l *RedisLock) Renew(ctx context.Context) (bool, error) {
	l.mu.Lock()
	m := l.mutex
	l.mu.Unlock()
	if m == nil {
		return false, nil
	}
	return m.ExtendContext(ctx)
}

func (l *RedisLock) Release(ctx context.Context) (bool, error) {
	l.mu.Lock()
	m := l.mutex
	l.mutex = nil
	l.mu.Unlock()
	if m == nil {
		return false, nil
	}
	return m.UnlockContext(ctx)
}

func (l *RedisLock) CurrentLeader(ctx context.Context) (string, error) {
	value, err := l.client.Get(ctx, l.name).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return parseLockValue(value), nil
}

func newLockValue(identity string) (string, error) {
	tokenBytes := make([]byte, 16)
	if _, err := rand.Read(tokenBytes); err != nil {
		return "", err
	}
	value := lockValue{
		Identity: identity,
		Token:    hex.EncodeToString(tokenBytes),
	}
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func parseLockValue(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	if trimmed[0] != '{' {
		return value
	}
	var decoded lockValue
	if err := json.Unmarshal([]byte(trimmed), &decoded); err != nil {
		if !strings.Contains(trimmed, ":") && !strings.Contains(trimmed, "\"") {
			return value
		}
		return ""
	}
	if decoded.Identity == "" {
		return ""
	}
	return decoded.Identity
}
