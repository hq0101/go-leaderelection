package consul

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/hq0101/go-leaderelection/resourcelock"

	consulapi "github.com/hashicorp/consul/api"
)

var _ resourcelock.Interface = (*ConsulLock)(nil)

const minConsulSessionTTL = 10 * time.Second

var (
	errNilClient   = errors.New("consul client is required")
	errLockHeld    = errors.New("consul lock is already held")
	errTTLTooShort = fmt.Errorf("lease duration must be >= %v for Consul backend", minConsulSessionTTL)
)

type ConsulLock struct {
	client   *consulapi.Client
	key      string
	identity string
	ttl      string
	token    string

	mu        sync.Mutex
	sessionID string
}

type kvValue struct {
	Identity string `json:"identity"`
	Token    string `json:"token"`
}

// New returns a ConsulLock backed by Consul sessions and KV.
// Returns an error if client is nil or leaseDuration < 10s (Consul minimum TTL).
func New(client *consulapi.Client, name, identity string, leaseDuration time.Duration) (*ConsulLock, error) {
	if client == nil {
		return nil, errNilClient
	}
	if leaseDuration < minConsulSessionTTL {
		return nil, errTTLTooShort
	}
	token, err := randomToken()
	if err != nil {
		return nil, err
	}
	return &ConsulLock{
		client:   client,
		key:      name,
		identity: identity,
		ttl:      consulTTL(leaseDuration),
		token:    token,
	}, nil
}

func (l *ConsulLock) Identity() string { return l.identity }

// Acquire creates a Consul session (with LockDelay=0 for fast failover) and
// performs a CAS KV acquire. Destroys the session on failure to avoid leaks.
func (l *ConsulLock) Acquire(ctx context.Context) error {
	if l == nil || l.client == nil {
		return errNilClient
	}

	w := (&consulapi.WriteOptions{}).WithContext(ctx)
	sessionID, _, err := l.client.Session().Create(&consulapi.SessionEntry{
		Name:      l.key,
		TTL:       l.ttl,
		LockDelay: 0,
		Behavior:  consulapi.SessionBehaviorDelete,
	}, w)
	if err != nil {
		return err
	}

	value, err := marshalKVValue(l.identity, l.token)
	if err != nil {
		l.destroySession(sessionID)
		return err
	}

	w = (&consulapi.WriteOptions{}).WithContext(ctx)
	acquired, _, err := l.client.KV().Acquire(&consulapi.KVPair{
		Key:     l.key,
		Value:   []byte(value),
		Session: sessionID,
	}, w)
	if err != nil {
		l.destroySession(sessionID)
		return err
	}
	if !acquired {
		l.destroySession(sessionID)
		return errLockHeld
	}

	l.mu.Lock()
	l.sessionID = sessionID
	l.mu.Unlock()
	return nil
}

// Renew extends the Consul session TTL and verifies the KV key is still owned
// by this session.
func (l *ConsulLock) Renew(ctx context.Context) (bool, error) {
	if l == nil || l.client == nil {
		return false, errNilClient
	}

	l.mu.Lock()
	sessionID := l.sessionID
	l.mu.Unlock()
	if sessionID == "" {
		return false, nil
	}

	w := (&consulapi.WriteOptions{}).WithContext(ctx)
	entry, _, err := l.client.Session().Renew(sessionID, w)
	if err != nil || entry == nil {
		l.mu.Lock()
		l.sessionID = ""
		l.mu.Unlock()
		return false, err
	}

	q := (&consulapi.QueryOptions{}).WithContext(ctx)
	pair, _, err := l.client.KV().Get(l.key, q)
	if err != nil {
		return false, err
	}
	if pair == nil || pair.Session != sessionID {
		l.mu.Lock()
		l.sessionID = ""
		l.mu.Unlock()
		l.destroySession(sessionID)
		return false, nil
	}
	return true, nil
}

// Release checks ownership then destroys the Consul session (which
// auto-deletes the KV key via SessionBehaviorDelete).
func (l *ConsulLock) Release(ctx context.Context) (bool, error) {
	if l == nil || l.client == nil {
		return false, errNilClient
	}

	l.mu.Lock()
	sessionID := l.sessionID
	l.sessionID = ""
	l.mu.Unlock()
	if sessionID == "" {
		return false, nil
	}

	q := (&consulapi.QueryOptions{}).WithContext(ctx)
	pair, _, err := l.client.KV().Get(l.key, q)
	if err != nil {
		return false, err
	}
	owned := pair != nil && pair.Session == sessionID
	l.destroySession(sessionID)
	return owned, nil
}

func (l *ConsulLock) CurrentLeader(ctx context.Context) (string, error) {
	if l == nil || l.client == nil {
		return "", errNilClient
	}
	q := (&consulapi.QueryOptions{}).WithContext(ctx)
	pair, _, err := l.client.KV().Get(l.key, q)
	if err != nil {
		return "", err
	}
	if pair == nil || len(pair.Value) == 0 {
		return "", nil
	}
	return parseKVValue(string(pair.Value)), nil
}

func (l *ConsulLock) destroySession(sessionID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	w := (&consulapi.WriteOptions{}).WithContext(ctx)
	_, _ = l.client.Session().Destroy(sessionID, w)
}

func consulTTL(d time.Duration) string {
	secs := int64(math.Ceil(d.Seconds()))
	return fmt.Sprintf("%ds", secs)
}

func randomToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func marshalKVValue(identity, token string) (string, error) {
	data, err := json.Marshal(kvValue{Identity: identity, Token: token})
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func parseKVValue(value string) string {
	var v kvValue
	if err := json.Unmarshal([]byte(value), &v); err != nil {
		return ""
	}
	if v.Identity == "" {
		return ""
	}
	return v.Identity
}
