package etcd

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/hq0101/go-leaderelection/resourcelock"

	rpctypes "go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var _ resourcelock.Interface = (*EtcdLock)(nil)

var errNilClient = errors.New("etcd client is required")
var errLockHeld = errors.New("etcd lock is already held")

const acquireCleanupTimeout = time.Second

type EtcdLock struct {
	client   *clientv3.Client
	key      string
	identity string
	ttl      int64

	mu      sync.Mutex
	leaseID clientv3.LeaseID
}

// New returns an EtcdLock backed by etcd lease + KV txn.
// Returns nil if client is nil.
func New(client *clientv3.Client, name, identity string, leaseDuration time.Duration) *EtcdLock {
	if client == nil {
		return nil
	}
	return &EtcdLock{
		client:   client,
		key:      name,
		identity: identity,
		ttl:      leaseTTLSeconds(leaseDuration),
	}
}

func (l *EtcdLock) Identity() string { return l.identity }

func (l *EtcdLock) Acquire(ctx context.Context) error {
	if l == nil || l.client == nil {
		return errNilClient
	}

	l.mu.Lock()
	held := l.leaseID != clientv3.NoLease
	l.mu.Unlock()
	if held {
		return nil
	}

	lease, err := l.client.Grant(ctx, l.ttl)
	if err != nil {
		return err
	}

	resp, err := l.client.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(l.key), "=", 0)).
		Then(clientv3.OpPut(l.key, l.identity, clientv3.WithLease(lease.ID))).
		Commit()
	if err != nil {
		_ = revokeLeaseWithTimeout(l.client, lease.ID, acquireCleanupTimeout)
		return err
	}
	if !resp.Succeeded {
		_ = revokeLeaseWithTimeout(l.client, lease.ID, acquireCleanupTimeout)
		return errLockHeld
	}

	l.mu.Lock()
	l.leaseID = lease.ID
	l.mu.Unlock()
	return nil
}

func (l *EtcdLock) Renew(ctx context.Context) (bool, error) {
	if l == nil || l.client == nil {
		return false, errNilClient
	}

	l.mu.Lock()
	leaseID := l.leaseID
	l.mu.Unlock()
	if leaseID == clientv3.NoLease {
		return false, nil
	}

	keepAliveResp, err := l.client.KeepAliveOnce(ctx, leaseID)
	if errors.Is(err, rpctypes.ErrLeaseNotFound) {
		l.mu.Lock()
		l.leaseID = clientv3.NoLease
		l.mu.Unlock()
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if keepAliveResp == nil || keepAliveResp.TTL <= 0 {
		l.mu.Lock()
		l.leaseID = clientv3.NoLease
		l.mu.Unlock()
		return false, nil
	}

	owned, err := l.isOwner(ctx, leaseID)
	if err != nil {
		return false, err
	}
	if !owned {
		l.mu.Lock()
		l.leaseID = clientv3.NoLease
		l.mu.Unlock()
		return false, nil
	}
	return true, nil
}

func (l *EtcdLock) Release(ctx context.Context) (bool, error) {
	if l == nil || l.client == nil {
		return false, errNilClient
	}

	l.mu.Lock()
	leaseID := l.leaseID
	l.leaseID = clientv3.NoLease
	l.mu.Unlock()

	if leaseID == clientv3.NoLease {
		return false, nil
	}

	owned, err := l.isOwner(ctx, leaseID)
	if err != nil {
		return false, err
	}
	if err := l.revokeLease(ctx, leaseID); err != nil {
		return false, err
	}
	return owned, nil
}

func (l *EtcdLock) CurrentLeader(ctx context.Context) (string, error) {
	if l == nil || l.client == nil {
		return "", errNilClient
	}
	resp, err := l.client.Get(ctx, l.key)
	if err != nil {
		return "", err
	}
	if len(resp.Kvs) == 0 {
		return "", nil
	}
	return string(resp.Kvs[0].Value), nil
}

func (l *EtcdLock) isOwner(ctx context.Context, leaseID clientv3.LeaseID) (bool, error) {
	resp, err := l.client.Get(ctx, l.key)
	if err != nil {
		return false, err
	}
	if len(resp.Kvs) == 0 {
		return false, nil
	}
	kv := resp.Kvs[0]
	if string(kv.Value) != l.identity {
		return false, nil
	}
	if clientv3.LeaseID(kv.Lease) != leaseID {
		return false, nil
	}
	return true, nil
}

func (l *EtcdLock) revokeLease(ctx context.Context, leaseID clientv3.LeaseID) error {
	if leaseID == clientv3.NoLease {
		return nil
	}
	_, err := l.client.Revoke(ctx, leaseID)
	if errors.Is(err, rpctypes.ErrLeaseNotFound) {
		return nil
	}
	return err
}

type leaseRevoker interface {
	Revoke(ctx context.Context, id clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error)
}

func revokeLeaseWithTimeout(revoker leaseRevoker, leaseID clientv3.LeaseID, timeout time.Duration) error {
	if leaseID == clientv3.NoLease {
		return nil
	}
	revokeCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, err := revoker.Revoke(revokeCtx, leaseID)
	if errors.Is(err, rpctypes.ErrLeaseNotFound) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func leaseTTLSeconds(d time.Duration) int64 {
	if d <= 0 {
		return 1
	}
	return int64(math.Ceil(d.Seconds()))
}
