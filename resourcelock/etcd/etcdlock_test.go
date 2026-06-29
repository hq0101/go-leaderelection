package etcd

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/hq0101/go-leaderelection/resourcelock"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

var _ resourcelock.Interface = (*EtcdLock)(nil)

type blockingRevoker struct {
	started chan<- struct{}
}

func (r blockingRevoker) Revoke(ctx context.Context, id clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error) {
	select {
	case r.started <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestLeaseTTLSecondsRoundsUp(t *testing.T) {
	tests := []struct {
		name string
		d    time.Duration
		want int64
	}{
		{name: "exact second", d: time.Second, want: 1},
		{name: "round up fractional second", d: 1500 * time.Millisecond, want: 2},
		{name: "round up multiple fractional seconds", d: 2500 * time.Millisecond, want: 3},
		{name: "minimum ttl for sub-second lease", d: 100 * time.Millisecond, want: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := leaseTTLSeconds(tt.d); got != tt.want {
				t.Fatalf("leaseTTLSeconds(%v) = %d, want %d", tt.d, got, tt.want)
			}
		})
	}
}

func TestNewReturnsNilForNilClient(t *testing.T) {
	if got := New(nil, "name", "id", time.Second); got != nil {
		t.Fatalf("expected nil for nil client, got %#v", got)
	}
}

func TestRevokeLeaseWithTimeoutReturnsPromptly(t *testing.T) {
	started := make(chan struct{}, 1)

	begin := time.Now()
	err := revokeLeaseWithTimeout(blockingRevoker{started: started}, clientv3.LeaseID(123), 20*time.Millisecond)
	if err != nil {
		t.Fatalf("revokeLeaseWithTimeout returned error: %v", err)
	}

	select {
	case <-started:
	default:
		t.Fatal("expected Revoke to be invoked")
	}

	if elapsed := time.Since(begin); elapsed > 200*time.Millisecond {
		t.Fatalf("expected bounded cleanup, took %v", elapsed)
	}
}

func TestNewKeepsConstructionInputs(t *testing.T) {
	client := &clientv3.Client{}
	l := New(client, "leader-lock", "node-a", 1500*time.Millisecond)
	if l == nil {
		t.Fatal("expected non-nil EtcdLock")
	}
	if l.client != client {
		t.Fatal("expected lock to retain etcd client")
	}
	if l.key != "leader-lock" {
		t.Fatalf("expected key %q, got %q", "leader-lock", l.key)
	}
	if l.identity != "node-a" {
		t.Fatalf("expected identity %q, got %q", "node-a", l.identity)
	}
	if l.ttl != 2 {
		t.Fatalf("expected ttl %d, got %d", 2, l.ttl)
	}
}

func TestAcquireCurrentLeaderRenewRelease(t *testing.T) {
	client := newEmbeddedClient(t)
	lock := New(client, "leader-lock", "node-a", time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := lock.Acquire(ctx); err != nil {
		t.Fatalf("Acquire returned error: %v", err)
	}

	leader, err := lock.CurrentLeader(ctx)
	if err != nil {
		t.Fatalf("CurrentLeader returned error: %v", err)
	}
	if leader != "node-a" {
		t.Fatalf("leader = %q, want %q", leader, "node-a")
	}

	time.Sleep(700 * time.Millisecond)
	ok, err := lock.Renew(ctx)
	if err != nil || !ok {
		t.Fatalf("Renew returned ok=%v err=%v", ok, err)
	}

	time.Sleep(700 * time.Millisecond)
	leader, err = lock.CurrentLeader(ctx)
	if err != nil {
		t.Fatalf("CurrentLeader after Renew returned error: %v", err)
	}
	if leader != "node-a" {
		t.Fatalf("leader after Renew = %q, want %q", leader, "node-a")
	}

	ok, err = lock.Release(ctx)
	if err != nil || !ok {
		t.Fatalf("Release returned ok=%v err=%v", ok, err)
	}

	if err := waitForNoLeader(ctx, lock); err != nil {
		t.Fatalf("leader did not clear after Release: %v", err)
	}
}

func TestAcquireExpiresWithoutRenew(t *testing.T) {
	client := newEmbeddedClient(t)
	first := New(client, "leader-lock", "node-a", time.Second)
	second := New(client, "leader-lock", "node-b", time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	if err := first.Acquire(ctx); err != nil {
		t.Fatalf("first Acquire returned error: %v", err)
	}

	if err := waitForNoLeader(ctx, first); err != nil {
		t.Fatalf("lock stayed held without Renew: %v", err)
	}

	if err := second.Acquire(ctx); err != nil {
		t.Fatalf("second Acquire after first lease expiry returned error: %v", err)
	}
}

func newEmbeddedClient(t *testing.T) *clientv3.Client {
	t.Helper()

	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.Logger = "zap"
	cfg.LogLevel = "error"

	clientURL, err := url.Parse("http://127.0.0.1:0")
	if err != nil {
		t.Fatalf("parse client url: %v", err)
	}
	peerURL, err := url.Parse("http://127.0.0.1:0")
	if err != nil {
		t.Fatalf("parse peer url: %v", err)
	}

	cfg.ListenClientUrls = []url.URL{*clientURL}
	cfg.AdvertiseClientUrls = []url.URL{*clientURL}
	cfg.ListenPeerUrls = []url.URL{*peerURL}
	cfg.AdvertisePeerUrls = []url.URL{*peerURL}
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)

	server, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Fatalf("StartEtcd returned error: %v", err)
	}
	t.Cleanup(func() {
		server.Close()
	})

	select {
	case <-server.Server.ReadyNotify():
	case <-time.After(10 * time.Second):
		t.Fatal("embedded etcd did not become ready")
	}

	endpoints := make([]string, 0, len(server.Clients))
	for _, listener := range server.Clients {
		endpoints = append(endpoints, listener.Addr().String())
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("clientv3.New returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = client.Close()
	})

	return client
}

func waitForNoLeader(ctx context.Context, lock *EtcdLock) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		leader, err := lock.CurrentLeader(ctx)
		if err != nil {
			return err
		}
		if leader == "" {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
