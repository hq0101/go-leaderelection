package zookeeper

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hq0101/go-leaderelection/resourcelock"
)

var _ resourcelock.Interface = (*ZookeeperLock)(nil)

func TestNewReturnsNilForNilConn(t *testing.T) {
	if got := New(nil, "name", "id"); got != nil {
		t.Fatalf("expected nil for nil conn, got %#v", got)
	}
}

func TestLockDirPath(t *testing.T) {
	l := &ZookeeperLock{lockDir: "/my-lock"}
	if l.lockDir != "/my-lock" {
		t.Fatalf("lockDir = %q, want %q", l.lockDir, "/my-lock")
	}
}

func TestNilConnRenewReturnsError(t *testing.T) {
	l := &ZookeeperLock{conn: nil, candidatePath: "set-so-nil-guard-fires"}
	_, err := l.Renew(context.Background())
	if !errors.Is(err, errNilConn) {
		t.Fatalf("expected errNilConn, got %v", err)
	}
}

func TestNilConnReleaseReturnsError(t *testing.T) {
	l := &ZookeeperLock{conn: nil, candidatePath: "set-so-nil-guard-fires"}
	_, err := l.Release(context.Background())
	if !errors.Is(err, errNilConn) {
		t.Fatalf("expected errNilConn, got %v", err)
	}
}

func TestEmptyCandidatePathOnZeroValue(t *testing.T) {
	l := &ZookeeperLock{}
	if l.candidatePath != "" {
		t.Fatal("expected empty candidatePath on zero-value ZookeeperLock")
	}
	_ = time.Second // leaseDuration not used by ZK backend
}
