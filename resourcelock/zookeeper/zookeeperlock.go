package zookeeper

import (
	"context"
	"errors"
	"path"
	"sort"
	"sync"

	"github.com/hq0101/go-leaderelection/resourcelock"

	"github.com/go-zookeeper/zk"
)

var _ resourcelock.Interface = (*ZookeeperLock)(nil)

var (
	errNilConn  = errors.New("zookeeper connection is required")
	errLockHeld = errors.New("zookeeper lock is already held")
)

// ZookeeperLock implements leader election using ephemeral sequential znodes.
//
// The effective lease duration is the ZooKeeper session timeout configured at
// connection time via zk.Connect — leaseDuration in LeaderElectionConfig is
// used only for RenewDeadline/RetryPeriod checks and does not affect session
// lifetime. Callers must configure an appropriate session timeout via zk.Connect.
type ZookeeperLock struct {
	conn     *zk.Conn
	lockDir  string
	identity string

	mu            sync.Mutex
	candidatePath string
}

// New returns a ZookeeperLock backed by ZooKeeper ephemeral sequential nodes.
// Returns nil if conn is nil.
func New(conn *zk.Conn, name, identity string) *ZookeeperLock {
	if conn == nil {
		return nil
	}
	return &ZookeeperLock{
		conn:     conn,
		lockDir:  "/" + name,
		identity: identity,
	}
}

func (l *ZookeeperLock) Identity() string { return l.identity }

// Acquire creates a persistent lock directory (if needed), then an ephemeral
// sequential candidate node. If our node is not the smallest (i.e. another
// leader exists), we delete our node and return errLockHeld.
//
// The ZooKeeper Create call runs in a goroutine so that ctx cancellation is
// respected before or after the node is created.
func (l *ZookeeperLock) Acquire(ctx context.Context) error {
	if l == nil || l.conn == nil {
		return errNilConn
	}

	if err := ensurePath(l.conn, l.lockDir); err != nil {
		return err
	}

	type createResult struct {
		nodePath string
		err      error
	}
	ch := make(chan createResult, 1)
	go func() {
		p, err := l.conn.Create(
			l.lockDir+"/candidate-",
			[]byte(l.identity),
			zk.FlagEphemeral|zk.FlagSequence,
			zk.WorldACL(zk.PermAll),
		)
		ch <- createResult{p, err}
	}()

	var nodePath string
	select {
	case <-ctx.Done():
		return ctx.Err()
	case r := <-ch:
		if r.err != nil {
			return r.err
		}
		nodePath = r.nodePath
	}

	children, _, err := l.conn.Children(l.lockDir)
	if err != nil {
		_ = l.conn.Delete(nodePath, -1)
		return err
	}

	sort.Strings(children)
	if len(children) == 0 || children[0] != path.Base(nodePath) {
		_ = l.conn.Delete(nodePath, -1)
		return errLockHeld
	}

	l.mu.Lock()
	l.candidatePath = nodePath
	l.mu.Unlock()
	return nil
}

// Renew checks that our candidate node still exists and remains the smallest.
// No network write is needed — ephemeral nodes are kept alive by the ZK session.
func (l *ZookeeperLock) Renew(ctx context.Context) (bool, error) {
	if l == nil || l.conn == nil {
		return false, errNilConn
	}

	l.mu.Lock()
	candidatePath := l.candidatePath
	l.mu.Unlock()
	if candidatePath == "" {
		return false, nil
	}

	children, _, err := l.conn.Children(l.lockDir)
	if errors.Is(err, zk.ErrNoNode) {
		l.mu.Lock()
		l.candidatePath = ""
		l.mu.Unlock()
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if len(children) == 0 {
		l.mu.Lock()
		l.candidatePath = ""
		l.mu.Unlock()
		return false, nil
	}

	sort.Strings(children)
	if children[0] != path.Base(candidatePath) {
		l.mu.Lock()
		l.candidatePath = ""
		l.mu.Unlock()
		return false, nil
	}
	return true, nil
}

// Release deletes our candidate node. ErrNoNode is treated as success
// (already cleaned up by session expiry).
func (l *ZookeeperLock) Release(ctx context.Context) (bool, error) {
	if l == nil || l.conn == nil {
		return false, errNilConn
	}

	l.mu.Lock()
	candidatePath := l.candidatePath
	l.candidatePath = ""
	l.mu.Unlock()
	if candidatePath == "" {
		return false, nil
	}

	if err := l.conn.Delete(candidatePath, -1); err != nil {
		if errors.Is(err, zk.ErrNoNode) {
			return true, nil
		}
		return false, err
	}
	return true, nil
}

func (l *ZookeeperLock) CurrentLeader(ctx context.Context) (string, error) {
	if l == nil || l.conn == nil {
		return "", errNilConn
	}
	children, _, err := l.conn.Children(l.lockDir)
	if errors.Is(err, zk.ErrNoNode) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if len(children) == 0 {
		return "", nil
	}
	sort.Strings(children)
	data, _, err := l.conn.Get(l.lockDir + "/" + children[0])
	if errors.Is(err, zk.ErrNoNode) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func ensurePath(conn *zk.Conn, dirPath string) error {
	_, err := conn.Create(dirPath, []byte{}, 0, zk.WorldACL(zk.PermAll))
	if errors.Is(err, zk.ErrNodeExists) {
		return nil
	}
	return err
}
