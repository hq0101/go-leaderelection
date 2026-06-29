package redis

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
)

func TestNewReturnsNilForNilClient(t *testing.T) {
	if got := New(nil, "name", "id", time.Second); got != nil {
		t.Fatalf("expected nil for nil client, got %#v", got)
	}
}

func TestLockValueRoundTrip(t *testing.T) {
	value, err := newLockValue("node-a")
	if err != nil {
		t.Fatalf("newLockValue returned error: %v", err)
	}

	var decoded lockValue
	if err := json.Unmarshal([]byte(value), &decoded); err != nil {
		t.Fatalf("lock value should be JSON: %v", err)
	}
	if decoded.Identity != "node-a" {
		t.Fatalf("expected identity node-a, got %q", decoded.Identity)
	}
	if decoded.Token == "" {
		t.Fatal("expected non-empty random token")
	}
	if got := parseLockValue(value); got != "node-a" {
		t.Fatalf("expected parsed identity node-a, got %q", got)
	}
}

func TestParseLockValueLegacyPlainStringIdentity(t *testing.T) {
	if got := parseLockValue("node-a"); got != "node-a" {
		t.Fatalf("expected legacy plain-string identity node-a, got %q", got)
	}
	if got := parseLockValue("  node-a  "); got != "  node-a  " {
		t.Fatalf("expected legacy plain-string identity with spaces to round-trip, got %q", got)
	}
	if got := parseLockValue("{node-a}"); got != "{node-a}" {
		t.Fatalf("expected brace-wrapped legacy plain-string identity to round-trip, got %q", got)
	}
}

func TestParseLockValueRejectsMalformedStructuredPayload(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{name: "malformed json object", value: `{"identity":"node-a"`},
		{name: "empty identity object", value: `{"identity":"","token":"abc"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseLockValue(tt.value); got != "" {
				t.Fatalf("expected empty identity, got %q", got)
			}
		})
	}
}

func TestAcquireCurrentLeaderRenewRelease(t *testing.T) {
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	defer client.Close()

	lock := New(client, "test-lock", "node-a", time.Second)

	ctx := context.Background()
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

	ok, err := lock.Renew(ctx)
	if err != nil || !ok {
		t.Fatalf("Renew returned ok=%v err=%v", ok, err)
	}

	ok, err = lock.Release(ctx)
	if err != nil || !ok {
		t.Fatalf("Release returned ok=%v err=%v", ok, err)
	}

	leader, err = lock.CurrentLeader(ctx)
	if err != nil {
		t.Fatalf("CurrentLeader after release returned error: %v", err)
	}
	if leader != "" {
		t.Fatalf("expected no leader after release, got %q", leader)
	}
}
