package consul

import (
	"testing"
	"time"

	"github.com/hq0101/go-leaderelection/resourcelock"

	consulapi "github.com/hashicorp/consul/api"
)

var _ resourcelock.Interface = (*ConsulLock)(nil)

func TestNewReturnsErrorForNilClient(t *testing.T) {
	_, err := New(nil, "name", "id", 15*time.Second)
	if err == nil {
		t.Fatal("expected error for nil client")
	}
}

func TestConsulTTL(t *testing.T) {
	tests := []struct {
		d    time.Duration
		want string
	}{
		{10 * time.Second, "10s"},
		{15 * time.Second, "15s"},
		{1500 * time.Millisecond, "2s"},
		{30 * time.Second, "30s"},
		{90 * time.Second, "90s"},
	}
	for _, tt := range tests {
		if got := consulTTL(tt.d); got != tt.want {
			t.Fatalf("consulTTL(%v) = %q, want %q", tt.d, got, tt.want)
		}
	}
}

func TestMarshalParseKVValue(t *testing.T) {
	s, err := marshalKVValue("node-a", "tok123")
	if err != nil {
		t.Fatalf("marshalKVValue error: %v", err)
	}
	if got := parseKVValue(s); got != "node-a" {
		t.Fatalf("parseKVValue = %q, want %q", got, "node-a")
	}
}

func TestParseKVValueMalformed(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{name: "not json", value: "node-a"},
		{name: "empty identity", value: `{"identity":"","token":"x"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseKVValue(tt.value); got != "" {
				t.Fatalf("expected empty result, got %q", got)
			}
		})
	}
}

func TestNewRejectsShortTTL(t *testing.T) {
	client, err := consulapi.NewClient(consulapi.DefaultConfig())
	if err != nil {
		t.Fatalf("NewClient error: %v", err)
	}
	_, err = New(client, "my-lock", "node-a", 5*time.Second)
	if err == nil {
		t.Fatal("expected error for leaseDuration < 10s")
	}
}

func TestNewPreservesInputs(t *testing.T) {
	client, err := consulapi.NewClient(consulapi.DefaultConfig())
	if err != nil {
		t.Fatalf("NewClient error: %v", err)
	}
	l, err := New(client, "test-lock", "node-a", 15*time.Second)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	if l.key != "test-lock" {
		t.Fatalf("key = %q, want %q", l.key, "test-lock")
	}
	if l.identity != "node-a" {
		t.Fatalf("identity = %q, want %q", l.identity, "node-a")
	}
	if l.ttl != "15s" {
		t.Fatalf("ttl = %q, want %q", l.ttl, "15s")
	}
	if l.token == "" {
		t.Fatal("expected non-empty token")
	}
}
