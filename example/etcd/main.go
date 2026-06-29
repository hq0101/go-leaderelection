package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	leaderelection "github.com/hq0101/go-leaderelection"
	etcdlock "github.com/hq0101/go-leaderelection/resourcelock/etcd"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	endpoints := flag.String("etcd-endpoints", "127.0.0.1:2379", "comma-separated etcd endpoints")
	lockName := flag.String("lock", "example-leader-election", "leader election lock name")
	identity := flag.String("id", defaultIdentity(), "leader identity")
	leaseDuration := flag.Duration("lease-duration", 15*time.Second, "lease duration")
	renewDeadline := flag.Duration("renew-deadline", 10*time.Second, "renew deadline")
	retryPeriod := flag.Duration("retry-period", 2*time.Second, "retry period")
	releaseOnCancel := flag.Bool("release-on-cancel", true, "release lock when shutting down")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   splitEndpoints(*endpoints),
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("create etcd client: %v", err)
	}
	defer client.Close()

	elector, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:            etcdlock.New(client, *lockName, *identity, *leaseDuration),
		LeaseDuration:   *leaseDuration,
		RenewDeadline:   *renewDeadline,
		RetryPeriod:     *retryPeriod,
		ReleaseOnCancel: *releaseOnCancel,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				log.Printf("started leading as %s", *identity)
				ticker := time.NewTicker(time.Second)
				defer ticker.Stop()
				for {
					select {
					case <-ctx.Done():
						log.Printf("leader work stopped for %s", *identity)
						return
					case now := <-ticker.C:
						log.Printf("leader heartbeat identity=%s time=%s", *identity, now.Format(time.RFC3339))
					}
				}
			},
			OnStoppedLeading: func() {
				log.Printf("stopped leading as %s", *identity)
			},
			OnNewLeader: func(identity string) {
				log.Printf("observed leader: %s", identity)
			},
		},
	})
	if err != nil {
		log.Fatalf("create leader elector: %v", err)
	}

	log.Printf("starting leader election identity=%s lock=%s etcd=%s", *identity, *lockName, *endpoints)
	elector.Run(ctx)
	log.Print("leader election stopped")
}

func splitEndpoints(raw string) []string {
	parts := strings.Split(raw, ",")
	endpoints := make([]string, 0, len(parts))
	for _, part := range parts {
		endpoint := strings.TrimSpace(part)
		if endpoint == "" {
			continue
		}
		endpoints = append(endpoints, endpoint)
	}
	return endpoints
}

func defaultIdentity() string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "unknown-host"
	}
	return fmt.Sprintf("%s-%d", hostname, os.Getpid())
}
