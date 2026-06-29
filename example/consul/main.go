package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	leaderelection "github.com/hq0101/go-leaderelection"
	consullock "github.com/hq0101/go-leaderelection/resourcelock/consul"

	consulapi "github.com/hashicorp/consul/api"
)

func main() {
	consulAddr := flag.String("consul-addr", "127.0.0.1:8500", "Consul HTTP address")
	consulToken := flag.String("consul-token", "", "Consul ACL token (optional)")
	lockName := flag.String("lock", "example-leader-election", "leader election lock name")
	identity := flag.String("id", defaultIdentity(), "leader identity")
	leaseDuration := flag.Duration("lease-duration", 15*time.Second, "lease duration (Consul session TTL, ≥10s)")
	renewDeadline := flag.Duration("renew-deadline", 10*time.Second, "renew deadline")
	retryPeriod := flag.Duration("retry-period", 2*time.Second, "retry period")
	releaseOnCancel := flag.Bool("release-on-cancel", true, "release lock on shutdown")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := consulapi.DefaultConfig()
	cfg.Address = *consulAddr
	if *consulToken != "" {
		cfg.Token = *consulToken
	}
	client, err := consulapi.NewClient(cfg)
	if err != nil {
		log.Fatalf("create consul client: %v", err)
	}

	lock, err := consullock.New(client, *lockName, *identity, *leaseDuration)
	if err != nil {
		log.Fatalf("create consul lock: %v", err)
	}

	elector, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:            lock,
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

	log.Printf("starting leader election identity=%s lock=%s consul=%s", *identity, *lockName, *consulAddr)
	elector.Run(ctx)
	log.Print("leader election stopped")
}

func defaultIdentity() string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "unknown-host"
	}
	return fmt.Sprintf("%s-%d", hostname, os.Getpid())
}
