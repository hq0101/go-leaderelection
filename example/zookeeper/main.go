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
	zklock "github.com/hq0101/go-leaderelection/resourcelock/zookeeper"

	"github.com/go-zookeeper/zk"
)

func main() {
	zkServers := flag.String("zk-servers", "127.0.0.1:2181", "comma-separated ZooKeeper servers")
	zkSessionTimeout := flag.Duration("zk-session-timeout", 15*time.Second, "ZooKeeper session timeout (also used as LeaseDuration)")
	lockName := flag.String("lock", "example-leader-election", "leader election lock name")
	identity := flag.String("id", defaultIdentity(), "leader identity")
	renewDeadline := flag.Duration("renew-deadline", 10*time.Second, "renew deadline")
	retryPeriod := flag.Duration("retry-period", 2*time.Second, "retry period")
	releaseOnCancel := flag.Bool("release-on-cancel", true, "release lock on shutdown")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	conn, _, err := zk.Connect(splitServers(*zkServers), *zkSessionTimeout)
	if err != nil {
		log.Fatalf("connect to ZooKeeper: %v", err)
	}
	defer conn.Close()

	elector, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:            zklock.New(conn, *lockName, *identity),
		LeaseDuration:   *zkSessionTimeout,
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

	log.Printf("starting leader election identity=%s lock=%s zk=%s", *identity, *lockName, *zkServers)
	elector.Run(ctx)
	log.Print("leader election stopped")
}

func splitServers(raw string) []string {
	parts := strings.Split(raw, ",")
	servers := make([]string, 0, len(parts))
	for _, part := range parts {
		if s := strings.TrimSpace(part); s != "" {
			servers = append(servers, s)
		}
	}
	return servers
}

func defaultIdentity() string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "unknown-host"
	}
	return fmt.Sprintf("%s-%d", hostname, os.Getpid())
}
