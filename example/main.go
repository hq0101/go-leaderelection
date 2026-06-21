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

	redis "github.com/redis/go-redis/v9"
)

func main() {
	redisAddr := flag.String("redis-addr", "192.168.127.131:6379", "Redis address")
	redisUsername := flag.String("redis-username", "", "Redis username")
	redisPassword := flag.String("redis-password", "123456", "Redis password")
	redisDB := flag.Int("redis-db", 0, "Redis database")
	lockName := flag.String("lock", "example-leader-election", "leader election lock name")
	identity := flag.String("id", defaultIdentity(), "leader identity")
	leaseDuration := flag.Duration("lease-duration", 15*time.Second, "lease duration")
	renewDeadline := flag.Duration("renew-deadline", 10*time.Second, "renew deadline")
	retryPeriod := flag.Duration("retry-period", 2*time.Second, "retry period")
	releaseOnCancel := flag.Bool("release-on-cancel", true, "release lock when shutting down")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client := redis.NewClient(newRedisOptions(*redisAddr, *redisUsername, *redisPassword, *redisDB))
	defer client.Close()

	elector, err := leaderelection.NewLeaderElector(leaderelection.Config{
		LockName:        *lockName,
		Identity:        *identity,
		LeaseDuration:   *leaseDuration,
		RenewDeadline:   *renewDeadline,
		RetryPeriod:     *retryPeriod,
		ReleaseOnCancel: *releaseOnCancel,
		RedisClient:     client,
		Callbacks: leaderelection.Callbacks{
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

	log.Printf("starting leader election identity=%s lock=%s redis=%s", *identity, *lockName, *redisAddr)
	elector.Run(ctx)
	log.Print("leader election stopped")
}

func newRedisOptions(addr, username, password string, db int) *redis.Options {
	return &redis.Options{
		Addr:     addr,
		Username: username,
		Password: password,
		DB:       db,
	}
}

func defaultIdentity() string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "unknown-host"
	}
	return fmt.Sprintf("%s-%d", hostname, os.Getpid())
}
