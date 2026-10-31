// payengine: a small payment processor.
//
//	payengine migrate                      create/update the database tables
//	payengine create-merchant -name Shop   make a merchant and print its api key
//	payengine serve                        run the api AND workers in one process
//	payengine api                          run only the api
//	payengine worker                       run only workers (you can run many of these)
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"payengine/internal/api"
	"payengine/internal/bank"
	"payengine/internal/db"
	"payengine/internal/merchants"
	"payengine/internal/worker"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	slog.SetDefault(logger)

	cmd, args := os.Args[1], os.Args[2:]
	var err error
	switch cmd {
	case "migrate":
		err = runMigrate()
	case "create-merchant":
		err = runCreateMerchant(args)
	case "serve":
		err = runServe(args, true, true)
	case "api":
		err = runServe(args, true, false)
	case "worker":
		err = runServe(args, false, true)
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		logger.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: payengine <command>

commands:
  migrate            create/update database tables
  create-merchant    make a merchant and print its api key  (-name "My Shop")
  serve              run api + workers
  api                run only the api
  worker             run only workers

env:
  DATABASE_URL       postgres connection string (required)`)
}

func databaseURL() (string, error) {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		return "", fmt.Errorf("set DATABASE_URL, e.g. postgres://pay:pay@localhost:5432/payengine")
	}
	return url, nil
}

func runMigrate() error {
	url, err := databaseURL()
	if err != nil {
		return err
	}
	ctx := context.Background()
	pool, err := db.Connect(ctx, url)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := db.Migrate(ctx, pool); err != nil {
		return err
	}
	fmt.Println("database is up to date")
	return nil
}

func runCreateMerchant(args []string) error {
	fs := flag.NewFlagSet("create-merchant", flag.ExitOnError)
	name := fs.String("name", "", "merchant name")
	fs.Parse(args)
	if *name == "" {
		return fmt.Errorf("give a name: payengine create-merchant -name \"My Shop\"")
	}
	url, err := databaseURL()
	if err != nil {
		return err
	}
	ctx := context.Background()
	pool, err := db.Connect(ctx, url)
	if err != nil {
		return err
	}
	defer pool.Close()
	id, key, err := merchants.Create(ctx, pool, *name)
	if err != nil {
		return err
	}
	fmt.Printf("merchant id: %s\napi key:     %s\n\n(save the key now, it is not stored and can't be shown again)\n", id, key)
	return nil
}

func runServe(args []string, withAPI, withWorkers bool) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("addr", ":8080", "api listen address")
	bankURL := fs.String("bank", "http://localhost:9090", "bank url")
	bankTimeout := fs.Duration("bank-timeout", 5*time.Second, "how long to wait for the bank")
	workers := fs.Int("workers", 4, "number of workers")
	maxAttempts := fs.Int("max-attempts", 6, "tries before a payment goes to review")
	lease := fs.Duration("lease", 30*time.Second, "job lease (must be longer than bank-timeout)")
	fs.Parse(args)

	if withWorkers && *lease <= *bankTimeout {
		return fmt.Errorf("-lease (%s) must be longer than -bank-timeout (%s), or two workers could take the same job", *lease, *bankTimeout)
	}

	url, err := databaseURL()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := db.Connect(ctx, url)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := db.Migrate(ctx, pool); err != nil {
		return err
	}

	var wg sync.WaitGroup
	var httpSrv *http.Server

	if withAPI {
		httpSrv = &http.Server{
			Addr:              *addr,
			Handler:           api.New(pool, slog.Default()).Routes(),
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       10 * time.Second,
			WriteTimeout:      15 * time.Second,
			IdleTimeout:       60 * time.Second,
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			slog.Info("api listening", "addr", *addr)
			if err := httpSrv.ListenAndServe(); err != http.ErrServerClosed {
				slog.Error("api stopped", "err", err)
				stop()
			}
		}()
	}

	if withWorkers {
		host, _ := os.Hostname()
		wg.Add(1)
		go func() {
			defer wg.Done()
			slog.Info("workers starting", "count", *workers, "bank", *bankURL)
			worker.RunMany(ctx, *workers, worker.Config{
				ID:          fmt.Sprintf("%s-%d", host, os.Getpid()),
				Pool:        pool,
				Bank:        bank.NewClient(*bankURL, *bankTimeout),
				Logger:      slog.Default(),
				Lease:       *lease,
				MaxAttempts: *maxAttempts,
			})
		}()
	}

	<-ctx.Done()
	slog.Info("shutting down...")
	if httpSrv != nil {
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		httpSrv.Shutdown(sctx)
	}
	wg.Wait() // workers finish the job they're on first
	return nil
}
