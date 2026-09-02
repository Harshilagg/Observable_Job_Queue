// Command jobqueue is the entrypoint for both enqueueing jobs and
// running workers. Usage:
//
//	jobqueue enqueue -type=<type> -payload=<json>
//	jobqueue work -workers=<n>
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/Harshilagg/Observable_Job_Queue/internal/config"
	"github.com/Harshilagg/Observable_Job_Queue/internal/logging"
	"github.com/Harshilagg/Observable_Job_Queue/internal/store"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: jobqueue <enqueue|work> [flags]")
		os.Exit(1)
	}

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "config error:", err)
		os.Exit(1)
	}
	logger := logging.New(os.Getenv("LOG_LEVEL"))

	ctx := context.Background()
	st, err := store.New(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer st.Close()

	if err := st.Ping(ctx); err != nil {
		logger.Error("database ping failed", "error", err)
		os.Exit(1)
	}

	switch os.Args[1] {
	case "enqueue":
		fmt.Fprintln(os.Stderr, "enqueue: not implemented yet (Step 3/4)")
		os.Exit(1)
	case "work":
		fmt.Fprintln(os.Stderr, "work: not implemented yet (Step 4)")
		os.Exit(1)
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n", os.Args[1])
		os.Exit(1)
	}
}
