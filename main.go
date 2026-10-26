// Command adhocron runs a command on a cron schedule.
//
// Usage:
//
//	go run cirello.io/adhocron "*/2 * * * *" -- command [args...]
//
// The schedule accepts standard 5-field crontab expressions and the
// predefined descriptors (@hourly, @daily, @midnight, @weekly, @monthly,
// @yearly, @annually) as well as @every <duration> (e.g. "@every 90s").
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"

	"github.com/robfig/cron/v3"
)

func main() {
	log.SetFlags(log.LstdFlags)
	log.SetPrefix("adhocron: ")

	spec, cmd, err := parseArgs(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "usage: go run cirello.io/adhocron \"*/2 * * * *\" -- command [args...]")
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scheduler := cron.New(cron.WithChain(
		cron.Recover(cron.DefaultLogger),
		cron.SkipIfStillRunning(cron.DefaultLogger),
	))
	if _, err := scheduler.AddFunc(spec, func() { run(ctx, cmd) }); err != nil {
		log.Fatalf("invalid cron expression %q: %v", spec, err)
	}
	scheduler.Start()
	log.Printf("scheduled %q -- %s", spec, strings.Join(cmd, " "))

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sig)
	<-sig
	done := scheduler.Stop().Done()
	log.Print("shutting down; waiting for current run")
	select {
	case <-done:
	case <-sig:
		log.Print("forcing shutdown")
		cancel() // terminate a command still in flight
		<-done
	}
}

// parseArgs splits the command line into the cron expression and the
// command to run. Arguments after -- are used verbatim, so quoting and
// word splitting are already handled by the invoking shell.
func parseArgs(args []string) (string, []string, error) {
	for i, arg := range args {
		if arg != "--" {
			continue
		}
		if i == 0 {
			return "", nil, errors.New("missing cron expression before --")
		}
		if i > 1 {
			return "", nil, fmt.Errorf("expected a single cron expression before --, got %d tokens", i)
		}
		if i+1 >= len(args) {
			return "", nil, errors.New("missing command after --")
		}
		return args[0], args[i+1:], nil
	}
	return "", nil, errors.New("missing -- separator between cron expression and command")
}

// run executes the command once, streaming its output to adhocron's own
// stdout/stderr. A non-zero exit is logged, but does not cancel the
// schedule.
func run(ctx context.Context, cmd []string) {
	log.Printf("running: %s", strings.Join(cmd, " "))
	c := exec.CommandContext(ctx, cmd[0], cmd[1:]...)
	c.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	c.Stdin = os.Stdin
	if err := c.Run(); err != nil {
		if exitErr, ok := errors.AsType[*exec.ExitError](err); ok {
			log.Printf("command exited with status %d", exitErr.ExitCode())
			return
		}
		log.Printf("could not start command: %v", err)
		return
	}
	log.Print("command finished")
}
