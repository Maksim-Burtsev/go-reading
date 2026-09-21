// Command wordfreq counts word frequencies and removes duplicate lines.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/Maksim-Burtsev/go-reading/apps/01-cli-wordfreq/internal/cli"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	context.AfterFunc(ctx, stop)
	err := run(ctx, os.Args, os.Getenv, os.Stdin, os.Stdout, os.Stderr)
	stop()
	if err != nil {
		fmt.Fprintf(os.Stderr, "wordfreq: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, getenv func(string) string, stdin io.Reader, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("empty argument list")
	}
	cmd := cli.NewRootCommand(getenv)
	cmd.SetArgs(args[1:])
	cmd.SetIn(stdin)
	cmd.SetOut(stdout)
	cmd.SetErr(stderr)
	return cmd.ExecuteContext(ctx)
}
