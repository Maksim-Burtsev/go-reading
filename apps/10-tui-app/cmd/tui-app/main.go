// Command tui-app is a two-pane terminal browser for a JSON task list.
package main

import (
	"cmp"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/Maksim-Burtsev/go-reading/apps/10-tui-app/internal/task"
	"github.com/Maksim-Burtsev/go-reading/apps/10-tui-app/internal/ui"
)

const defaultTasksFile = "apps/10-tui-app/tasks.json"

func main() {
	if err := run(context.Background(), os.Args, os.Getenv, os.Stdin, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(
	ctx context.Context,
	args []string,
	getenv func(string) string,
	stdin io.Reader,
	stdout, stderr io.Writer,
) error {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	flags := flag.NewFlagSet(args[0], flag.ContinueOnError)
	flags.SetOutput(stderr)
	path := flags.String("file", cmp.Or(getenv("TASKS_FILE"), defaultTasksFile), "path to the tasks JSON file")
	if err := flags.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return fmt.Errorf("parse flags: %w", err)
	}

	logger := slog.New(slog.NewJSONHandler(stderr, nil))

	tasks, err := task.Load(ctx, *path)
	if err != nil {
		return fmt.Errorf("load tasks: %w", err)
	}
	logger.InfoContext(ctx, "tasks loaded", slog.String("path", *path), slog.Int("count", len(tasks)))

	program := tea.NewProgram(
		ui.New(tasks, time.Now()),
		tea.WithContext(ctx),
		tea.WithInput(stdin),
		tea.WithOutput(stdout),
		tea.WithoutSignalHandler(),
	)
	final, err := program.Run()
	if err != nil {
		return fmt.Errorf("run program: %w", err)
	}

	model, ok := final.(ui.Model)
	if !ok {
		return fmt.Errorf("unexpected final model %T", final)
	}
	if !model.Modified() {
		return nil
	}
	if err := task.Save(ctx, *path, model.Tasks()); err != nil {
		return fmt.Errorf("save tasks: %w", err)
	}
	logger.InfoContext(ctx, "tasks saved", slog.String("path", *path), slog.Int("count", len(model.Tasks())))
	return nil
}
