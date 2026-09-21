// Package cli builds the wordfreq command tree.
package cli

import (
	"cmp"
	"errors"
	"fmt"
	"log/slog"

	"github.com/spf13/cobra"
)

// ErrInvalidFlag is returned when a flag value is outside its allowed range.
var ErrInvalidFlag = errors.New("invalid flag value")

type rootCommand struct {
	logLevel string
	logger   *slog.Logger
}

// NewRootCommand returns the wordfreq command with all subcommands attached.
// getenv supplies defaults for flags that can also be set from the
// environment.
func NewRootCommand(getenv func(string) string) *cobra.Command {
	root := &rootCommand{}
	cmd := &cobra.Command{
		Use:   "wordfreq",
		Short: "Count word frequencies and remove duplicate lines",
		Long: `wordfreq reads text from files or standard input.

Operands are file paths; "-" or no operands at all means standard input.`,
		SilenceUsage:      true,
		SilenceErrors:     true,
		PersistentPreRunE: root.setUp,
	}
	cmd.PersistentFlags().StringVar(&root.logLevel, "log-level", cmp.Or(getenv("WORDFREQ_LOG_LEVEL"), "warn"),
		"minimum level of JSON logs written to stderr: debug, info, warn or error (env WORDFREQ_LOG_LEVEL)")
	cmd.AddCommand(newCountCommand(root), newDedupeCommand(root))
	return cmd
}

func (r *rootCommand) setUp(cmd *cobra.Command, _ []string) error {
	var level slog.Level
	if err := level.UnmarshalText([]byte(r.logLevel)); err != nil {
		return fmt.Errorf("%w: --log-level: %w", ErrInvalidFlag, err)
	}
	r.logger = slog.New(slog.NewJSONHandler(cmd.ErrOrStderr(), &slog.HandlerOptions{Level: level}))
	return nil
}
