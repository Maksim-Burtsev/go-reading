package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/Maksim-Burtsev/go-reading/apps/01-cli-wordfreq/internal/input"
	"github.com/Maksim-Burtsev/go-reading/apps/01-cli-wordfreq/internal/wordfreq"
)

type countCommand struct {
	root      *rootCommand
	top       int
	minLen    int
	jobs      int
	json      bool
	keepGoing bool
}

type report struct {
	Total   int              `json:"total"`
	Unique  int              `json:"unique"`
	Words   []wordfreq.Entry `json:"words"`
	Skipped []string         `json:"skipped,omitempty"`
}

func newCountCommand(root *rootCommand) *cobra.Command {
	c := &countCommand{root: root}
	cmd := &cobra.Command{
		Use:   "count [file...]",
		Short: "Print the most frequent words",
		Long: `Count prints the most frequent words across all inputs.

Words are runs of Unicode letters and digits, compared case-insensitively.
Inputs are read concurrently; the output order is always count descending,
then word ascending.

By default the first input that cannot be opened or read stops the command.
With --keep-going such inputs are skipped with a warning and the rest are
still counted.`,
		Example: `  wordfreq count --top 20 book.txt
  cat *.md | wordfreq count --min-len 4 --json
  wordfreq count --keep-going --top 50 notes/*.txt`,
		RunE: c.run,
	}
	f := cmd.Flags()
	f.IntVarP(&c.top, "top", "n", 10, "number of words to print, 0 for all")
	f.IntVar(&c.minLen, "min-len", 1, "skip words shorter than this many letters")
	f.IntVarP(&c.jobs, "jobs", "j", runtime.GOMAXPROCS(0), "maximum number of inputs read concurrently")
	f.BoolVar(&c.json, "json", false, "print JSON instead of a table")
	f.BoolVarP(&c.keepGoing, "keep-going", "k", false, "skip inputs that cannot be opened or read instead of failing")
	return cmd
}

func (c *countCommand) validate() error {
	switch {
	case c.top < 0:
		return fmt.Errorf("%w: --top must not be negative, got %d", ErrInvalidFlag, c.top)
	case c.minLen < 1:
		return fmt.Errorf("%w: --min-len must be at least 1, got %d", ErrInvalidFlag, c.minLen)
	case c.jobs < 1:
		return fmt.Errorf("%w: --jobs must be at least 1, got %d", ErrInvalidFlag, c.jobs)
	}
	return nil
}

func (c *countCommand) run(cmd *cobra.Command, args []string) error {
	if err := c.validate(); err != nil {
		return err
	}
	names, err := input.Names(args)
	if err != nil {
		return err
	}

	ctx := cmd.Context()
	start := time.Now()
	open := input.Opener{Stdin: cmd.InOrStdin()}
	opts := wordfreq.Options{MinLen: c.minLen, Jobs: c.jobs, KeepGoing: c.keepGoing}
	counts, skipped, err := wordfreq.CountAll(ctx, open, names, opts)
	if err != nil {
		return fmt.Errorf("count: %w", err)
	}
	rep := report{Total: counts.Total(), Unique: len(counts), Words: counts.Top(c.top)}
	for _, s := range skipped {
		c.root.logger.WarnContext(ctx, "input skipped", slog.String("input", s.Name), slog.Any("error", s.Err))
		rep.Skipped = append(rep.Skipped, s.Name)
	}
	c.root.logger.InfoContext(ctx, "count finished",
		slog.Int("inputs", len(names)),
		slog.Int("skipped", len(skipped)),
		slog.Int("words", rep.Total),
		slog.Int("unique", rep.Unique),
		slog.Duration("elapsed", time.Since(start)),
	)

	if c.json {
		return writeJSON(cmd.OutOrStdout(), rep)
	}
	return writeTable(cmd.OutOrStdout(), rep)
}

func writeJSON(w io.Writer, rep report) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(rep); err != nil {
		return fmt.Errorf("write json: %w", err)
	}
	return nil
}

func writeTable(w io.Writer, rep report) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "RANK\tWORD\tCOUNT\tSHARE"); err != nil {
		return fmt.Errorf("write table: %w", err)
	}
	for i, e := range rep.Words {
		share := 100 * float64(e.Count) / float64(rep.Total)
		if _, err := fmt.Fprintf(tw, "%d\t%s\t%d\t%.2f%%\n", i+1, e.Word, e.Count, share); err != nil {
			return fmt.Errorf("write table: %w", err)
		}
	}
	if err := tw.Flush(); err != nil {
		return fmt.Errorf("write table: %w", err)
	}
	return nil
}
