package cli

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"

	"github.com/spf13/cobra"

	"github.com/Maksim-Burtsev/go-reading/apps/01-cli-wordfreq/internal/dedupe"
	"github.com/Maksim-Burtsev/go-reading/apps/01-cli-wordfreq/internal/input"
)

type dedupeCommand struct {
	root      *rootCommand
	withCount bool
}

func newDedupeCommand(root *rootCommand) *cobra.Command {
	d := &dedupeCommand{root: root}
	cmd := &cobra.Command{
		Use:   "dedupe [file...]",
		Short: "Print every distinct line once, in first-seen order",
		Long: `Dedupe prints each distinct line of its inputs once, in the order the lines
are first seen. Unlike uniq, repeats do not have to be adjacent.

Without --count distinct lines are written while input is still being read.
With --count the output is written after all input is consumed.`,
		Example: `  zcat access.log.gz | cut -d' ' -f1 | wordfreq dedupe
  wordfreq dedupe --count a.txt b.txt`,
		RunE: d.run,
	}
	cmd.Flags().BoolVarP(&d.withCount, "count", "c", false, "prefix each line with the number of its occurrences")
	return cmd
}

func (d *dedupeCommand) run(cmd *cobra.Command, args []string) error {
	names, err := input.Names(args)
	if err != nil {
		return err
	}

	bw := bufio.NewWriter(cmd.OutOrStdout())
	var onFirst func(string) error
	if !d.withCount {
		onFirst = func(line string) error {
			_, err := fmt.Fprintln(bw, line)
			return err
		}
	}

	ctx := cmd.Context()
	set := dedupe.NewSet()
	err = set.ScanAll(ctx, input.Opener{Stdin: cmd.InOrStdin()}, names, onFirst)
	if err == nil && d.withCount {
		err = writeCounted(bw, set.Lines())
	}
	if flushErr := bw.Flush(); err == nil {
		err = flushErr
	}
	if err != nil {
		return fmt.Errorf("dedupe: %w", err)
	}

	d.root.logger.InfoContext(ctx, "dedupe finished",
		slog.Int("inputs", len(names)),
		slog.Int("lines", set.Total()),
		slog.Int("unique", set.Len()),
	)
	return nil
}

func writeCounted(w io.Writer, lines []dedupe.Line) error {
	for _, l := range lines {
		if _, err := fmt.Fprintf(w, "%7d %s\n", l.Count, l.Text); err != nil {
			return err
		}
	}
	return nil
}
