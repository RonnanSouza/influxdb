package inspect

import (
	"context"
	"fmt"
	"io/ioutil"
	"path/filepath"

	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/tsi1"
	"github.com/influxdata/influxdb/tsdb/tsm1"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func NewCompactCommand() *cobra.Command {
	var dataDir string
	var level int
	var fast bool

	cmd := &cobra.Command{
		Use:   `compact`,
		Short: "Compacts a set of tsm1 files",
		Long: `
This command will execute a compaction on a given set of files.
TSM1 will automatically handle compactions during normal operation
and this command should only be used for debugging.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()

			// Generate series file & index in temporary directory.
			dir, err := ioutil.TempDir("", "")
			if err != nil {
				return err
			}
			sfile := tsdb.NewSeriesFile(filepath.Join(dir, "series"))
			if err := sfile.Open(ctx); err != nil {
				return fmt.Errorf("cannot open series file: %s", err)
			}
			defer sfile.Close()

			idx := tsi1.NewIndex(sfile, tsi1.NewConfig(), tsi1.WithPath(filepath.Join(dir, "index")))
			if err := idx.Open(ctx); err != nil {
				return fmt.Errorf("cannot open index: %s", err)
			}
			defer idx.Close()

			if dataDir, err = filepath.Abs(dataDir); err != nil {
				return err
			}

			logger, _ := zap.NewProduction()
			defer logger.Sync()

			// Open engine based on data directory argument.
			engine := tsm1.NewEngine(dataDir, idx, tsm1.NewConfig())
			engine.WithLogger(logger)
			if err := engine.Open(ctx); err != nil {
				return fmt.Errorf("cannot open engine: %s", err)
			}
			defer engine.Close()

			// Convert string arguments to a compaction group.
			group := make(tsm1.CompactionGroup, len(args))
			for i, arg := range args {
				if group[i], err = filepath.Abs(arg); err != nil {
					return err
				}
			}

			// Execute compaction.
			engine.CompactGroup(ctx, group, level, fast)
			return nil
		},
	}

	cmd.Flags().StringVarP(&dataDir, "data-dir", "", "", "shard data directory")
	cmd.Flags().IntVarP(&level, "level", "", 0, "compaction level")
	cmd.Flags().BoolVarP(&fast, "fast", "", false, "enable fast compaction")

	cmd.MarkFlagRequired("data-dir")
	cmd.MarkFlagRequired("level")

	return cmd
}
