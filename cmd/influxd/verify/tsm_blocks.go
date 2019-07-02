package verify

import (
	"errors"
	"os"

	"github.com/influxdata/influxdb/tsdb/tsm1"
	"github.com/spf13/cobra"
)

// tsmBlocksFlags defines the `tsm-blocks` Command.
var tsmBlocksFlags = struct {
	pattern  string
	exact    bool
	detailed bool

	orgID, bucketID string
	path            string
}{}

func newTSMBlocksCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:  "tsm-blocks",
		RunE: verifyTSMBlocks,
	}

	cmd.Flags().StringVarP(&tsmBlocksFlags.pattern, "pattern", "", "", "only process TSM files containing pattern")

	return cmd
}

func verifyTSMBlocks(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return errors.New("missing tsm file")
	}

	v := tsm1.VerifyTSMBlocks{
		Stderr: os.Stderr,
		Stdout: os.Stdout,
		Path:   args[0],
	}

	return v.Run()
}
