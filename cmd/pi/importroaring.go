package main

import (
	"context"
	"os"

	"github.com/jaffee/commandeer"
	"github.com/pilosa/tools/bench"
	"github.com/spf13/cobra"
)

func NewImportRoaringCommand() *cobra.Command {
	b := bench.NewImportRoaringBenchmark()
	cmd := &cobra.Command{
		Use:   "import-roaring",
		Short: "ImportRoaring random data into Pilosa quickly.",
		Long:  `import generates random data which can be controlled by command line flags and streams it into Pilosa's /import endpoint. Agent num has no effect`,
		RunE: func(cmd *cobra.Command, args []string) error {
			flags := cmd.Flags()
			b.Logger = NewLoggerFromFlags(flags)
			client, err := NewClientFromFlags(flags)
			if err != nil {
				return err
			}
			agentNum, err := flags.GetInt("agent-num")
			if err != nil {
				return err
			}
			result, err := b.Run(context.Background(), client, agentNum)
			if err != nil {
				result.Error = err.Error()
			}
			return PrintResults(cmd, result, os.Stdout)
		},
	}

	flags := cmd.Flags()
	err := commandeer.Flags(flags, b)
	if err != nil {
		panic(err)
	}

	return cmd
}
