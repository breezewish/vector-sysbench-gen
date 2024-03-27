package cmd

import (
	"os"

	"github.com/spf13/cobra"
	prettyconsole "github.com/thessem/zap-prettyconsole"
	"go.uber.org/zap"
)

var logger *zap.Logger

var rootCmd = &cobra.Command{
	Use:   "vector-gen",
	Short: "Generate vector data CSV files",
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		logger.Error("Exit with error", zap.Error(err))
		os.Exit(1)
	}
}

func init() {
	logger = prettyconsole.NewLogger(zap.DebugLevel)
}
