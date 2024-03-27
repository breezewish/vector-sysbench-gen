package cmd

import (
	"bufio"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
	"vector-sysbench-gen/utils"

	"github.com/alitto/pond"
	"github.com/dustin/go-humanize"
	"github.com/pelletier/go-toml/v2"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Start the generator workload",
	Run: func(cmd *cobra.Command, args []string) {
		err := run()
		if err != nil {
			logger.Error("Failed", zap.Error(err))
			os.Exit(1)
		}
	},
}

type Flags struct {
	DB          string
	Table       string
	N           int
	Rows        int
	Dimensions  int
	Output      string
	SplitBytes  int
	Concurrency int

	// Derived Properties
	OutputDirectory string
	SplitRows       int
}

var flags Flags

func init() {
	rootCmd.AddCommand(runCmd)

	flags = Flags{
		DB:          "test",
		Table:       "sbtest",
		N:           32,
		Rows:        13000,
		Dimensions:  1536,
		Output:      "./output",
		SplitBytes:  100 * 1000 * 1000,
		Concurrency: 16,
	}

	runCmd.Flags().StringVar(&flags.DB, "db", flags.DB, "Database name")
	runCmd.Flags().StringVar(&flags.Table, "table", flags.Table, "Table name")
	runCmd.Flags().IntVar(&flags.N, "n", flags.N, "Number of tables")
	runCmd.Flags().IntVarP(&flags.Rows, "rows", "r", flags.Rows, "Number of rows per table")
	runCmd.Flags().IntVar(&flags.Dimensions, "dimensions", flags.Dimensions, "Vector dimensions")
	runCmd.Flags().StringVarP(&flags.Output, "output", "o", flags.Output, "Output directory")
	runCmd.Flags().IntVar(&flags.SplitBytes, "split-bytes", flags.SplitBytes, "Size per file (count by vector bytes)")
	runCmd.Flags().IntVar(&flags.Concurrency, "concurrency", flags.Concurrency, "")
}

type taskInfo struct {
	tableIndex     int
	pkStart        int
	pkEnd          int
	filePartialIdx int // only used in output file name
}

var pgTracker *utils.ProgressTracker

func run() error {
	flags.SplitRows = flags.SplitBytes / (flags.Dimensions * 4)
	flags.OutputDirectory = path.Join(flags.Output, fmt.Sprintf(
		"vector_%d_%d_%d_%d",
		flags.N,
		flags.Rows,
		flags.Dimensions,
		time.Now().UnixNano()))
	filesPerTable := flags.Rows / flags.SplitRows
	if flags.Rows%flags.SplitRows != 0 {
		filesPerTable++
	}

	if err := os.MkdirAll(flags.OutputDirectory, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create output directory %s: %w", flags.OutputDirectory, err)
	}

	logger.Info("Start writing data",
		zap.Int("tables", flags.N),
		zap.Int("rows-per-table", flags.Rows),
		zap.Int("rows-per-file", flags.SplitRows),
		zap.Int("vec-dims", flags.Dimensions),
		zap.String("output-directory", flags.OutputDirectory),
		zap.String("total-vec-size", humanize.Bytes(uint64(flags.Dimensions*flags.Rows*flags.N*4))),
	)

	// Prepare progress
	pgTracker = utils.NewProgressTracker(flags.N, flags.Rows, flags.Table)

	// Prepare schema files
	if err := prepareDBSchemaFile(); err != nil {
		return fmt.Errorf("failed to write db schema: %w", err)
	}
	if err := prepareLightningConfigFile(); err != nil {
		return fmt.Errorf("failed to write lightning config: %w", err)
	}
	for tableIdx := 0; tableIdx < flags.N; tableIdx++ {
		if err := prepareTableSchemaFile(tableIdx); err != nil {
			return fmt.Errorf("failed to write table schema: %w", err)
		}
	}

	// Prepare data tasks
	pool := pond.New(flags.Concurrency, filesPerTable*flags.N)

	for tableIdx := 0; tableIdx < flags.N; tableIdx++ {
		for filePartialIdx := 0; filePartialIdx < filesPerTable; filePartialIdx++ {

			pkStart := filePartialIdx * flags.SplitRows
			pkEnd := pkStart + flags.SplitRows
			if pkEnd > flags.Rows {
				pkEnd = flags.Rows
			}

			task := taskInfo{
				tableIndex:     tableIdx,
				pkStart:        pkStart,
				pkEnd:          pkEnd,
				filePartialIdx: filePartialIdx,
			}

			pool.Submit(task.run)

		}
	}

	pool.StopAndWait()
	pgTracker.Wait()

	return nil
}

func (t *taskInfo) run() {
	f, err := os.Create(path.Join(flags.OutputDirectory, fmt.Sprintf(
		"%s.%s%d.%06d.csv",
		flags.DB,
		flags.Table,
		t.tableIndex+1,
		t.filePartialIdx+1,
	)))

	if err != nil {
		logger.Error("Failed to create file",
			zap.Error(err),
			zap.String("file", f.Name()))
		panic(err)
	}

	defer f.Close()

	w := bufio.NewWriter(f)
	defer w.Flush()

	finishedRow := 0
	var finishedBytes uint64 = 0

	// Add header by default
	fmt.Fprintf(w, "id,k,vec\n")

	for rowId := t.pkStart; rowId < t.pkEnd; rowId++ {
		// ColPK
		fmt.Fprintf(w, "%d,", rowId)
		// ColK
		fmt.Fprintf(w, "%d,", utils.RandRange(1, flags.Rows))
		// ColVec
		fmt.Fprintf(w, "\"[")
		for dim, dimMax := 0, flags.Dimensions-1; dim < dimMax; dim++ {
			fmt.Fprintf(w, "%.2g,", utils.RandFloat32Range(-1.0, 1.0))
		}
		fmt.Fprintf(w, "%.2g]\"\n", utils.RandFloat32Range(-1.0, 1.0))

		finishedRow++
		finishedBytes += uint64(flags.Dimensions) * 4
		if rowId%1000 == 0 {
			pgTracker.AddFinishedRow(t.tableIndex, finishedRow, finishedBytes)
			finishedRow = 0
			finishedBytes = 0
		}
	}

	pgTracker.AddFinishedRow(t.tableIndex, finishedRow, finishedBytes)
}

func prepareTableSchemaFile(tableIdx int) error {
	template := `
CREATE TABLE sbtest%d (
  id INTEGER NOT NULL AUTO_INCREMENT,
  k INTEGER DEFAULT '0' NOT NULL,
  vec VECTOR<FLOAT>(%d) NOT NULL COMMENT 'hnsw(distance=cosine)',
  PRIMARY KEY (id)
);
	`

	data := fmt.Sprintf(
		template,
		tableIdx+1,
		flags.Dimensions)

	filePath := path.Join(flags.OutputDirectory, fmt.Sprintf(
		"%s.%s%d-schema.sql",
		flags.DB,
		flags.Table,
		tableIdx+1,
	))

	return os.WriteFile(filePath, []byte(strings.TrimSpace(data)), 0644)
}

func prepareDBSchemaFile() error {
	template := `
CREATE DATABASE IF NOT EXISTS %s;
	`

	data := fmt.Sprintf(
		template,
		flags.DB)

	filePath := path.Join(flags.OutputDirectory, fmt.Sprintf(
		"%s-schema-create.sql",
		flags.DB,
	))

	return os.WriteFile(filePath, []byte(strings.TrimSpace(data)), 0644)
}

func prepareLightningConfigFile() error {
	sortedKvDiv, err := filepath.Abs(path.Join(flags.Output, "sorted-kv"))
	if err != nil {
		return err
	}

	dataDir, err := filepath.Abs(flags.OutputDirectory)
	if err != nil {
		return err
	}

	cfg := map[string]any{
		"lightning": map[string]any{
			"level": "info",
			"file":  "tidb-lightning.log",
		},
		"tikv-importer": map[string]any{
			"backend":       "local",
			"sorted-kv-dir": sortedKvDiv,
			"keyspace-name": "mykeyspace",
		},
		"mydumper": map[string]any{
			"data-source-dir": dataDir,
		},
		"tidb": map[string]any{
			"host":     "127.0.0.1",
			"port":     4000,
			"user":     "root",
			"password": "",
			"pd-addr":  "127.0.0.1:2379",
		},
	}
	b, err := toml.Marshal(cfg)
	if err != nil {
		return err
	}
	filePath := path.Join(flags.OutputDirectory, "lightning.toml")
	return os.WriteFile(filePath, b, 0644)
}
