package utils

import (
	"fmt"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/fatih/color"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
	"go.uber.org/atomic"
)

type ProgressTracker struct {
	mu sync.Mutex

	progress *mpb.Progress

	totalBytes atomic.Uint64

	barTotal    *mpb.Bar
	barPerTable []*mpb.Bar

	currentSpeed atomic.String
	isFinished   atomic.Bool
}

func NewProgressTracker(tables int, rowsPerTable int, tableName string) *ProgressTracker {
	progress := mpb.New(
		mpb.WithOutput(color.Output),
		mpb.WithWidth(60),
	)
	pgTracker := &ProgressTracker{
		progress:    progress,
		barPerTable: make([]*mpb.Bar, 0, tables),
		totalBytes:  atomic.Uint64{},
		isFinished:  atomic.Bool{},
	}

	colorTotal := color.New(color.Bold)
	colorSpeed := color.New(color.FgHiCyan, color.Bold)

	for tableIdx := 0; tableIdx < tables; tableIdx++ {
		bar := progress.AddBar(
			int64(rowsPerTable),
			mpb.PrependDecorators(
				decor.Name(fmt.Sprintf("%s%d", tableName, tableIdx+1)),
			),
			mpb.AppendDecorators(decor.Percentage(decor.WC{W: 5})),
		)
		pgTracker.barPerTable = append(pgTracker.barPerTable, bar)
	}
	pgTracker.barTotal = progress.AddBar(
		int64(rowsPerTable*tables),
		mpb.PrependDecorators(
			decor.Name(colorTotal.Sprint("Total")),
		),
		mpb.AppendDecorators(
			decor.Any(func(s decor.Statistics) string {
				return colorSpeed.Sprintf("%s/s ", pgTracker.currentSpeed.Load())
			}),
			decor.Percentage(decor.WC{W: 5}),
		),
	)

	go pgTracker.speedCalcWorker()

	return pgTracker
}

func (t *ProgressTracker) AddFinishedRow(tableIdx int, rows int, bytes uint64) {
	t.totalBytes.Add(bytes)

	t.mu.Lock()
	defer t.mu.Unlock()
	t.barTotal.IncrBy(rows)
	t.barPerTable[tableIdx].IncrBy(rows)
}

func (t *ProgressTracker) speedCalcWorker() {
	var lastBytes uint64 = 0
	var lastTime time.Time = time.Now()

	for {
		time.Sleep(time.Second)

		currentBytes := t.totalBytes.Load()
		currentTime := time.Now()

		speedOfBytes := float64(currentBytes-lastBytes) / currentTime.Sub(lastTime).Seconds()
		t.currentSpeed.Store(humanize.Bytes(uint64(speedOfBytes)))

		if t.isFinished.Load() {
			return
		}
	}
}

func (t *ProgressTracker) Wait() {
	t.progress.Wait()
	t.isFinished.Store(true)
}
