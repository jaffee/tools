package bench

import (
	"context"
	"fmt"
	"io"
	"time"
)

// NewSliceWidth creates a new slice width benchmark with stdin/out/err
// initialized.
func NewSliceWidth(stdin io.Reader, stdout, stderr io.Writer) *SliceWidth {
	return &SliceWidth{
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
	}
}

// SliceWidth helps importing data based on slice-width and data density.
// a single slice on query time.
type SliceWidth struct {
	HasClient
	hostSetup *HostSetup

	Name       string  `json:"name"`
	Index      string  `json:"index"`
	Frame      string  `json:"frame"`
	BitDensity float64 `json:"bit-density"`
	SliceWidth int64   `json:"slice-width"`
	SliceCount int64   `json:"slice-count"`

	Stdin  io.Reader `json:"-"`
	Stdout io.Writer `json:"-"`
	Stderr io.Writer `json:"-"`
}

// Init sets up the slice width.
func (b *SliceWidth) Init(hostSetup *HostSetup, agentNum int) error {
	b.Name = "slice-width"
	b.hostSetup = hostSetup

	err := initIndex(hostSetup.Hosts[0], hostSetup.ClientOptions, b.Index, b.Frame)
	if err != nil {
		return err
	}
	return b.HasClient.Init(hostSetup, agentNum)
}

// Run runs the SliceWidth to import data
func (b *SliceWidth) Run(ctx context.Context) *Result {
	bitDensity := b.BitDensity
	sliceCount := b.SliceCount
	numColumns := b.SliceWidth * sliceCount
	numRows := int64(1000)
	iteration := int64(float64(numColumns)*bitDensity) * numRows
	results := NewResult()
	imp := &Import{
		MaxRowID:     numRows,
		MinColumnID:  0,
		MaxColumnID:  numColumns,
		Iterations:   iteration,
		Index:        b.Index,
		Frame:        b.Frame,
		Distribution: "uniform",
		BufferSize:   1000000,
	}
	err := imp.Init(b.hostSetup, 0)
	if err != nil {
		results.err = fmt.Errorf("error initializing importer, err: %v", err)
	}
	start := time.Now()
	imp.Run(ctx)
	results.Add(time.Since(start), nil)

	return results
}
