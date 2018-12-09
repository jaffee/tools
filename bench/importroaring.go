package bench

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	gopilosa "github.com/pilosa/go-pilosa"
	pilosa "github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/roaring"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

var _ Benchmark = (*ImportRoaringBenchmark)(nil)

// ImportRoaringBenchmark benchmarks importing roaring data into a single Pilosa shard.
type ImportRoaringBenchmark struct {
	Name        string      `json:"name"`
	NumRows     uint64      `json:"num-rows"`
	Concurrency int         `json:"concurrency" short:"o"`
	Index       string      `json:"index"`
	Field       string      `json:"field"`
	Seed        int64       `json:"seed"`
	Logger      *log.Logger `json:"-"`
}

// NewImportRoaringBenchmark returns a new instance of ImportRoaringBenchmark.
func NewImportRoaringBenchmark() *ImportRoaringBenchmark {
	return &ImportRoaringBenchmark{
		Name:        "import-roaring",
		NumRows:     10,
		Concurrency: 1,
		Index:       "i",
		Field:       "f",
		Logger:      log.New(os.Stderr, "", log.LstdFlags),
	}
}

// Run runs the Import benchmark
func (b *ImportRoaringBenchmark) Run(ctx context.Context, client *gopilosa.Client, agentNum int) (*Result, error) {
	result := NewResult()
	result.AgentNum = agentNum
	result.Configuration = b

	// Initialize schema.
	_, field, err := ensureSchema(client, b.Index, b.Field)
	if err != nil {
		return result, err
	}

	start := time.Now()
	fragmentNodeTimes := make([]time.Duration, b.Concurrency)
	importTimes := make([]time.Duration, b.Concurrency)
	eg := errgroup.Group{}
	data := getZipfRowsSliceRoaring(b.NumRows, b.Seed+int64(agentNum))
	for i := 0; i < b.Concurrency; i++ {
		i := i
		eg.Go(func() error {
			start := time.Now()
			nodes, err := client.FetchFragmentNodes(b.Index, 0)
			if err != nil {
				return errors.Wrap(err, "getting ndoes for shard")
			}
			fragmentNodeTimes[i] = time.Since(start)
			start = time.Now()
			err = client.ImportRoaringBitmap(
				nodes[0].URI(), field, uint64(i), gopilosa.ViewImports{"standard": data}, &gopilosa.ImportOptions{})
			importTimes[i] = time.Since(start)
			return errors.Wrap(err, "importing")
		})
	}
	eg.Wait()
	result.Stats.Total = time.Since(start)
	result.Extra["total"] = result.Stats.Total.String()
	result.Extra["import-times"] = fmt.Sprintf("%s", importTimes)
	result.Extra["fragment-node-times"] = fmt.Sprintf("%s", fragmentNodeTimes)
	return result, errors.Wrap(err, "importing bitmap")
}

// getZipfRowsSliceRoaring generates a random fragment with the given number of
// rows, and 1 bit set in each column. The row each bit is set in is chosen via
// the Zipf generator, and so will be skewed toward lower row numbers. If this
// is edited to change the data distribution, getZipfRowsSliceStandard should be
// edited as well.
func getZipfRowsSliceRoaring(numRows uint64, seed int64) *roaring.Bitmap {
	b := roaring.NewBitmap()
	s := rand.NewSource(seed)
	r := rand.New(s)
	z := rand.NewZipf(r, 1.6, 50, numRows-1)
	for i := uint64(0); i < pilosa.ShardWidth; i++ {
		row := z.Uint64()
		b.DirectAdd(row*pilosa.ShardWidth + i)
	}

	return b
}
