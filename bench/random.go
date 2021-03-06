package bench

import (
	"context"
	"fmt"
	"math/rand"
	"time"
)

// RandomSetBits sets bits randomly and deterministically based on a seed.
type RandomSetBits struct {
	HasClient
	Name        string `json:"name"`
	MinRowID    int64  `json:"min-row-id"`
	MaxRowID    int64  `json:"max-row-id"`
	MinColumnID int64  `json:"min-column-id"`
	MaxColumnID int64  `json:"max-column-id"`
	Iterations  int    `json:"iterations"`
	Seed        int64  `json:"seed"`
	Index       string `json:"index"`
	Frame       string `json:"frame"`
}

// Init adds the agent num to the random seed and initializes the client.
func (b *RandomSetBits) Init(hostSetup *HostSetup, agentNum int) error {
	b.Name = "random-set-bits"
	b.Seed = b.Seed + int64(agentNum)
	err := b.HasClient.Init(hostSetup, agentNum)
	if err != nil {
		return err
	}
	return b.InitIndex(b.Index, b.Frame)
}

// Run runs the RandomSetBits benchmark
func (b *RandomSetBits) Run(ctx context.Context) *Result {
	src := rand.NewSource(b.Seed)
	rng := rand.New(src)
	results := NewResult()
	if b.client == nil {
		results.err = fmt.Errorf("No client set for RandomSetBits")
		return results
	}
	for n := 0; n < b.Iterations; n++ {
		rowID := rng.Int63n(b.MaxRowID - b.MinRowID)
		profID := rng.Int63n(b.MaxColumnID - b.MinColumnID)
		query := fmt.Sprintf("SetBit(frame='%s', rowID=%d, columnID=%d)", b.Frame, b.MinRowID+rowID, b.MinColumnID+profID)
		start := time.Now()
		_, err := b.ExecuteQuery(ctx, b.Index, query)
		results.Add(time.Since(start), nil)
		if err != nil {
			results.err = err
			return results
		}
	}
	return results
}
