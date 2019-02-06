package apophenia

import (
	"errors"
	"math/bits"
)

// PermutationGenerator provides a way to pass integer IDs through a permutation
// map that is pseudorandom but repeatable. This could be done with rand.Perm,
// but that would require storing a [Iterations]int64 array, which we want to avoid
// for large values of Iterations.
//
// Not actually cryptographically secure.
type PermutationGenerator struct {
	src     Sequence
	permRow uint32
	max     int64
	counter int64
	rounds  int
	bits    Uint128
	k       []uint64
}

// Design notes:
//
// This is based on:
// http://arxiv.org/abs/1208.1176v2
//
// This simulates the results of a shuffle in a way allowing a lookup of
// the results of the shuffle for any given position, in time proportional
// to a number of "rounds", each of which is 50% likely to swap a slot
// with another slot. The number of rounds needed to achieve a reasonable
// probability of safety is log(N)*6 or so.
//
// Each permutation is fully defined by a "key", consisting of:
//   1. A key "KF" naming a value in [0,max) for each round.
//   2. A series of round functions mapping values in [0,max) to bits,
//      one for each round.
// I refer to these as K[r] and F[r]. Thus, K[0] is the index used to
// compute swap operations four round 0, and F[0] is the series of bits
// used to determine whether a swap is performed, with F[0][0] being
// the swap decision for slot 0 in round 0. (Except it probably isn't,
// because the swap decision is actually made based on the highest index
// in a pair, to ensure that a swap between A and B always uses the same
// decision bit.)
//
// For K values, we set byte 8 of the plain text to 0x01, and use
// encoding/binary to dump r into the first 8 bytes of the plain text. The
// value K[r] is then the first 8 bytes of the resulting value, converted to
// uint64, mod max. This is slightly biased. We don't care.
//
// For F values, we set byte 8 of the plain text to 0x00, and use
// encoding/binary to dump the slot number into the first 8 bytes. This
// yields 128 values, which we treat as the values for the first 128 rounds,
// and then recycle for rounds 129+ if those exist. This is not very
// secure, but we're already at 1/2^128 chances by that time and don't care.
// We could probably trim rounds to 64 or so and not lose much data.

// NewPermutationGenerator creates a PermutationGenerator which will
// generate values in [0,m), given the provided seed.
//
// The row parameter selects different shuffles, and is useful if you need
// to generate multiple distinct shuffles from the same underlying sequence.
func NewPermutationGenerator(max int64, row uint32, src Sequence) (*PermutationGenerator, error) {
	if max < 1 {
		return nil, errors.New("period must be positive")
	}
	// number of rounds to get "good" results is roughly 6 log N. but that gets
	// pretty slow, so we use fewer.
	bits := 64 - bits.LeadingZeros64(uint64(max))
	p := PermutationGenerator{max: max, rounds: 6 * bits, counter: 0}

	p.src = src
	p.k = make([]uint64, p.rounds)
	p.permRow = row
	offset := OffsetFor(SequencePermutationK, p.permRow, 0, 0)
	for i := uint64(0); i < uint64(p.rounds); i++ {
		offset.SetLow(i)
		bits := p.src.BitsAt(offset)
		crypt := bits.Low()
		p.k[i] = crypt % uint64(p.max)
	}
	return &p, nil
}

// Next generates the next value from the permutation generator.
func (p *PermutationGenerator) Next() (ret int64) {
	return p.nextValue()
}

// Nth generates the Nth value from the permutation generator. For instance,
// given a new permutation generator, calling Next() once produces the same
// value you'd get from calling Nth(0). Calling Next() twice produces the same
// value as calling Nth(1). The Nth call changes the location of the permutation
// generator within its sequence.
func (p *PermutationGenerator) Nth(n int64) (ret int64) {
	p.counter = n
	ret = p.nextValue()
	return ret
}

func (p *PermutationGenerator) nextValue() int64 {
	p.counter++
	p.counter = int64(uint64(p.counter) % uint64(p.max))
	x := uint64(p.counter)
	// a value which can't possibly be the next value we need, so we
	// always hash on the first pass.
	prev := uint64(p.max) + 1
	offset := OffsetFor(SequencePermutationF, p.permRow, 0, 0)
	for i := uint64(0); i < uint64(p.rounds); i++ {
		xPrime := (p.k[i] + uint64(p.max) - x) % uint64(p.max)
		xCaret := x
		if xPrime > xCaret {
			xCaret = xPrime
		}
		if xCaret != prev {
			offset.lo = xCaret
			p.bits = p.src.BitsAt(offset)
			prev = xCaret
		}
		if p.bits.Bit(i) != 0 {
			x = xPrime
		}
	}
	return int64(x)
}