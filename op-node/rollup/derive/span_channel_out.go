package derive

import (
	"bytes"
	"compress/zlib"
	"crypto/rand"
	"fmt"
	"io"
	"math/big"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"

	"github.com/ethereum-optimism/optimism/op-node/rollup"
)

type SpanChannelOut struct {
	id ChannelID
	// Frame ID of the next frame to emit. Increment after emitting
	frame uint64
	// rlp is the encoded, uncompressed data of the channel. length must be less than MAX_RLP_BYTES_PER_CHANNEL
	// it is a double buffer to allow us to "undo" the last change to the RLP structure when the target size is exceeded
	rlp [2]*bytes.Buffer
	// lastCompressedRLPSize tracks the *uncompressed* size of the last RLP buffer that was compressed
	// it is used to measure the growth of the RLP buffer when adding a new batch to optimize compression
	lastCompressedRLPSize int
	// rlpIndex is the index of the current rlp buffer
	rlpIndex int
	// compressed contains compressed data for making output frames
	compressed *bytes.Buffer
	// compress is the zlib writer for the channel
	compressor *zlib.Writer
	// target is the target size of the compressed data
	target uint64
	// closed indicates if the channel is closed
	closed bool
	// spanBatch is the batch being built, which immutably holds genesis timestamp and chain ID, but otherwise can be reset
	spanBatch *SpanBatch
}

func (co *SpanChannelOut) ID() ChannelID {
	return co.id
}

func (co *SpanChannelOut) randomID() error {
	_, err := rand.Read(co.id[:])
	if err != nil {
		return err
	}
	return nil
}

func NewSpanChannelOut(genesisTimestamp uint64, chainID *big.Int, targetOutputSize uint64) (*SpanChannelOut, error) {
	c := &SpanChannelOut{
		id:         ChannelID{},
		frame:      0,
		spanBatch:  NewSpanBatch(genesisTimestamp, chainID),
		rlp:        [2]*bytes.Buffer{{}, {}},
		compressed: &bytes.Buffer{},
		target:     targetOutputSize,
	}
	var err error
	if err = c.randomID(); err != nil {
		return nil, err
	}
	if c.compressor, err = zlib.NewWriterLevel(c.compressed, zlib.BestCompression); err != nil {
		return nil, err
	}
	return c, nil
}

func (co *SpanChannelOut) Reset() error {
	co.closed = false
	co.frame = 0
	co.rlp[0].Reset()
	co.rlp[1].Reset()
	co.lastCompressedRLPSize = 0
	co.compressed.Reset()
	co.compressor.Reset(co.compressed)
	co.spanBatch = NewSpanBatch(co.spanBatch.GenesisTimestamp, co.spanBatch.ChainID)
	// setting the new randomID is the only part of the reset that can fail
	return co.randomID()
}

func (co *SpanChannelOut) activeRLP() *bytes.Buffer {
	return co.rlp[co.rlpIndex]
}

func (co *SpanChannelOut) switchRLP() {
	co.rlpIndex = (co.rlpIndex + 1) % 2
}

func (co *SpanChannelOut) AddBlock(rollupCfg *rollup.Config, block *types.Block) (uint64, error) {
	if co.closed {
		return 0, ErrChannelOutAlreadyClosed
	}

	batch, l1Info, err := BlockToSingularBatch(rollupCfg, block)
	if err != nil {
		return 0, err
	}
	return co.AddSingularBatch(batch, l1Info.SequenceNumber)
}

func (co *SpanChannelOut) AddSingularBatch(batch *SingularBatch, seqNum uint64) (uint64, error) {
	//fmt.Println("adding singular batch")
	// sentinel error for closed channel
	if co.closed {
		//fmt.Println("channel already closed")
		return 0, ErrChannelOutAlreadyClosed
	}

	//fmt.Println("rlp lens", co.rlp[0].Len(), co.rlp[1].Len())

	// update the SpanBatch with the SingularBatch
	if err := co.spanBatch.AppendSingularBatch(batch, seqNum); err != nil {
		//fmt.Println("failed to append singular batch")
		return 0, fmt.Errorf("failed to append SingularBatch to SpanBatch: %w", err)
	}
	// convert Span batch to RawSpanBatch
	rawSpanBatch, err := co.spanBatch.ToRawSpanBatch()
	if err != nil {
		//fmt.Println("failed to convert to raw")
		return 0, fmt.Errorf("failed to convert SpanBatch into RawSpanBatch: %w", err)
	}

	// switch to the other buffer and reset it
	co.switchRLP()
	co.activeRLP().Reset()
	if err = rlp.Encode(co.activeRLP(), NewBatchData(rawSpanBatch)); err != nil {
		//fmt.Println("failed to rlp encode")
		return 0, fmt.Errorf("failed to encode RawSpanBatch into bytes: %w", err)
	}
	// check the RLP length against the max
	if co.activeRLP().Len() > MaxRLPBytesPerChannel {
		//fmt.Println("rlp too big")
		return 0, fmt.Errorf("could not take %d bytes as replacement of channel of %d bytes, max is %d. err: %w",
			co.lastCompressedRLPSize, co.lastCompressedRLPSize, MaxRLPBytesPerChannel, ErrTooManyRLPBytes)
	}

	// if the compressed data *plus* the new rlp data is under the target size, return early
	// this optimizes out cases where the compressor will obviously come in under the target size
	rlpGrowth := co.activeRLP().Len() - co.lastCompressedRLPSize
	if uint64(co.compressed.Len()+rlpGrowth) < co.target {
		//fmt.Println("growth early return: ", co.rlpIndex, ":", co.compressed.Len(), "+", rlpGrowth, "<", co.target)
		return uint64(co.lastCompressedRLPSize), nil
	}

	//fmt.Println("rlp lens", co.rlp[0].Len(), co.rlp[1].Len())

	//fmt.Println("compressing")
	// we must compress the data to check if we've met or exceeded the target size
	co.freshCompress()
	co.lastCompressedRLPSize = co.activeRLP().Len()

	if uint64(co.compressed.Len()) > co.target {
		//fmt.Println("past target", co.compressed.Len(), co.target)
		// if there is only one batch in the channel, it *must* be returned
		if len(co.spanBatch.Batches) == 1 {
			co.Close()
			return uint64(co.compressed.Len()), nil
		}

		// if there is more than one batch in the channel, we revert the last batch by switching the RLP buffer
		co.switchRLP()
		co.freshCompress()
		co.Close()
		return uint64(co.compressed.Len()), ErrCompressorFull
	}

	return uint64(co.compressed.Len()), nil
}

func (co *SpanChannelOut) freshCompress() {
	//fmt.Println("compressed was", co.compressed.Len())
	co.compressed.Reset()
	//fmt.Println("after reset", co.compressed.Len())
	co.compressor.Reset(co.compressed)
	//fmt.Println("active rlp len", co.activeRLP().Len())
	co.compressor.Write(co.activeRLP().Bytes())
	co.compressor.Flush()
	//fmt.Println("compressed is", co.compressed.Len())
}

// InputBytes returns the total amount of RLP-encoded input bytes.
func (co *SpanChannelOut) InputBytes() int {
	return co.lastCompressedRLPSize
}

// ReadyBytes returns the total amount of compressed bytes that are ready to be output.
// Span Channel Out does not provide early output, so this will always be 0 until the channel is closed.
func (co *SpanChannelOut) ReadyBytes() int {
	if co.closed {
		return co.compressed.Len()
	}
	return 0
}

// Flush flushes the internal compression stage to the ready buffer. It enables pulling a larger & more
// complete frame. It reduces the compression efficiency.
func (co *SpanChannelOut) Flush() error {
	return nil
}

func (co *SpanChannelOut) FullErr() error {
	if uint64(co.compressed.Len()) >= co.target {
		return ErrCompressorFull
	}
	return nil
}

func (co *SpanChannelOut) Close() error {
	if co.closed {
		return ErrChannelOutAlreadyClosed
	}
	co.closed = true
	if err := co.Flush(); err != nil {
		return err
	}
	return co.compressor.Close()
}

// OutputFrame writes a frame to w with a given max size and returns the frame
// number.
// Use `ReadyBytes`, `Flush`, and `Close` to modify the ready buffer.
// Returns an error if the `maxSize` < FrameV0OverHeadSize.
// Returns io.EOF when the channel is closed & there are no more frames.
// Returns nil if there is still more buffered data.
// Returns an error if it ran into an error during processing.
func (co *SpanChannelOut) OutputFrame(w *bytes.Buffer, maxSize uint64) (uint16, error) {
	// Check that the maxSize is large enough for the frame overhead size.
	if maxSize < FrameV0OverHeadSize {
		return 0, ErrMaxFrameSizeTooSmall
	}

	f := createEmptyFrame(co.id, co.frame, co.ReadyBytes(), co.closed, maxSize)

	if _, err := io.ReadFull(co.compressed, f.Data); err != nil {
		return 0, err
	}

	if err := f.MarshalBinary(w); err != nil {
		return 0, err
	}

	co.frame += 1
	fn := f.FrameNumber
	if f.IsLast {
		return fn, io.EOF
	} else {
		return fn, nil
	}
}
