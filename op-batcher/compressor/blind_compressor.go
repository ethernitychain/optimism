package compressor

import (
	"bytes"
	"compress/zlib"
	"fmt"

	"github.com/ethereum-optimism/optimism/op-node/rollup/derive"
)

// BlindCompressor is a simple compressor that blindly compresses data
// the only way to know if the target size has been reached is to first flush the buffer
// and then check the length of the compressed data
type BlindCompressor struct {
	config Config

	inputBytes int
	buf        bytes.Buffer
	compress   *zlib.Writer
	buildbuf   []byte
}

// NewBlindCompressor creates a new derive.Compressor implementation that compresses
func NewBlindCompressor(config Config) (derive.Compressor, error) {
	c := &BlindCompressor{
		config: config,
	}

	compress, err := zlib.NewWriterLevel(&c.buf, zlib.BestCompression)
	if err != nil {
		return nil, err
	}
	c.compress = compress

	return c, nil
}

func (t *BlindCompressor) TargetOutputSize() uint64 {
	return t.config.TargetOutputSize
}

func (t *BlindCompressor) Write(p []byte) (int, error) {
	// always start by flushing for an accurate size of compressed data
	t.compress.Flush()
	// grow the building buffer with the new input
	t.buildbuf = append(t.buildbuf, p...)
	// if the buffer plus the already compressed data is under the target size, return
	// once enough data has been written, the buffer will be flushed and compressed
	if uint64(len(t.buildbuf)+t.buf.Len()) < t.config.TargetOutputSize {
		fmt.Println("returning early")
		return len(p), nil
	}
	if err := t.FullErr(); err != nil {
		return 0, err
	}
	t.inputBytes += len(p)
	t.buildbuf = nil
	fmt.Println("compressing")
	return t.compress.Write(p)
}

func (t *BlindCompressor) Close() error {
	return t.compress.Close()
}

func (t *BlindCompressor) Read(p []byte) (int, error) {
	return t.buf.Read(p)
}

func (t *BlindCompressor) Reset() {
	t.buf.Reset()
	t.compress.Reset(&t.buf)
	t.inputBytes = 0
}

func (t *BlindCompressor) Len() int {
	return t.buf.Len()
}

func (t *BlindCompressor) Flush() error {
	return t.compress.Flush()
}

// FullErr returns an error if the target output size has been reached.
// Flush *must* be called before this method to ensure the buffer is up to date
func (t *BlindCompressor) FullErr() error {
	if uint64(t.Len()) >= t.config.TargetOutputSize {
		return derive.ErrCompressorFull
	}
	return nil
}
