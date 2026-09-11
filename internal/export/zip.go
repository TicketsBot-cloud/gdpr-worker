package export

import (
	"archive/zip"
	"bytes"
	"compress/flate"
	"fmt"
	"hash/crc32"
	"io"
	"time"
)

// Two per message: Discord caps a request at 25 MiB and an attachment at 20 MiB, so 12 MiB uses
// the request budget almost exactly where a single 20 MiB part would waste a fifth of it.
const DefaultMaxPartBytes = 12 * 1024 * 1024

// Local header (30) + central directory record (46) + slack for extra fields the writer may emit.
const perEntryOverhead = 30 + 46 + 64

const trailerOverhead = 128

type Part struct {
	Name string
	Data []byte
}

// Each part is independently valid, so a recipient can open any one without the others.
type Builder struct {
	maxBytes int

	buf     *bytes.Buffer
	writer  *zip.Writer
	entries int
	// Central directory bytes the current part still has to write.
	reserved int

	done      [][]byte
	oversized []string
	total     int
}

func NewBuilder(maxBytes int) *Builder {
	if maxBytes <= 0 {
		maxBytes = DefaultMaxPartBytes
	}

	return &Builder{maxBytes: maxBytes}
}

func (b *Builder) start() {
	b.buf = new(bytes.Buffer)
	b.writer = zip.NewWriter(b.buf)
	b.entries = 0
	b.reserved = trailerOverhead
}

// An entry too large for even an empty part is recorded by Oversized and skipped, rather than
// emitting an archive Discord will reject.
func (b *Builder) Add(name string, data []byte) error {
	compressed, crc, err := deflate(data)
	if err != nil {
		return fmt.Errorf("failed to compress zip entry %q: %w", name, err)
	}

	cost := len(compressed) + perEntryOverhead + 2*len(name)

	if cost+trailerOverhead > b.maxBytes {
		b.oversized = append(b.oversized, name)
		return nil
	}

	if b.writer == nil {
		b.start()
	}

	if err := b.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush zip part: %w", err)
	}

	if b.entries > 0 && b.buf.Len()+b.reserved+cost > b.maxBytes {
		if err := b.closePart(); err != nil {
			return err
		}
		b.start()
	}

	header := &zip.FileHeader{
		Name:               name,
		Method:             zip.Deflate,
		Modified:           time.Now().UTC(),
		CRC32:              crc,
		CompressedSize64:   uint64(len(compressed)),
		UncompressedSize64: uint64(len(data)),
	}

	w, err := b.writer.CreateRaw(header)
	if err != nil {
		return fmt.Errorf("failed to create zip entry %q: %w", name, err)
	}

	if _, err := w.Write(compressed); err != nil {
		return fmt.Errorf("failed to write zip entry %q: %w", name, err)
	}

	b.entries++
	b.total++
	b.reserved += 46 + len(name) + 64

	return nil
}

func (b *Builder) closePart() error {
	if err := b.writer.Close(); err != nil {
		return fmt.Errorf("failed to close zip part: %w", err)
	}

	b.done = append(b.done, b.buf.Bytes())
	b.writer = nil
	b.buf = nil

	return nil
}

func (b *Builder) Count() int { return b.total }

func (b *Builder) Oversized() []string { return b.oversized }

func (b *Builder) Finish(baseName string) ([]Part, error) {
	if b.writer != nil {
		if b.entries == 0 {
			b.writer = nil
			b.buf = nil
		} else if err := b.closePart(); err != nil {
			return nil, err
		}
	}

	parts := make([]Part, len(b.done))
	for i, data := range b.done {
		name := fmt.Sprintf("%s.zip", baseName)
		if len(b.done) > 1 {
			name = fmt.Sprintf("%s_%d-%d.zip", baseName, i+1, len(b.done))
		}
		parts[i] = Part{Name: name, Data: data}
	}

	return parts, nil
}

func deflate(data []byte) ([]byte, uint32, error) {
	var out bytes.Buffer

	w, err := flate.NewWriter(&out, flate.DefaultCompression)
	if err != nil {
		return nil, 0, err
	}

	if _, err := io.Copy(w, bytes.NewReader(data)); err != nil {
		return nil, 0, err
	}

	if err := w.Close(); err != nil {
		return nil, 0, err
	}

	return out.Bytes(), crc32.ChecksumIEEE(data), nil
}
