package export

import (
	"archive/zip"
	"bytes"
	"fmt"
)

// ZipBuilder provides an in-memory ZIP archive builder for constructing
// data export archives without writing to disk.
type ZipBuilder struct {
	buf    *bytes.Buffer
	writer *zip.Writer
	closed bool
}

// NewZipBuilder creates a new ZipBuilder that writes to an in-memory buffer.
func NewZipBuilder() *ZipBuilder {
	buf := new(bytes.Buffer)
	return &ZipBuilder{
		buf:    buf,
		writer: zip.NewWriter(buf),
	}
}

// AddFile adds a file with the given name and data to the ZIP archive.
func (z *ZipBuilder) AddFile(name string, data []byte) error {
	if z.closed {
		return fmt.Errorf("zip builder is already closed")
	}

	f, err := z.writer.Create(name)
	if err != nil {
		return fmt.Errorf("failed to create zip entry %q: %w", name, err)
	}

	if _, err := f.Write(data); err != nil {
		return fmt.Errorf("failed to write zip entry %q: %w", name, err)
	}

	return nil
}

// Close finalises the ZIP archive and returns the resulting bytes.
func (z *ZipBuilder) Close() ([]byte, error) {
	if z.closed {
		return nil, fmt.Errorf("zip builder is already closed")
	}

	z.closed = true

	if err := z.writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close zip writer: %w", err)
	}

	return z.buf.Bytes(), nil
}
