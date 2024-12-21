package io

import (
	"encoding/binary"
	"github.com/hauke96/sigolo/v2"
	"github.com/pkg/errors"
	"io"
	"math"
	"os"
)

type IndexedReader struct {
	file         *os.File
	offsetInFile int64
	buffer       []byte
	// The len(buffer) function returns always the same size since it's a fixed sized buffer. This number represents the
	// actual number of bytes in the buffer. Since the buffer-array is reused, it may contain data that doesn't belong
	//to the last read data. However, caller to the read function only receive a slice of the buffer.
	bufferLength int64
}

func NewIndexReader(file *os.File) *IndexedReader {
	return &IndexedReader{
		file:         file,
		offsetInFile: 0,
		buffer:       make([]byte, 10_000),
		bufferLength: 0,
	}
}

func (r *IndexedReader) read(at int64, length int) ([]byte, error) {
	// TODO handle length > buffer size
	// TODO handle other overlap situations (i.e. at+length < buffer start etc.)
	bufferEnd := r.offsetInFile + r.bufferLength
	// If requested data is (partially) outside buffer -> refetch data
	if at+int64(length) >= bufferEnd || at > bufferEnd {
		// Reset buffer to not contain outdated data
		for i := 0; i < len(r.buffer); i++ {
			r.buffer[i] = 0
		}

		// Actually read data from
		readBytes, err := r.file.ReadAt(r.buffer, at)

		// Handle errors. EOF-errors are okay, since this only means that the buffer is not full due to an end of the
		//input, which is not an actual error.
		if err != nil && err != io.EOF {
			return nil, errors.Wrapf(err, "Error fetching data from reader starting at index %d", at)
		}

		// Handle case when nothing was read
		if err != io.EOF && readBytes == 0 {
			return nil, errors.Errorf("No bytes were read starting at index %d with returned error %+v", at, err)
		}

		r.offsetInFile = at
		r.bufferLength = int64(readBytes)
	}

	return r.buffer[at-r.offsetInFile : at+int64(length)-r.offsetInFile], nil
}

func (r *IndexedReader) Read(at int64, length int) []byte {
	data, err := r.read(at, length)
	sigolo.FatalCheck(err)
	return data
}

func (r *IndexedReader) Has(at int64) bool {
	_, err := r.read(at, 1)
	return err == nil && at < r.offsetInFile+r.bufferLength
}

func (r *IndexedReader) Uint64(at int64) uint64 {
	data, err := r.read(at, 8)
	sigolo.FatalCheck(err)
	return binary.LittleEndian.Uint64(data)
}

func (r *IndexedReader) Uint32(at int64) uint32 {
	data, err := r.read(at, 4)
	sigolo.FatalCheck(err)
	return binary.LittleEndian.Uint32(data)
}

func (r *IndexedReader) Uint16(at int64) uint16 {
	data, err := r.read(at, 2)
	sigolo.FatalCheck(err)
	return binary.LittleEndian.Uint16(data)
}

// IntFromUint32 returns a 32-bit long integer from a uint32 value. It's basically a type cast on most systems.
func (r *IndexedReader) IntFromUint32(at int64) int {
	return int(r.Uint32(at))
}

// IntFromUint24 returns a 32-bit long integer from a 24-bit long part of the reader.
func (r *IndexedReader) IntFromUint24(at int64) int {
	// Remove first byte since in little endian reading, this byte belongs to the next value of the reader and should therefore be skipped.
	return int(r.Uint32(at)) & 0x00FFFFFF
}

// IntFromUint16 returns a 32-bit long integer from a uint16 value. It means, this methods reads 2 bytes and then casts it to a normal int value.
func (r *IndexedReader) IntFromUint16(at int64) int {
	return int(r.Uint16(at))
}

func (r *IndexedReader) Int64(at int64) int64 {
	return int64(r.Uint64(at))
}

func (r *IndexedReader) Float32(at int64) float32 {
	return math.Float32frombits(r.Uint32(at))
}
