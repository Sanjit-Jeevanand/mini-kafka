package log

import (
	"io"
	"os"
)

func Recover(f *os.File, idx *Index, baseOffset uint64) (nextOffset uint64, err error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}

	data, err := io.ReadAll(f)
	if err != nil {
		return 0, err
	}

	var (
		pos        int
		goodPos    int
		lastOffset = baseOffset - 1
		seenAny    bool
	)

	for pos < len(data) {
		rec, n, decErr := DecodeRecord(data[pos:])
		if decErr != nil {
			break // CRC mismatch or truncated record — stop here
		}
		goodPos = pos + n
		lastOffset = rec.Offset
		seenAny = true
		pos += n
	}

	if pos < len(data) {
		if err := f.Truncate(int64(goodPos)); err != nil {
			return 0, err
		}
		if err := idx.Truncate(uint32(goodPos)); err != nil {
			return 0, err
		}
	}

	if !seenAny {
		return baseOffset, nil
	}
	return lastOffset + 1, nil
}
