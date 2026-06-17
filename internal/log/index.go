package log

import (
	"encoding/binary"
	"errors"
	"os"
)

const (
	// [4 bytes relativeOffset][4 bytes filePosition] = 8 bytes.
	entryWidth = 8
	indexStride = 4096
)

type Index struct {
	file       *os.File
	baseOffset uint64
	size       int
	entries    []indexEntry
}

type indexEntry struct {
	relOffset uint32
	position  uint32
}

func OpenIndex(path string, baseOffset uint64) (*Index, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	idx := &Index{file: f, baseOffset: baseOffset}
	if err := idx.load(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return idx, nil
}

func (idx *Index) load() error {
	info, err := idx.file.Stat()
	if err != nil {
		return err
	}
	n := int(info.Size()) / entryWidth
	idx.entries = make([]indexEntry, 0, n)

	buf := make([]byte, entryWidth)
	for i := 0; i < n; i++ {
		if _, err := idx.file.Read(buf); err != nil {
			return err
		}
		idx.entries = append(idx.entries, indexEntry{
			relOffset: binary.BigEndian.Uint32(buf[0:4]),
			position:  binary.BigEndian.Uint32(buf[4:8]),
		})
	}
	idx.size = len(idx.entries)
	return nil
}

func (idx *Index) MaybeAppend(absoluteOffset uint64, filePosition uint32, bytesWritten uint32) error {
	lastPos := uint32(0)
	if len(idx.entries) > 0 {
		lastPos = idx.entries[len(idx.entries)-1].position
	}
	if len(idx.entries) > 0 && filePosition-lastPos < indexStride {
		return nil
	}
	return idx.append(absoluteOffset, filePosition)
}

func (idx *Index) append(absoluteOffset uint64, filePosition uint32) error {
	rel := uint32(absoluteOffset - idx.baseOffset)
	e := indexEntry{relOffset: rel, position: filePosition}

	var buf [entryWidth]byte
	binary.BigEndian.PutUint32(buf[0:4], rel)
	binary.BigEndian.PutUint32(buf[4:8], filePosition)
	if _, err := idx.file.Write(buf[:]); err != nil {
		return err
	}
	idx.entries = append(idx.entries, e)
	idx.size++
	return nil
}

func (idx *Index) Lookup(absoluteOffset uint64) (filePosition uint32, err error) {
	if len(idx.entries) == 0 {
		return 0, nil
	}
	if absoluteOffset < idx.baseOffset {
		return 0, errors.New("index: offset predates this segment's base offset")
	}
	rel := uint32(absoluteOffset - idx.baseOffset)

	lo, hi := 0, len(idx.entries)-1
	for lo < hi {
		mid := (lo + hi + 1) / 2
		if idx.entries[mid].relOffset <= rel {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	return idx.entries[lo].position, nil
}

func (idx *Index) Close() error {
	if err := idx.file.Sync(); err != nil {
		return err
	}
	return idx.file.Close()
}

func (idx *Index) Truncate(fromPosition uint32) error {
	keep := 0
	for _, e := range idx.entries {
		if e.position < fromPosition {
			keep++
		}
	}
	idx.entries = idx.entries[:keep]
	idx.size = keep
	// Rewrite the file to match.
	if err := idx.file.Truncate(int64(keep) * entryWidth); err != nil {
		return err
	}
	return nil
}
