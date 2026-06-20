package log

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type Segment struct {
	file       *os.File
	index      *Index
	baseOffset uint64
	nextOffset uint64
	size       uint32 // bytes written to the log file
	maxBytes   uint32
}

func openSegment(dir string, baseOffset uint64, maxBytes uint32) (*Segment, error) {
	logPath := filepath.Join(dir, fmt.Sprintf("%020d.log", baseOffset))
	f, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	idxPath := filepath.Join(dir, fmt.Sprintf("%020d.index", baseOffset))
	idx, err := OpenIndex(idxPath, baseOffset)
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	s := &Segment{
		file:       f,
		index:      idx,
		baseOffset: baseOffset,
		nextOffset: baseOffset,
		size:       uint32(info.Size()),
		maxBytes:   maxBytes,
	}
	return s, nil
}

func (s *Segment) Append(r Record) (uint64, error) {
	offset, err := s.appendNoSync(r)
	if err != nil {
		return 0, err
	}
	if err := s.file.Sync(); err != nil {
		return 0, err
	}
	return offset, nil
}

func (s *Segment) appendNoSync(r Record) (uint64, error) {
	r.Offset = s.nextOffset
	r.Timestamp = time.Now().UnixNano()

	var buf []byte
	buf = EncodeRecord(buf, r)

	pos := s.size
	if _, err := s.file.Write(buf); err != nil {
		return 0, err
	}

	if err := s.index.MaybeAppend(r.Offset, pos, uint32(len(buf))); err != nil {
		return 0, err
	}

	s.size += uint32(len(buf))
	offset := s.nextOffset
	s.nextOffset++
	return offset, nil
}

func (s *Segment) sync() error {
	return s.file.Sync()
}

func (s *Segment) Read(absoluteOffset uint64) (Record, error) {
	startPos, err := s.index.Lookup(absoluteOffset)
	if err != nil {
		return Record{}, err
	}

	// Read from startPos to end of file, then scan forward.
	remaining := int64(s.size) - int64(startPos)
	if remaining <= 0 {
		return Record{}, fmt.Errorf("segment: offset %d not found", absoluteOffset)
	}

	buf := make([]byte, remaining)
	if _, err := s.file.ReadAt(buf, int64(startPos)); err != nil {
		return Record{}, err
	}

	var consumed int
	for consumed < len(buf) {
		rec, n, err := DecodeRecord(buf[consumed:])
		if err != nil {
			return Record{}, err
		}
		if rec.Offset == absoluteOffset {
			return rec, nil
		}
		consumed += n
	}
	return Record{}, fmt.Errorf("segment: offset %d not found in segment starting at %d", absoluteOffset, s.baseOffset)
}

func (s *Segment) IsFull() bool {
	return s.size >= s.maxBytes
}

func (s *Segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	return s.file.Close()
}

func (s *Segment) Truncate(fromPosition uint32) error {
	if err := s.file.Truncate(int64(fromPosition)); err != nil {
		return err
	}
	if err := s.index.Truncate(fromPosition); err != nil {
		return err
	}
	s.size = fromPosition
	return nil
}
