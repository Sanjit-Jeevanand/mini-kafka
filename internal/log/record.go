package log

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

var ErrCorrupted = errors.New("log: record CRC mismatch — data corrupted")

// On-disk layout (all integers big-endian):
//	[8 offset][8 timestamp][4 key_len][key bytes][4 val_len][value bytes][4 CRC]
type Record struct {
	Offset    uint64
	Timestamp int64
	Key       []byte
	Value     []byte
}

// offset(8) + timestamp(8) + key_len(4) + val_len(4) = 24 bytes.
const headerSize = 24

var crcTable = crc32.MakeTable(crc32.IEEE)

// EncodedSize returns the exact number of bytes EncodeRecord will produce for r.
// Useful for pre-allocating write buffers without a separate encode call.
func EncodedSize(r Record) int {
	return headerSize + len(r.Key) + len(r.Value) + 4 // 4 = CRC
}

func EncodeRecord(dst []byte, r Record) []byte {
	size := EncodedSize(r)
	if cap(dst)-len(dst) < size {
		grown := make([]byte, len(dst), len(dst)+size)
		copy(grown, dst)
		dst = grown
	}
	start := len(dst)
	dst = dst[:start+size]
	b := dst[start:]

	binary.BigEndian.PutUint64(b[0:8], r.Offset)
	binary.BigEndian.PutUint64(b[8:16], uint64(r.Timestamp))
	binary.BigEndian.PutUint32(b[16:20], uint32(len(r.Key)))
	copy(b[20:20+len(r.Key)], r.Key)
	valOff := 20 + len(r.Key)
	binary.BigEndian.PutUint32(b[valOff:valOff+4], uint32(len(r.Value)))
	copy(b[valOff+4:valOff+4+len(r.Value)], r.Value)

	// CRC covers every byte before it.
	crcOff := valOff + 4 + len(r.Value)
	checksum := crc32.Checksum(b[:crcOff], crcTable)
	binary.BigEndian.PutUint32(b[crcOff:crcOff+4], checksum)

	return dst
}

func DecodeRecord(b []byte) (Record, int, error) {
	if len(b) < headerSize+4 { // need at least header + CRC
		return Record{}, 0, errors.New("log: buffer too short to contain a record")
	}

	offset := binary.BigEndian.Uint64(b[0:8])
	timestamp := int64(binary.BigEndian.Uint64(b[8:16]))
	keyLen := int(binary.BigEndian.Uint32(b[16:20]))

	if len(b) < 20+keyLen+4 {
		return Record{}, 0, errors.New("log: buffer truncated in key field")
	}
	key := b[20 : 20+keyLen]

	valOff := 20 + keyLen
	if len(b) < valOff+4 {
		return Record{}, 0, errors.New("log: buffer truncated before value length")
	}
	valLen := int(binary.BigEndian.Uint32(b[valOff : valOff+4]))

	crcOff := valOff + 4 + valLen
	if len(b) < crcOff+4 {
		return Record{}, 0, errors.New("log: buffer truncated in value field")
	}
	value := b[valOff+4 : crcOff]

	stored := binary.BigEndian.Uint32(b[crcOff : crcOff+4])
	computed := crc32.Checksum(b[:crcOff], crcTable)
	if stored != computed {
		return Record{}, 0, ErrCorrupted
	}

	r := Record{
		Offset:    offset,
		Timestamp: timestamp,
		Key:       make([]byte, keyLen),
		Value:     make([]byte, valLen),
	}
	copy(r.Key, key)
	copy(r.Value, value)

	return r, crcOff + 4, nil
}
