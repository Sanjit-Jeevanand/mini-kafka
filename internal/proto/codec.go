package proto

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

func WriteFrame(conn net.Conn, msgType uint16, payload []byte) error {
	buf := make([]byte, HeaderSize+len(payload))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint16(buf[4:6], VersionV1)
	binary.BigEndian.PutUint16(buf[6:8], msgType)
	copy(buf[8:], payload)
	_, err := conn.Write(buf)
	return err
}

func ReadFrame(conn net.Conn) (msgType uint16, payload []byte, err error) {
	var hdr [HeaderSize]byte
	if _, err = io.ReadFull(conn, hdr[:]); err != nil {
		return 0, nil, err
	}

	length := binary.BigEndian.Uint32(hdr[0:4])
	version := binary.BigEndian.Uint16(hdr[4:6])
	msgType = binary.BigEndian.Uint16(hdr[6:8])

	if length > MaxFrameSize {
		return 0, nil, ErrFrameTooLarge
	}
	if version != VersionV1 {
		return 0, nil, ErrBadVersion
	}

	payload = make([]byte, length)
	if _, err = io.ReadFull(conn, payload); err != nil {
		return 0, nil, err
	}
	return msgType, payload, nil
}

// Layout:
//   [2 topic_len][topic bytes]
//   [4 record_count]
//   for each record:
//     [4 key_len][key bytes][4 val_len][val bytes]
func EncodeProduceRequest(r ProduceRequest) []byte {
	size := 2 + len(r.Topic) + 4
	for _, rec := range r.Records {
		size += 4 + len(rec.Key) + 4 + len(rec.Value)
	}
	buf := make([]byte, size)
	off := 0
	off += putString(buf[off:], r.Topic)
	binary.BigEndian.PutUint32(buf[off:], uint32(len(r.Records)))
	off += 4
	for _, rec := range r.Records {
		off += putBytes(buf[off:], rec.Key)
		off += putBytes(buf[off:], rec.Value)
	}
	return buf
}

func DecodeProduceRequest(b []byte) (ProduceRequest, error) {
	var r ProduceRequest
	off := 0
	topic, n, err := getString(b[off:])
	if err != nil {
		return r, err
	}
	r.Topic = topic
	off += n

	if len(b[off:]) < 4 {
		return r, fmt.Errorf("proto: truncated record count")
	}
	count := int(binary.BigEndian.Uint32(b[off:]))
	off += 4

	r.Records = make([]ProduceRecord, 0, count)
	for i := 0; i < count; i++ {
		key, n, err := getBytes(b[off:])
		if err != nil {
			return r, err
		}
		off += n
		val, n, err := getBytes(b[off:])
		if err != nil {
			return r, err
		}
		off += n
		r.Records = append(r.Records, ProduceRecord{Key: key, Value: val})
	}
	return r, nil
}

// Layout: [8 base_offset][2 err_len][err bytes]
func EncodeProduceResponse(r ProduceResponse) []byte {
	buf := make([]byte, 8+2+len(r.Err))
	binary.BigEndian.PutUint64(buf[0:8], r.BaseOffset)
	putString(buf[8:], r.Err)
	return buf
}

func DecodeProduceResponse(b []byte) (ProduceResponse, error) {
	if len(b) < 10 {
		return ProduceResponse{}, fmt.Errorf("proto: truncated produce response")
	}
	var r ProduceResponse
	r.BaseOffset = binary.BigEndian.Uint64(b[0:8])
	s, _, err := getString(b[8:])
	r.Err = s
	return r, err
}

// Layout: [2 topic_len][topic bytes][8 offset][4 max_bytes]
func EncodeFetchRequest(r FetchRequest) []byte {
	buf := make([]byte, 2+len(r.Topic)+8+4)
	off := 0
	off += putString(buf[off:], r.Topic)
	binary.BigEndian.PutUint64(buf[off:], r.Offset)
	off += 8
	binary.BigEndian.PutUint32(buf[off:], r.MaxBytes)
	return buf
}

func DecodeFetchRequest(b []byte) (FetchRequest, error) {
	var r FetchRequest
	topic, n, err := getString(b)
	if err != nil {
		return r, err
	}
	r.Topic = topic
	off := n
	if len(b[off:]) < 12 {
		return r, fmt.Errorf("proto: truncated fetch request")
	}
	r.Offset = binary.BigEndian.Uint64(b[off:])
	off += 8
	r.MaxBytes = binary.BigEndian.Uint32(b[off:])
	return r, nil
}

// Layout:
//   [8 next_offset][2 err_len][err bytes]
//   [4 record_count]
//   for each record:
//     [8 offset][8 timestamp][4 key_len][key bytes][4 val_len][val bytes]
func EncodeFetchResponse(r FetchResponse) []byte {
	size := 8 + 2 + len(r.Err) + 4
	for _, rec := range r.Records {
		size += 8 + 8 + 4 + len(rec.Key) + 4 + len(rec.Value)
	}
	buf := make([]byte, size)
	off := 0
	binary.BigEndian.PutUint64(buf[off:], r.NextOffset)
	off += 8
	off += putString(buf[off:], r.Err)
	binary.BigEndian.PutUint32(buf[off:], uint32(len(r.Records)))
	off += 4
	for _, rec := range r.Records {
		binary.BigEndian.PutUint64(buf[off:], rec.Offset)
		off += 8
		binary.BigEndian.PutUint64(buf[off:], uint64(rec.Timestamp))
		off += 8
		off += putBytes(buf[off:], rec.Key)
		off += putBytes(buf[off:], rec.Value)
	}
	return buf
}

func DecodeFetchResponse(b []byte) (FetchResponse, error) {
	var r FetchResponse
	if len(b) < 14 {
		return r, fmt.Errorf("proto: truncated fetch response")
	}
	r.NextOffset = binary.BigEndian.Uint64(b[0:8])
	errStr, n, err := getString(b[8:])
	if err != nil {
		return r, err
	}
	r.Err = errStr
	off := 8 + n

	if len(b[off:]) < 4 {
		return r, fmt.Errorf("proto: truncated record count")
	}
	count := int(binary.BigEndian.Uint32(b[off:]))
	off += 4

	r.Records = make([]FetchRecord, 0, count)
	for i := 0; i < count; i++ {
		if len(b[off:]) < 16 {
			return r, fmt.Errorf("proto: truncated fetch record header")
		}
		var rec FetchRecord
		rec.Offset = binary.BigEndian.Uint64(b[off:])
		off += 8
		rec.Timestamp = int64(binary.BigEndian.Uint64(b[off:]))
		off += 8
		key, n, err := getBytes(b[off:])
		if err != nil {
			return r, err
		}
		off += n
		val, n, err := getBytes(b[off:])
		if err != nil {
			return r, err
		}
		off += n
		rec.Key = key
		rec.Value = val
		r.Records = append(r.Records, rec)
	}
	return r, nil
}

// Layout: [2 topic_len][topic bytes]
func EncodeMetaRequest(r MetaRequest) []byte {
	buf := make([]byte, 2+len(r.Topic))
	putString(buf, r.Topic)
	return buf
}

func DecodeMetaRequest(b []byte) (MetaRequest, error) {
	topic, _, err := getString(b)
	return MetaRequest{Topic: topic}, err
}

// Layout: [2 topic_len][topic][2 addr_len][addr][4 partitions][2 err_len][err]
func EncodeMetaResponse(r MetaResponse) []byte {
	buf := make([]byte, 2+len(r.Topic)+2+len(r.Addr)+4+2+len(r.Err))
	off := 0
	off += putString(buf[off:], r.Topic)
	off += putString(buf[off:], r.Addr)
	binary.BigEndian.PutUint32(buf[off:], uint32(r.Partitions))
	off += 4
	putString(buf[off:], r.Err)
	return buf
}

func DecodeMetaResponse(b []byte) (MetaResponse, error) {
	var r MetaResponse
	off := 0
	topic, n, err := getString(b[off:])
	if err != nil {
		return r, err
	}
	r.Topic = topic
	off += n
	addr, n, err := getString(b[off:])
	if err != nil {
		return r, err
	}
	r.Addr = addr
	off += n
	if len(b[off:]) < 4 {
		return r, fmt.Errorf("proto: truncated meta response")
	}
	r.Partitions = int(binary.BigEndian.Uint32(b[off:]))
	off += 4
	errStr, _, err := getString(b[off:])
	r.Err = errStr
	return r, err
}

// ── helpers ───────────────────────────────────────────────────────────────────

// putString writes a 2-byte length-prefixed string into dst and returns bytes written.
func putString(dst []byte, s string) int {
	binary.BigEndian.PutUint16(dst[0:2], uint16(len(s)))
	copy(dst[2:], s)
	return 2 + len(s)
}

// getString reads a 2-byte length-prefixed string from src.
func getString(src []byte) (string, int, error) {
	if len(src) < 2 {
		return "", 0, fmt.Errorf("proto: buffer too short for string length")
	}
	l := int(binary.BigEndian.Uint16(src[0:2]))
	if len(src) < 2+l {
		return "", 0, fmt.Errorf("proto: buffer too short for string body")
	}
	return string(src[2 : 2+l]), 2 + l, nil
}

// putBytes writes a 4-byte length-prefixed byte slice into dst.
func putBytes(dst []byte, b []byte) int {
	binary.BigEndian.PutUint32(dst[0:4], uint32(len(b)))
	copy(dst[4:], b)
	return 4 + len(b)
}

// getBytes reads a 4-byte length-prefixed byte slice from src.
func getBytes(src []byte) ([]byte, int, error) {
	if len(src) < 4 {
		return nil, 0, fmt.Errorf("proto: buffer too short for bytes length")
	}
	l := int(binary.BigEndian.Uint32(src[0:4]))
	if len(src) < 4+l {
		return nil, 0, fmt.Errorf("proto: buffer too short for bytes body")
	}
	out := make([]byte, l)
	copy(out, src[4:4+l])
	return out, 4 + l, nil
}
