package proto

import "errors"

// Wire frame layout:
//   [4 bytes: payload length][2 bytes: version][2 bytes: message type][payload bytes]

const (
	HeaderSize   = 8
	MaxFrameSize = 16 * 1024 * 1024 // 16 MiB

	VersionV1 uint16 = 1

	TypeProduceRequest  uint16 = 1
	TypeProduceResponse uint16 = 2
	TypeFetchRequest    uint16 = 3
	TypeFetchResponse   uint16 = 4
	TypeMetaRequest     uint16 = 5
	TypeMetaResponse    uint16 = 6
)

var (
	ErrFrameTooLarge = errors.New("proto: frame exceeds MaxFrameSize")
	ErrBadVersion    = errors.New("proto: unsupported frame version")
)

type FrameHeader struct {
	Length  uint32
	Version uint16
	Type    uint16
}
