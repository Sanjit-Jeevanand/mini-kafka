package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"

	ilog "github.com/sanjit-jeevanand/mini-kafka/internal/log"
	"github.com/sanjit-jeevanand/mini-kafka/internal/proto"
	"github.com/sanjit-jeevanand/mini-kafka/internal/topic"
)

type Handler struct {
	topic *topic.Topic
	addr  string
}

func NewHandler(t *topic.Topic, addr string) *Handler {
	return &Handler{topic: t, addr: addr}
}

func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msgType, payload, err := proto.ReadFrame(conn)
		if err != nil {
			if err != io.EOF {
				slog.Debug("read frame error", "err", err)
			}
			return
		}

		var (
			respType uint16
			respData []byte
		)

		switch msgType {
		case proto.TypeProduceRequest:
			respType, respData = h.handleProduce(ctx, payload)
		case proto.TypeFetchRequest:
			respType, respData = h.handleFetch(ctx, payload)
		case proto.TypeMetaRequest:
			respType, respData = h.handleMeta(payload)
		default:
			slog.Warn("unknown message type", "type", msgType)
			return
		}

		if err := proto.WriteFrame(conn, respType, respData); err != nil {
			slog.Debug("write frame error", "err", err)
			return
		}
	}
}

func (h *Handler) handleProduce(ctx context.Context, payload []byte) (uint16, []byte) {
	req, err := proto.DecodeProduceRequest(payload)
	if err != nil {
		return proto.TypeProduceResponse, proto.EncodeProduceResponse(
			proto.ProduceResponse{Err: fmt.Sprintf("decode error: %v", err)},
		)
	}

	// Use the first record's key to pick the partition for the whole batch.
	// All records in one ProduceRequest land on the same partition.
	var key []byte
	if len(req.Records) > 0 {
		key = req.Records[0].Key
	}

	var baseOffset uint64
	var partition int
	for i, rec := range req.Records {
		p, offset, err := h.topic.Append(ctx, key, ilog.Record{
			Key:   rec.Key,
			Value: rec.Value,
		})
		if err != nil {
			return proto.TypeProduceResponse, proto.EncodeProduceResponse(
				proto.ProduceResponse{Err: fmt.Sprintf("append error: %v", err)},
			)
		}
		if i == 0 {
			baseOffset = offset
			partition = p
		}
	}

	return proto.TypeProduceResponse, proto.EncodeProduceResponse(
		proto.ProduceResponse{Partition: partition, BaseOffset: baseOffset},
	)
}

func (h *Handler) handleFetch(ctx context.Context, payload []byte) (uint16, []byte) {
	req, err := proto.DecodeFetchRequest(payload)
	if err != nil {
		return proto.TypeFetchResponse, proto.EncodeFetchResponse(
			proto.FetchResponse{Err: fmt.Sprintf("decode error: %v", err)},
		)
	}

	var records []proto.FetchRecord
	var bytesRead uint32
	offset := req.Offset

	for {
		if req.MaxBytes > 0 && bytesRead >= req.MaxBytes {
			break
		}
		rec, err := h.topic.Read(ctx, int(req.Partition), offset)
		if err != nil {
			break // no more records at this offset
		}
		records = append(records, proto.FetchRecord{
			Offset:    rec.Offset,
			Timestamp: rec.Timestamp,
			Key:       rec.Key,
			Value:     rec.Value,
		})
		bytesRead += uint32(len(rec.Key) + len(rec.Value))
		offset++
	}

	return proto.TypeFetchResponse, proto.EncodeFetchResponse(proto.FetchResponse{
		Records:    records,
		NextOffset: offset,
	})
}

func (h *Handler) handleMeta(payload []byte) (uint16, []byte) {
	req, err := proto.DecodeMetaRequest(payload)
	if err != nil {
		return proto.TypeMetaResponse, proto.EncodeMetaResponse(
			proto.MetaResponse{Err: fmt.Sprintf("decode error: %v", err)},
		)
	}

	return proto.TypeMetaResponse, proto.EncodeMetaResponse(proto.MetaResponse{
		Topic:      req.Topic,
		Addr:       h.addr,
		Partitions: h.topic.NumPartitions(),
	})
}
