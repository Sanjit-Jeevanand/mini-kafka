package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"

	ilog "github.com/sanjit-jeevanand/mini-kafka/internal/log"
	"github.com/sanjit-jeevanand/mini-kafka/internal/proto"
)

type Handler struct {
	log  *ilog.Log
	addr string
}

func NewHandler(l *ilog.Log, addr string) *Handler {
	return &Handler{log: l, addr: addr}
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

	var baseOffset uint64
	for i, rec := range req.Records {
		offset, err := h.log.Append(ctx, ilog.Record{
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
		}
	}

	return proto.TypeProduceResponse, proto.EncodeProduceResponse(
		proto.ProduceResponse{BaseOffset: baseOffset},
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
		rec, err := h.log.Read(ctx, offset)
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
		Partitions: 1,
	})
}
