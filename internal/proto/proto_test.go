package proto

import (
	"net"
	"testing"
)

// ── round-trip tests ──────────────────────────────────────────────────────────

func TestProduceRequestRoundTrip(t *testing.T) {
	want := ProduceRequest{
		Topic: "orders",
		Records: []ProduceRecord{
			{Key: []byte("k1"), Value: []byte("v1")},
			{Key: []byte("k2"), Value: []byte("v2")},
			{Key: nil, Value: []byte("no-key")},
		},
	}
	got, err := DecodeProduceRequest(EncodeProduceRequest(want))
	if err != nil {
		t.Fatal(err)
	}
	if got.Topic != want.Topic {
		t.Fatalf("topic: got %q want %q", got.Topic, want.Topic)
	}
	if len(got.Records) != len(want.Records) {
		t.Fatalf("record count: got %d want %d", len(got.Records), len(want.Records))
	}
	for i, r := range got.Records {
		if string(r.Key) != string(want.Records[i].Key) {
			t.Fatalf("record[%d] key: got %q want %q", i, r.Key, want.Records[i].Key)
		}
		if string(r.Value) != string(want.Records[i].Value) {
			t.Fatalf("record[%d] value: got %q want %q", i, r.Value, want.Records[i].Value)
		}
	}
}

func TestProduceResponseRoundTrip(t *testing.T) {
	cases := []ProduceResponse{
		{BaseOffset: 0, Err: ""},
		{BaseOffset: 42, Err: ""},
		{BaseOffset: 0, Err: "log full"},
	}
	for _, want := range cases {
		got, err := DecodeProduceResponse(EncodeProduceResponse(want))
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("got %+v want %+v", got, want)
		}
	}
}

func TestFetchRequestRoundTrip(t *testing.T) {
	want := FetchRequest{Topic: "events", Offset: 1000, MaxBytes: 65536}
	got, err := DecodeFetchRequest(EncodeFetchRequest(want))
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("got %+v want %+v", got, want)
	}
}

func TestFetchResponseRoundTrip(t *testing.T) {
	want := FetchResponse{
		NextOffset: 5,
		Err:        "",
		Records: []FetchRecord{
			{Offset: 3, Timestamp: 1234567890, Key: []byte("k"), Value: []byte("v")},
			{Offset: 4, Timestamp: 9999999999, Key: nil, Value: []byte("no-key")},
		},
	}
	got, err := DecodeFetchResponse(EncodeFetchResponse(want))
	if err != nil {
		t.Fatal(err)
	}
	if got.NextOffset != want.NextOffset || got.Err != want.Err {
		t.Fatalf("header mismatch: got %+v want %+v", got, want)
	}
	if len(got.Records) != len(want.Records) {
		t.Fatalf("record count: got %d want %d", len(got.Records), len(want.Records))
	}
	for i, r := range got.Records {
		w := want.Records[i]
		if r.Offset != w.Offset || r.Timestamp != w.Timestamp ||
			string(r.Key) != string(w.Key) || string(r.Value) != string(w.Value) {
			t.Fatalf("record[%d]: got %+v want %+v", i, r, w)
		}
	}
}

func TestMetaRoundTrip(t *testing.T) {
	req := MetaRequest{Topic: "payments"}
	gotReq, err := DecodeMetaRequest(EncodeMetaRequest(req))
	if err != nil || gotReq != req {
		t.Fatalf("meta request: err=%v got=%+v", err, gotReq)
	}

	resp := MetaResponse{Topic: "payments", Addr: "127.0.0.1:9092", Partitions: 1, Err: ""}
	gotResp, err := DecodeMetaResponse(EncodeMetaResponse(resp))
	if err != nil || gotResp != resp {
		t.Fatalf("meta response: err=%v got=%+v", err, gotResp)
	}
}

// ── frame tests ───────────────────────────────────────────────────────────────

func TestFrameRoundTrip(t *testing.T) {
	server, client := net.Pipe() // synchronous in-memory connection
	defer server.Close()
	defer client.Close()

	payload := []byte("hello broker")

	go func() {
		if err := WriteFrame(client, TypeProduceRequest, payload); err != nil {
			t.Errorf("WriteFrame: %v", err)
		}
	}()

	msgType, got, err := ReadFrame(server)
	if err != nil {
		t.Fatal(err)
	}
	if msgType != TypeProduceRequest {
		t.Fatalf("msgType: got %d want %d", msgType, TypeProduceRequest)
	}
	if string(got) != string(payload) {
		t.Fatalf("payload: got %q want %q", got, payload)
	}
}

func TestFrameTooLarge(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	go func() {
		// Manually write a header claiming MaxFrameSize+1 bytes.
		hdr := make([]byte, HeaderSize)
		hdr[0] = 0xFF
		hdr[1] = 0xFF
		hdr[2] = 0xFF
		hdr[3] = 0xFF // length = 4294967295 — way over MaxFrameSize
		hdr[4] = 0x00
		hdr[5] = 0x01 // version = 1
		hdr[6] = 0x00
		hdr[7] = 0x01 // type = TypeProduceRequest
		client.Write(hdr)
	}()

	_, _, err := ReadFrame(server)
	if err != ErrFrameTooLarge {
		t.Fatalf("expected ErrFrameTooLarge, got %v", err)
	}
}

func TestFrameBadVersion(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	go func() {
		hdr := make([]byte, HeaderSize)
		// length = 0, version = 99 (unknown), type = 1
		hdr[4] = 0x00
		hdr[5] = 0x63 // version 99
		hdr[6] = 0x00
		hdr[7] = 0x01
		client.Write(hdr)
	}()

	_, _, err := ReadFrame(server)
	if err != ErrBadVersion {
		t.Fatalf("expected ErrBadVersion, got %v", err)
	}
}

// ── fuzz test ─────────────────────────────────────────────────────────────────

// FuzzReadFrame feeds arbitrary bytes as a frame header+payload.
// The decoder must never panic — it may return any error, but must not crash.
//
// Run with: go test -fuzz=FuzzReadFrame -fuzztime=30s
func FuzzReadFrame(f *testing.F) {
	// Seed with a valid frame.
	valid := make([]byte, HeaderSize+4)
	valid[0], valid[1], valid[2], valid[3] = 0, 0, 0, 4 // length=4
	valid[4], valid[5] = 0, 1                            // version=1
	valid[6], valid[7] = 0, 1                            // type=1
	valid[8], valid[9], valid[10], valid[11] = 'f', 'u', 'z', 'z'
	f.Add(valid)

	f.Fuzz(func(t *testing.T, data []byte) {
		server, client := net.Pipe()
		defer server.Close()

		go func() {
			client.Write(data)
			client.Close()
		}()

		// Must not panic — any error is acceptable.
		ReadFrame(server) //nolint:errcheck
	})
}
