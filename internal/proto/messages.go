package proto

type ProduceRequest struct {
	Topic  string
	Records []ProduceRecord
}

type ProduceRecord struct {
	Key   []byte
	Value []byte
}

type ProduceResponse struct {
	Partition  int
	BaseOffset uint64
	Err        string // non-empty if the broker rejected the batch
}

type FetchRequest struct {
	Topic     string
	Partition int32
	Offset    uint64
	MaxBytes  uint32
}

type FetchResponse struct {
	Records    []FetchRecord
	NextOffset uint64
	Err        string
}

type FetchRecord struct {
	Offset    uint64
	Timestamp int64
	Key       []byte
	Value     []byte
}

type MetaRequest struct {
	Topic string
}

type MetaResponse struct {
	Topic      string
	Addr       string
	Partitions int
	Err        string
}
