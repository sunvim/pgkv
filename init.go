package pgkv

import "github.com/alitto/pond/v2"

var (
	workers = pond.NewPool(32)
)

const (
	DefaultShardCount   = 256
	BatchSize           = 1024
	TTLCleanupBatchSize = 10000
)
