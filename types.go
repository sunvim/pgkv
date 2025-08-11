package pgkv

import (
	"time"

	"github.com/uptrace/bun"
)

type KVPair struct {
	bun.BaseModel `bun:"table:kv_store_shard_?,alias:kv"`

	Key       string     `bun:"key,pk" json:"key"`
	KeyHash   string     `bun:"key_hash,notnull" json:"-"` // 用于快速查找
	Value     string     `bun:"value,type:jsonb" json:"value"`
	CreatedAt time.Time  `bun:"created_at,nullzero,notnull,default:current_timestamp" json:"created_at"`
	UpdatedAt time.Time  `bun:"updated_at,nullzero,notnull,default:current_timestamp" json:"updated_at"`
	TTL       *time.Time `bun:"ttl" json:"ttl,omitempty"`
	Size      int        `bun:"size,notnull,default:0" json:"size"` // 存储值大小，用于统计
}

// KVMetrics 存储统计信息
type KVMetrics struct {
	bun.BaseModel `bun:"table:kv_metrics,alias:metrics"`

	ShardID     int       `bun:"shard_id,pk" json:"shard_id"`
	TotalKeys   int64     `bun:"total_keys,notnull,default:0" json:"total_keys"`
	TotalSize   int64     `bun:"total_size,notnull,default:0" json:"total_size"`
	ExpiredKeys int64     `bun:"expired_keys,notnull,default:0" json:"expired_keys"`
	LastCleanup time.Time `bun:"last_cleanup,nullzero,default:current_timestamp" json:"last_cleanup"`
	UpdatedAt   time.Time `bun:"updated_at,nullzero,notnull,default:current_timestamp" json:"updated_at"`
}
