package pgkv

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/bytedance/sonic"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"
	"github.com/uptrace/bun/extra/bundebug"
)

type ShardedKVStore struct {
	db              *bun.DB
	shardCount      int
	getStmts        map[int]*sql.Stmt
	setStmts        map[int]*sql.Stmt
	deleteStmts     map[int]*sql.Stmt
	shardTableNames []string
}

func NewShardedKVStore(dsn string, shardCount int) (*ShardedKVStore, error) {
	if shardCount <= 0 {
		shardCount = DefaultShardCount
	}

	connector := pgdriver.NewConnector(
		pgdriver.WithDSN(dsn),
		pgdriver.WithReadTimeout(30*time.Second),
		pgdriver.WithWriteTimeout(30*time.Second),
	)

	sqldb := sql.OpenDB(connector)
	sqldb.SetMaxOpenConns(100)
	sqldb.SetMaxIdleConns(20)
	sqldb.SetConnMaxLifetime(5 * time.Minute)
	sqldb.SetConnMaxIdleTime(1 * time.Minute)

	db := bun.NewDB(sqldb, pgdialect.New())

	if false {
		db.AddQueryHook(bundebug.NewQueryHook(
			bundebug.WithVerbose(false),
		))
	}

	shardTableNames := make([]string, shardCount)
	for i := 0; i < shardCount; i++ {
		shardTableNames[i] = "kv_store_shard_" + strconv.Itoa(i)
	}

	store := &ShardedKVStore{
		db:              db,
		shardCount:      shardCount,
		getStmts:        make(map[int]*sql.Stmt),
		setStmts:        make(map[int]*sql.Stmt),
		deleteStmts:     make(map[int]*sql.Stmt),
		shardTableNames: shardTableNames,
	}

	if err := store.initDatabase(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	return store, nil
}

func (s *ShardedKVStore) hashKey(key string) (int, string) {
	h := md5.Sum([]byte(key))
	hashStr := hex.EncodeToString(h[:])
	shardID := int(h[0]) % s.shardCount
	return shardID, hashStr
}

func (s *ShardedKVStore) getShardTableName(shardID int) string {
	return s.shardTableNames[shardID]
}

func (s *ShardedKVStore) initDatabase(ctx context.Context) error {
	_, err := s.db.NewCreateTable().
		Model((*KVMetrics)(nil)).
		IfNotExists().
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to create metrics table: %w", err)
	}

	for i := 0; i < s.shardCount; i++ {
		if err := s.createShardTable(ctx, i); err != nil {
			return fmt.Errorf("failed to create shard table %d: %w", i, err)
		}
	}

	return s.initMetrics(ctx)
}

func (s *ShardedKVStore) createShardTable(ctx context.Context, shardID int) error {
	tableName := s.getShardTableName(shardID)

	var buf strings.Builder
	buf.WriteString("CREATE TABLE IF NOT EXISTS ")
	buf.WriteString(tableName)
	buf.WriteString(` (
		key TEXT PRIMARY KEY,
		key_hash CHAR(32) NOT NULL,
		value JSONB NOT NULL,
		created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
		ttl TIMESTAMP WITH TIME ZONE,
		size INTEGER DEFAULT 0
	)`)

	_, err := s.db.ExecContext(ctx, buf.String())
	if err != nil {
		return err
	}

	indexes := make([]string, 0, 5)

	var indexBuf strings.Builder

	indexBuf.Reset()
	indexBuf.WriteString("CREATE INDEX IF NOT EXISTS idx_")
	indexBuf.WriteString(tableName)
	indexBuf.WriteString("_key_hash ON ")
	indexBuf.WriteString(tableName)
	indexBuf.WriteString(" USING hash(key_hash)")
	indexes = append(indexes, indexBuf.String())

	indexBuf.Reset()
	indexBuf.WriteString("CREATE INDEX IF NOT EXISTS idx_")
	indexBuf.WriteString(tableName)
	indexBuf.WriteString("_ttl ON ")
	indexBuf.WriteString(tableName)
	indexBuf.WriteString(" (ttl) WHERE ttl IS NOT NULL")
	indexes = append(indexes, indexBuf.String())

	indexBuf.Reset()
	indexBuf.WriteString("CREATE INDEX IF NOT EXISTS idx_")
	indexBuf.WriteString(tableName)
	indexBuf.WriteString("_key_ttl ON ")
	indexBuf.WriteString(tableName)
	indexBuf.WriteString(" (key_hash, ttl) WHERE ttl IS NOT NULL")
	indexes = append(indexes, indexBuf.String())

	indexBuf.Reset()
	indexBuf.WriteString("CREATE INDEX IF NOT EXISTS idx_")
	indexBuf.WriteString(tableName)
	indexBuf.WriteString("_created_at ON ")
	indexBuf.WriteString(tableName)
	indexBuf.WriteString(" (created_at)")
	indexes = append(indexes, indexBuf.String())

	indexBuf.Reset()
	indexBuf.WriteString("CREATE INDEX IF NOT EXISTS idx_")
	indexBuf.WriteString(tableName)
	indexBuf.WriteString("_size ON ")
	indexBuf.WriteString(tableName)
	indexBuf.WriteString(" (size) WHERE size > 0")
	indexes = append(indexes, indexBuf.String())

	for _, indexSQL := range indexes {
		if _, err := s.db.ExecContext(ctx, indexSQL); err != nil {
			return fmt.Errorf("failed to create index: %w", err)
		}
	}

	var triggerBuf strings.Builder
	triggerBuf.WriteString("CREATE OR REPLACE FUNCTION update_")
	triggerBuf.WriteString(tableName)
	triggerBuf.WriteString(`_updated_at()
		RETURNS TRIGGER AS $$
		BEGIN
			NEW.updated_at = CURRENT_TIMESTAMP;
			RETURN NEW;
		END;
		$$ language 'plpgsql';

		DROP TRIGGER IF EXISTS trigger_`)
	triggerBuf.WriteString(tableName)
	triggerBuf.WriteString("_updated_at ON ")
	triggerBuf.WriteString(tableName)
	triggerBuf.WriteString(`;
		CREATE TRIGGER trigger_`)
	triggerBuf.WriteString(tableName)
	triggerBuf.WriteString("_updated_at BEFORE UPDATE ON ")
	triggerBuf.WriteString(tableName)
	triggerBuf.WriteString(" FOR EACH ROW EXECUTE FUNCTION update_")
	triggerBuf.WriteString(tableName)
	triggerBuf.WriteString("_updated_at();")

	_, err = s.db.ExecContext(ctx, triggerBuf.String())

	return err
}

func (s *ShardedKVStore) initMetrics(ctx context.Context) error {
	for i := 0; i < s.shardCount; i++ {
		_, err := s.db.NewInsert().
			Model(&KVMetrics{ShardID: i}).
			On("CONFLICT (shard_id) DO NOTHING").
			Exec(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *ShardedKVStore) Set(ctx context.Context, key string, value interface{}) error {
	return s.SetWithTTL(ctx, key, value, 0)
}

func (s *ShardedKVStore) SetWithTTL(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	shardID, keyHash := s.hashKey(key)
	tableName := s.getShardTableName(shardID)

	jsonValue, err := sonic.MarshalString(value)
	if err != nil {
		return fmt.Errorf("failed to marshal value: %w", err)
	}

	var ttlTime *time.Time
	if ttl > 0 {
		t := time.Now().Add(ttl)
		ttlTime = &t
	}

	var query string
	var args []interface{}

	if ttlTime != nil {
		query = "INSERT INTO " + tableName +
			" (key, key_hash, value, size, ttl) VALUES (?, ?, ?, ?, ?)" +
			" ON CONFLICT (key) DO UPDATE SET" +
			" value = EXCLUDED.value," +
			" size = EXCLUDED.size," +
			" ttl = EXCLUDED.ttl," +
			" updated_at = CURRENT_TIMESTAMP"
		args = []any{key, keyHash, jsonValue, len(jsonValue), ttlTime}
	} else {
		query = "INSERT INTO " + tableName +
			" (key, key_hash, value, size) VALUES (?, ?, ?, ?)" +
			" ON CONFLICT (key) DO UPDATE SET" +
			" value = EXCLUDED.value," +
			" size = EXCLUDED.size," +
			" ttl = NULL," +
			" updated_at = CURRENT_TIMESTAMP"
		args = []any{key, keyHash, jsonValue, len(jsonValue)}
	}

	_, err = s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to execute SetWithTTL query: table %s: %w", tableName, err)
	}

	workers.Submit(func() {
		s.updateMetrics(context.Background(), shardID)
	})

	return nil
}

func (s *ShardedKVStore) Get(ctx context.Context, key string, dest interface{}) error {
	shardID, keyHash := s.hashKey(key)
	tableName := s.getShardTableName(shardID)

	var value string
	query := "SELECT value FROM " + tableName +
		" WHERE key_hash = ? AND key = ? AND (ttl IS NULL OR ttl > CURRENT_TIMESTAMP)"

	err := s.db.QueryRowContext(ctx, query, keyHash, key).Scan(&value)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("key not found: %s", key)
		}
		return err
	}

	return sonic.UnmarshalString(value, dest)
}

func (s *ShardedKVStore) GetRaw(ctx context.Context, key string) (json.RawMessage, error) {
	shardID, keyHash := s.hashKey(key)
	tableName := s.getShardTableName(shardID)

	var value json.RawMessage
	query := "SELECT value FROM " + tableName +
		" WHERE key_hash = ? AND key = ? AND (ttl IS NULL OR ttl > CURRENT_TIMESTAMP)"

	err := s.db.QueryRowContext(ctx, query, keyHash, key).Scan(&value)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("key not found: %s", key)
		}
		return nil, err
	}

	return value, nil
}

func (s *ShardedKVStore) Exists(ctx context.Context, key string) (bool, error) {
	shardID, keyHash := s.hashKey(key)
	tableName := s.getShardTableName(shardID)

	var exists bool
	query := "SELECT EXISTS(SELECT 1 FROM " + tableName +
		" WHERE key_hash = ? AND key = ? AND (ttl IS NULL OR ttl > CURRENT_TIMESTAMP))"

	err := s.db.QueryRowContext(ctx, query, keyHash, key).Scan(&exists)
	return exists, err
}

func (s *ShardedKVStore) Delete(ctx context.Context, key string) error {
	shardID, keyHash := s.hashKey(key)
	tableName := s.getShardTableName(shardID)

	query := "DELETE FROM " + tableName + " WHERE key_hash = ? AND key = ?"
	result, err := s.db.ExecContext(ctx, query, keyHash, key)
	if err != nil {
		return err
	}

	affected, _ := result.RowsAffected()
	if affected == 0 {
		return fmt.Errorf("key not found: %s", key)
	}

	workers.Submit(func() {
		s.updateMetrics(context.Background(), shardID)
	})

	return nil
}

type batchItem struct {
	key     string
	keyHash string
	value   json.RawMessage
	size    int
}

func (s *ShardedKVStore) BatchSet(ctx context.Context, kvPairs map[string]interface{}) error {
	shardBatches := make(map[int][]batchItem)

	for key, value := range kvPairs {
		shardID, keyHash := s.hashKey(key)
		jsonValue, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("failed to marshal value for key %s: %w", key, err)
		}

		shardBatches[shardID] = append(shardBatches[shardID], batchItem{
			key:     key,
			keyHash: keyHash,
			value:   jsonValue,
			size:    len(jsonValue),
		})
	}

	errCh := make(chan error, len(shardBatches))

	for shardID, items := range shardBatches {
		go func(sID int, batch []batchItem) {
			errCh <- s.batchSetShard(ctx, sID, batch)
		}(shardID, items)
	}

	for i := 0; i < len(shardBatches); i++ {
		if err := <-errCh; err != nil {
			return err
		}
	}

	return nil
}

func (s *ShardedKVStore) batchSetShard(ctx context.Context, shardID int, items []batchItem) error {
	tableName := s.getShardTableName(shardID)

	for i := 0; i < len(items); i += BatchSize {
		end := i + BatchSize
		if end > len(items) {
			end = len(items)
		}

		batch := items[i:end]
		if err := s.executeBatch(ctx, tableName, batch); err != nil {
			return err
		}
	}

	workers.Submit(func() {
		s.updateMetrics(context.Background(), shardID)
	})

	return nil
}

func (s *ShardedKVStore) executeBatchGet(ctx context.Context, tableName string, keyPairs []keyHashPair) (map[string]json.RawMessage, error) {
	if len(keyPairs) == 0 {
		return make(map[string]json.RawMessage), nil
	}

	keyHashes := make([]string, len(keyPairs))
	keyMap := make(map[string]string)

	for i, pair := range keyPairs {
		keyHashes[i] = pair.keyHash
		keyMap[pair.keyHash] = pair.key
	}

	query := "SELECT key, key_hash, value FROM " + tableName +
		" WHERE key_hash IN(?) AND (ttl IS NULL OR ttl > CURRENT_TIMESTAMP)"

	rows, err := s.db.QueryContext(ctx, query, bun.In(keyHashes))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]json.RawMessage)
	for rows.Next() {
		var key, keyHash string
		var value json.RawMessage

		if err := rows.Scan(&key, &keyHash, &value); err != nil {
			return nil, err
		}

		result[key] = value
	}

	return result, rows.Err()
}

type keyValuePair struct {
	key       string
	value     json.RawMessage
	createdAt time.Time
}

func (s *ShardedKVStore) GetByPrefix(ctx context.Context, prefix string, limit int, offset int) (map[string]json.RawMessage, error) {
	if limit <= 0 {
		limit = 1000
	}

	resultCh := make(chan map[string]json.RawMessage, s.shardCount)
	errorCh := make(chan error, s.shardCount)

	shardLimit := limit + offset

	for i := 0; i < s.shardCount; i++ {
		go func(shardID int) {
			result, err := s.getPrefixFromShard(ctx, shardID, prefix, shardLimit)
			resultCh <- result
			errorCh <- err
		}(i)
	}

	var allResults []keyValuePair

	for i := 0; i < s.shardCount; i++ {
		if err := <-errorCh; err != nil {
			return nil, err
		}

		shardResult := <-resultCh
		for k, v := range shardResult {
			allResults = append(allResults, keyValuePair{
				key:   k,
				value: v,
			})
		}
	}

	for i := 0; i < len(allResults)-1; i++ {
		for j := i + 1; j < len(allResults); j++ {
			if allResults[i].key > allResults[j].key {
				allResults[i], allResults[j] = allResults[j], allResults[i]
			}
		}
	}

	start := offset
	if start >= len(allResults) {
		return make(map[string]json.RawMessage), nil
	}

	end := start + limit
	if end > len(allResults) {
		end = len(allResults)
	}

	result := make(map[string]json.RawMessage)
	for i := start; i < end; i++ {
		result[allResults[i].key] = allResults[i].value
	}

	return result, nil
}

func (s *ShardedKVStore) getPrefixFromShard(ctx context.Context, shardID int, prefix string, limit int) (map[string]json.RawMessage, error) {
	tableName := s.getShardTableName(shardID)

	var queryBuf strings.Builder
	queryBuf.WriteString("SELECT key, value FROM ")
	queryBuf.WriteString(tableName)
	queryBuf.WriteString(" WHERE key LIKE ? AND (ttl IS NULL OR ttl > CURRENT_TIMESTAMP) ORDER BY key LIMIT ?")

	rows, err := s.db.QueryContext(ctx, queryBuf.String(), prefix+"%", limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]json.RawMessage)
	for rows.Next() {
		var key string
		var value json.RawMessage

		if err := rows.Scan(&key, &value); err != nil {
			return nil, err
		}

		result[key] = value
	}

	return result, rows.Err()
}

func (s *ShardedKVStore) ListKeysByPrefix(ctx context.Context, prefix string, limit int, offset int) ([]string, error) {
	if limit <= 0 {
		limit = 1000
	}

	resultCh := make(chan []string, s.shardCount)
	errorCh := make(chan error, s.shardCount)

	shardLimit := limit + offset

	for i := 0; i < s.shardCount; i++ {
		go func(shardID int) {
			result, err := s.listKeysPrefixFromShard(ctx, shardID, prefix, shardLimit)
			resultCh <- result
			errorCh <- err
		}(i)
	}

	var allKeys []string
	for i := 0; i < s.shardCount; i++ {
		if err := <-errorCh; err != nil {
			return nil, err
		}

		shardKeys := <-resultCh
		allKeys = append(allKeys, shardKeys...)
	}

	for i := 0; i < len(allKeys)-1; i++ {
		for j := i + 1; j < len(allKeys); j++ {
			if allKeys[i] > allKeys[j] {
				allKeys[i], allKeys[j] = allKeys[j], allKeys[i]
			}
		}
	}

	start := offset
	if start >= len(allKeys) {
		return []string{}, nil
	}

	end := start + limit
	if end > len(allKeys) {
		end = len(allKeys)
	}

	return allKeys[start:end], nil
}

func (s *ShardedKVStore) listKeysPrefixFromShard(ctx context.Context, shardID int, prefix string, limit int) ([]string, error) {
	tableName := s.getShardTableName(shardID)

	var queryBuf strings.Builder
	queryBuf.WriteString("SELECT key FROM ")
	queryBuf.WriteString(tableName)
	queryBuf.WriteString(" WHERE key LIKE ? AND (ttl IS NULL OR ttl > CURRENT_TIMESTAMP) ORDER BY key LIMIT ?")

	rows, err := s.db.QueryContext(ctx, queryBuf.String(), prefix+"%", limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}

	return keys, rows.Err()
}

func (s *ShardedKVStore) GetRange(ctx context.Context, startKey, endKey string, limit int) (map[string]json.RawMessage, error) {
	if limit <= 0 {
		limit = 1000
	}

	resultCh := make(chan map[string]json.RawMessage, s.shardCount)
	errorCh := make(chan error, s.shardCount)

	for i := 0; i < s.shardCount; i++ {
		workers.Submit(func() {
			sharedID := i
			result, err := s.getRangeFromShard(ctx, sharedID, startKey, endKey, limit)
			resultCh <- result
			errorCh <- err
		})
	}

	var allResults []keyValuePair

	for i := 0; i < s.shardCount; i++ {
		if err := <-errorCh; err != nil {
			return nil, err
		}

		shardResult := <-resultCh
		for k, v := range shardResult {
			allResults = append(allResults, keyValuePair{key: k, value: v})
		}
	}

	for i := 0; i < len(allResults)-1; i++ {
		for j := i + 1; j < len(allResults); j++ {
			if allResults[i].key > allResults[j].key {
				allResults[i], allResults[j] = allResults[j], allResults[i]
			}
		}
	}

	end := limit
	if end > len(allResults) {
		end = len(allResults)
	}

	result := make(map[string]json.RawMessage)
	for i := 0; i < end; i++ {
		result[allResults[i].key] = allResults[i].value
	}

	return result, nil
}

func (s *ShardedKVStore) getRangeFromShard(ctx context.Context, shardID int, startKey, endKey string, limit int) (map[string]json.RawMessage, error) {
	tableName := s.getShardTableName(shardID)

	var queryBuf strings.Builder
	queryBuf.WriteString("SELECT key, value FROM ")
	queryBuf.WriteString(tableName)
	queryBuf.WriteString(" WHERE key >= ? AND key <= ? AND (ttl IS NULL OR ttl > CURRENT_TIMESTAMP) ORDER BY key LIMIT ?")

	rows, err := s.db.QueryContext(ctx, queryBuf.String(), startKey, endKey, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]json.RawMessage)
	for rows.Next() {
		var key string
		var value json.RawMessage

		if err := rows.Scan(&key, &value); err != nil {
			return nil, err
		}

		result[key] = value
	}

	return result, rows.Err()
}

func (s *ShardedKVStore) CountByPrefix(ctx context.Context, prefix string) (int64, error) {
	countCh := make(chan int64, s.shardCount)
	errorCh := make(chan error, s.shardCount)

	for i := 0; i < s.shardCount; i++ {
		go func(shardID int) {
			count, err := s.countPrefixFromShard(ctx, shardID, prefix)
			countCh <- count
			errorCh <- err
		}(i)
	}

	var totalCount int64
	for i := 0; i < s.shardCount; i++ {
		if err := <-errorCh; err != nil {
			return 0, err
		}
		totalCount += <-countCh
	}

	return totalCount, nil
}

func (s *ShardedKVStore) countPrefixFromShard(ctx context.Context, shardID int, prefix string) (int64, error) {
	tableName := s.getShardTableName(shardID)

	var count int64
	var queryBuf strings.Builder
	queryBuf.WriteString("SELECT COUNT(*) FROM ")
	queryBuf.WriteString(tableName)
	queryBuf.WriteString(" WHERE key LIKE ? AND (ttl IS NULL OR ttl > CURRENT_TIMESTAMP)")

	err := s.db.QueryRowContext(ctx, queryBuf.String(), prefix+"%").Scan(&count)
	return count, err
}

func (s *ShardedKVStore) DeleteByPrefix(ctx context.Context, prefix string) (int64, error) {
	deletedCh := make(chan int64, s.shardCount)
	errorCh := make(chan error, s.shardCount)

	for i := 0; i < s.shardCount; i++ {
		go func(shardID int) {
			deleted, err := s.deletePrefixFromShard(ctx, shardID, prefix)
			deletedCh <- deleted
			errorCh <- err
		}(i)
	}

	var totalDeleted int64
	for i := 0; i < s.shardCount; i++ {
		if err := <-errorCh; err != nil {
			return totalDeleted, err
		}
		totalDeleted += <-deletedCh
	}

	for i := 0; i < s.shardCount; i++ {
		workers.Submit(func() {
			s.updateMetrics(context.Background(), i)
		})
	}

	return totalDeleted, nil
}

func (s *ShardedKVStore) deletePrefixFromShard(ctx context.Context, shardID int, prefix string) (int64, error) {
	tableName := s.getShardTableName(shardID)

	var totalDeleted int64

	var queryBuf strings.Builder
	queryBuf.WriteString("DELETE FROM ")
	queryBuf.WriteString(tableName)
	queryBuf.WriteString(" WHERE ctid IN (SELECT ctid FROM ")
	queryBuf.WriteString(tableName)
	queryBuf.WriteString(" WHERE key LIKE ? LIMIT ")
	queryBuf.WriteString(strconv.Itoa(BatchSize))
	queryBuf.WriteString(")")

	queryStr := queryBuf.String()
	prefixPattern := prefix + "%"

	for {
		result, err := s.db.ExecContext(ctx, queryStr, prefixPattern)
		if err != nil {
			return totalDeleted, err
		}

		affected, _ := result.RowsAffected()
		totalDeleted += affected

		if affected == 0 {
			break
		}

		time.Sleep(10 * time.Millisecond)
	}

	return totalDeleted, nil
}

func (s *ShardedKVStore) GetTotalStats(ctx context.Context) (map[string]int64, error) {
	var result struct {
		TotalKeys   int64 `bun:"total_keys"`
		TotalSize   int64 `bun:"total_size"`
		ExpiredKeys int64 `bun:"expired_keys"`
		TotalShards int64 `bun:"total_shards"`
	}

	err := s.db.NewSelect().
		Model((*KVMetrics)(nil)).
		ColumnExpr("SUM(total_keys) as total_keys").
		ColumnExpr("SUM(total_size) as total_size").
		ColumnExpr("SUM(expired_keys) as expired_keys").
		ColumnExpr("COUNT(*) as total_shards").
		Scan(ctx, &result)

	if err != nil {
		return nil, err
	}

	return map[string]int64{
		"total_keys":   result.TotalKeys,
		"total_size":   result.TotalSize,
		"expired_keys": result.ExpiredKeys,
		"total_shards": result.TotalShards,
		"shard_count":  int64(s.shardCount),
	}, nil
}

func (s *ShardedKVStore) Close() error {
	return s.db.Close()
}

func (s *ShardedKVStore) executeBatch(ctx context.Context, tableName string, items []batchItem) error {
	if len(items) == 0 {
		return nil
	}

	var queryBuf strings.Builder
	queryBuf.Grow(len(items) * 50)

	queryBuf.WriteString("INSERT INTO ")
	queryBuf.WriteString(tableName)
	queryBuf.WriteString(" (key, key_hash, value, size) VALUES ")

	var args []interface{}
	args = make([]interface{}, 0, len(items)*4)

	for i, item := range items {
		if i > 0 {
			queryBuf.WriteString(", ")
		}
		queryBuf.WriteString("(?")
		queryBuf.WriteString(", ?")
		queryBuf.WriteString(", ?")
		queryBuf.WriteString(", ?")
		queryBuf.WriteString(")")

		args = append(args, item.key, item.keyHash, item.value, item.size)
	}

	queryBuf.WriteString(" ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value,")
	queryBuf.WriteString(" size = EXCLUDED.size, updated_at = CURRENT_TIMESTAMP")

	_, err := s.db.ExecContext(ctx, queryBuf.String(), args...)
	return err
}

func (s *ShardedKVStore) CleanupExpired(ctx context.Context) (int64, error) {
	var totalCleaned int64
	errCh := make(chan error, s.shardCount)
	cleanedCh := make(chan int64, s.shardCount)

	for i := 0; i < s.shardCount; i++ {
		go func(shardID int) {
			cleaned, err := s.cleanupShardExpired(ctx, shardID)
			errCh <- err
			cleanedCh <- cleaned
		}(i)
	}

	for i := 0; i < s.shardCount; i++ {
		if err := <-errCh; err != nil {
			return totalCleaned, err
		}
		totalCleaned += <-cleanedCh
	}

	return totalCleaned, nil
}

func (s *ShardedKVStore) cleanupShardExpired(ctx context.Context, shardID int) (int64, error) {
	tableName := s.getShardTableName(shardID)

	var totalCleaned int64

	var queryBuf strings.Builder
	queryBuf.WriteString("DELETE FROM ")
	queryBuf.WriteString(tableName)
	queryBuf.WriteString(" WHERE ctid IN (SELECT ctid FROM ")
	queryBuf.WriteString(tableName)
	queryBuf.WriteString(" WHERE ttl IS NOT NULL AND ttl <= CURRENT_TIMESTAMP LIMIT ")
	queryBuf.WriteString(strconv.Itoa(TTLCleanupBatchSize))
	queryBuf.WriteString(")")

	queryStr := queryBuf.String()

	for {
		result, err := s.db.ExecContext(ctx, queryStr)
		if err != nil {
			return totalCleaned, err
		}

		affected, _ := result.RowsAffected()
		totalCleaned += affected

		if affected == 0 {
			break
		}

		time.Sleep(10 * time.Millisecond)
	}

	go s.updateMetrics(context.Background(), shardID)

	return totalCleaned, nil
}

func (s *ShardedKVStore) GetMetrics(ctx context.Context) ([]KVMetrics, error) {
	var metrics []KVMetrics
	err := s.db.NewSelect().
		Model(&metrics).
		Order("shard_id").
		Scan(ctx)
	return metrics, err
}

func (s *ShardedKVStore) updateMetrics(ctx context.Context, shardID int) {
	tableName := s.getShardTableName(shardID)

	var totalKeys, totalSize, expiredKeys int64

	query1 := "SELECT COUNT(*), COALESCE(SUM(size), 0) FROM " + tableName +
		" WHERE (ttl IS NULL OR ttl > CURRENT_TIMESTAMP)"

	err := s.db.QueryRowContext(ctx, query1).Scan(&totalKeys, &totalSize)
	if err != nil {
		return
	}

	query2 := "SELECT COUNT(*) FROM " + tableName +
		" WHERE ttl IS NOT NULL AND ttl <= CURRENT_TIMESTAMP"

	err = s.db.QueryRowContext(ctx, query2).Scan(&expiredKeys)
	if err != nil {
		return
	}

	_, err = s.db.NewUpdate().
		Model((*KVMetrics)(nil)).
		Set("total_keys = ?", totalKeys).
		Set("total_size = ?", totalSize).
		Set("expired_keys = ?", expiredKeys).
		Set("updated_at = CURRENT_TIMESTAMP").
		Where("shard_id = ?", shardID).
		Exec(ctx)
}

type keyHashPair struct {
	key     string
	keyHash string
}

func (s *ShardedKVStore) BatchGet(ctx context.Context, keys []string) (map[string]json.RawMessage, error) {
	if len(keys) == 0 {
		return make(map[string]json.RawMessage), nil
	}

	shardGroups := make(map[int][]keyHashPair)

	for _, key := range keys {
		shardID, keyHash := s.hashKey(key)
		shardGroups[shardID] = append(shardGroups[shardID], keyHashPair{
			key:     key,
			keyHash: keyHash,
		})
	}

	resultCh := make(chan map[string]json.RawMessage, len(shardGroups))
	errorCh := make(chan error, len(shardGroups))

	for shardID, keyPairs := range shardGroups {
		go func(sID int, pairs []keyHashPair) {
			result, err := s.batchGetFromShard(ctx, sID, pairs)
			resultCh <- result
			errorCh <- err
		}(shardID, keyPairs)
	}

	finalResult := make(map[string]json.RawMessage)
	for i := 0; i < len(shardGroups); i++ {
		if err := <-errorCh; err != nil {
			return nil, err
		}
		shardResult := <-resultCh
		for k, v := range shardResult {
			finalResult[k] = v
		}
	}

	return finalResult, nil
}

func (s *ShardedKVStore) batchGetFromShard(ctx context.Context, shardID int, keyPairs []keyHashPair) (map[string]json.RawMessage, error) {
	if len(keyPairs) == 0 {
		return make(map[string]json.RawMessage), nil
	}

	tableName := s.getShardTableName(shardID)
	result := make(map[string]json.RawMessage)

	for i := 0; i < len(keyPairs); i += BatchSize {
		end := i + BatchSize
		if end > len(keyPairs) {
			end = len(keyPairs)
		}

		batch := keyPairs[i:end]
		batchResult, err := s.executeBatchGet(ctx, tableName, batch)
		if err != nil {
			return nil, err
		}

		for k, v := range batchResult {
			result[k] = v
		}
	}

	return result, nil
}
