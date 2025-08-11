package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net/url"
	"strconv"
	"time"

	"github.com/sunvim/pgkv"
)

func main() {
	Init()
	password := "Pg#123!"
	encodedPassword := url.QueryEscape(password)

	// 构建连接字符串
	dsn := fmt.Sprintf("postgres://pguser:%s@localhost:5432/test?sslmode=disable", encodedPassword)

	store, err := pgkv.NewShardedKVStore(dsn, 32)

	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()

	// 1. 单个操作示例
	err = store.Set(ctx, "user:1000000", map[string]any{
		"id":   1000000,
		"name": "User " + strconv.Itoa(1000000),
		"data": "some large data...",
	})
	if err != nil {
		slog.Error("Error setting data", "error", err)
	} else {
		slog.Info("Data set successfully")
	}

	// 2. 批量写入示例（适合大量数据导入）
	batchData := make(map[string]interface{})
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("user:%d", i)
		batchData[key] = map[string]interface{}{
			"id":      i,
			"name":    fmt.Sprintf("User %d", i),
			"email":   fmt.Sprintf("user%d@example.com", i),
			"created": time.Now(),
		}
	}

	start := time.Now()
	err = store.BatchSet(ctx, batchData)
	if err != nil {
		slog.Error("Error batch setting", "error", err)
	} else {
		slog.Info("Batch set items successfully", "count", len(batchData), "duration", time.Since(start))
	}

	// 3. 批量读取示例
	keysToGet := []string{"user:1", "user:100", "user:500", "user:1000", "user:9999"}
	start = time.Now()
	batchResults, err := store.BatchGet(ctx, keysToGet)
	if err != nil {
		slog.Error("Error batch getting", "error", err)
	} else {
		slog.Info("Batch get items successfully", "count", len(keysToGet), "duration", time.Since(start))
		for key, value := range batchResults {
			slog.Info("Batch get item successfully", "key", key, "value", string(value))
		}
	}

	// 4. 前缀查询示例
	slog.Info("=== 前缀查询示例 ===")

	// 获取前缀为"user:"的前10个记录
	start = time.Now()
	prefixResults, err := store.GetByPrefix(ctx, "user:", 10, 0)
	if err != nil {
		slog.Error("Error getting by prefix", "error", err)
	} else {
		slog.Info("Get by prefix 'user:' found items successfully", "count", len(prefixResults), "duration", time.Since(start))
		for key := range prefixResults {
			slog.Info("Found key", "key", key)
		}
	}

	// 5. 仅获取keys列表（性能更好）
	start = time.Now()
	keysList, err := store.ListKeysByPrefix(ctx, "user:", 20, 0)
	if err != nil {
		slog.Error("Error listing keys by prefix", "error", err)
	} else {
		slog.Info("List keys by prefix found items successfully", "count", len(keysList), "duration", time.Since(start))
	}

	// 6. 计算前缀匹配数量
	start = time.Now()
	count, err := store.CountByPrefix(ctx, "user:")
	if err != nil {
		slog.Error("Error counting by prefix", "error", err)
	} else {
		slog.Info("Count by prefix 'user:' found items successfully", "count", count, "duration", time.Since(start))
	}

	// 7. 范围查询示例
	slog.Info("=== 范围查询示例 ===")

	start = time.Now()
	rangeResults, err := store.GetRange(ctx, "user:1000", "user:1010", 20)
	if err != nil {
		slog.Error("Error getting range", "error", err)
	} else {
		slog.Info("Get range found items successfully", "count", len(rangeResults), "duration", time.Since(start))
		for key := range rangeResults {
			slog.Info("Range key", "key", key)
		}
	}

	// 8. 添加一些带前缀的测试数据用于删除
	testData := make(map[string]interface{})
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("temp:test:%d", i)
		testData[key] = fmt.Sprintf("test data %d", i)
	}
	store.BatchSet(ctx, testData)

	// 9. 按前缀删除示例
	slog.Info("=== 按前缀删除示例 ===")
	start = time.Now()
	deleted, err := store.DeleteByPrefix(ctx, "temp:test:")
	if err != nil {
		slog.Error("Error deleting by prefix", "error", err)
	} else {
		slog.Info("Deleted items by prefix successfully", "count", deleted, "duration", time.Since(start))
	}

	// 10. 单个读取测试
	var userData map[string]interface{}
	err = store.Get(ctx, "user:1000000", &userData)
	if err != nil {
		slog.Error("Error getting data", "error", err)
	} else {
		slog.Info("Retrieved user data successfully", "data", userData)
	}

	// 11. 获取存储统计信息
	slog.Info("=== 存储统计信息 ===")
	stats, err := store.GetTotalStats(ctx)
	if err != nil {
		slog.Error("Error getting stats", "error", err)
	} else {
		slog.Info("Storage stats retrieved successfully", "stats", stats)
	}

	// 12. 清理过期数据
	cleaned, err := store.CleanupExpired(ctx)
	if err != nil {
		slog.Error("Error cleaning up expired keys", "error", err)
	} else {
		slog.Info("Cleaned up expired keys successfully", "count", cleaned)
	}

	// 13. 性能测试示例
	slog.Info("=== 性能测试 ===")
	performanceBenchmark(store)
}

// 性能基准测试
func performanceBenchmark(store *pgkv.ShardedKVStore) {
	ctx := context.Background()

	// 批量写入性能测试
	slog.Info("批量写入性能测试...")
	batchSizes := []int{100, 1000, 10000}

	for _, size := range batchSizes {
		batchData := make(map[string]interface{})
		for i := 0; i < size; i++ {
			key := fmt.Sprintf("perf:batch_%d:item_%d", size, i)
			batchData[key] = map[string]interface{}{
				"id":        i,
				"data":      fmt.Sprintf("performance test data for item %d", i),
				"timestamp": time.Now().Unix(),
				"metadata": map[string]interface{}{
					"batch_size": size,
					"created_by": "benchmark",
				},
			}
		}

		start := time.Now()
		err := store.BatchSet(ctx, batchData)
		duration := time.Since(start)

		if err != nil {
			slog.Error("批量写入错误", "size", size, "error", err)
		} else {
			qps := float64(size) / duration.Seconds()
			slog.Info("批量写入性能测试", "size", size, "duration", duration, "QPS", qps)
		}
	}

	// 批量读取性能测试
	slog.Info("批量读取性能测试...")
	for _, size := range batchSizes {
		keys := make([]string, size)
		for i := 0; i < size; i++ {
			keys[i] = fmt.Sprintf("perf:batch_%d:item_%d", size, i)
		}

		start := time.Now()
		results, err := store.BatchGet(ctx, keys)
		duration := time.Since(start)

		if err != nil {
			slog.Error("批量读取错误", "size", size, "error", err)
		} else {
			qps := float64(len(results)) / duration.Seconds()
			slog.Info("批量读取性能测试", "size", size, "duration", duration, "QPS", qps, "成功率", float64(len(results))/float64(size)*100)
		}
	}

	// 前缀查询性能测试
	slog.Info("前缀查询性能测试...")
	prefixes := []string{"perf:batch_100:", "perf:batch_1000:", "perf:batch_10000:"}

	for _, prefix := range prefixes {
		// 测试获取前100条记录
		start := time.Now()
		results, err := store.GetByPrefix(ctx, prefix, 100, 0)
		duration := time.Since(start)

		if err != nil {
			slog.Error("前缀查询错误", "prefix", prefix, "error", err)
		} else {
			slog.Info("前缀查询成功", "prefix", prefix, "count", len(results), "duration", duration)
		}

		// 测试仅获取keys列表
		start = time.Now()
		keys, err := store.ListKeysByPrefix(ctx, prefix, 100, 0)
		duration = time.Since(start)

		if err != nil {
			slog.Error("前缀keys查询错误", "prefix", prefix, "error", err)
		} else {
			slog.Info("前缀keys查询成功", "prefix", prefix, "count", len(keys), "duration", duration)
		}

		// 测试计数
		start = time.Now()
		count, err := store.CountByPrefix(ctx, prefix)
		duration = time.Since(start)

		if err != nil {
			slog.Error("前缀计数错误", "prefix", prefix, "error", err)
		} else {
			slog.Info("前缀计数成功", "prefix", prefix, "count", count, "duration", duration)
		}
	}

	// 清理测试数据
	slog.Info("清理测试数据...")
	start := time.Now()
	deleted, err := store.DeleteByPrefix(ctx, "perf:")
	duration := time.Since(start)

	if err != nil {
		slog.Error("清理测试数据错误", "error", err)
	} else {
		slog.Info("清理测试数据成功", "count", deleted, "duration", duration)
	}
}
