// Example 11 — High-Throughput Configuration and Tuning
//
// WHAT: Configure Kafka producers and consumers for maximum throughput.
//
// DEFAULT vs HIGH-THROUGHPUT SETTINGS:
//
//   Setting                  Default        High-Throughput
//   ─────────────────────────────────────────────────────────
//   Compression              none           snappy/lz4/zstd
//   Batch size               16KB           512KB-1MB
//   Linger (flush wait)      0ms            5-50ms
//   MaxOpenRequests          5              5+ (or 1 for idempotent)
//   Buffer memory            32MB           256MB
//   Fetch min bytes          1B             1MB (consumer)
//   Fetch max wait           500ms          500ms
//
// THROUGHPUT vs LATENCY TRADE-OFF:
//
//   Low latency:   small batches, no linger, no compression
//                  → fast but inefficient, lots of small network requests
//
//   High throughput: large batches, linger wait, compression
//                  → slower per-message but amortizes network overhead
//                  → compression reduces broker disk + network by 3-10x
//
// BENCHMARKING THIS EXAMPLE:
//   We measure: messages/second and MB/second for both async and sync producers.
//   Async producers achieve significantly higher throughput because they batch.
//
// HOW TO RUN:
//   make docker-up && make run-high-throughput
//
// PRODUCTION TIPS:
//   - Monitor broker CPU and network as you scale
//   - Use snappy for balanced CPU/compression; lz4 if CPU is the bottleneck
//   - Keep message size < 1MB (broker default limit)
//   - Tune num.partitions for your target throughput × parallelism
//   - Use async producer for >50k msg/s; sync producer caps at ~10k msg/s per goroutine
package main

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
)

const (
	topic        = "high-throughput-demo"
	numMessages  = 10_000 // messages to benchmark
	messageSize  = 256    // bytes per message
)

func main() {
	brokers := []string{"localhost:9092"}

	fmt.Println("High-Throughput Kafka Benchmarks")
	fmt.Printf("Messages: %d × %d bytes = %.1f KB total\n\n",
		numMessages, messageSize, float64(numMessages*messageSize)/1024)

	// ── Benchmark 1: Default sync producer (baseline) ────────────────────────
	fmt.Println("═══ Benchmark 1: Sync Producer (default settings) ═══")
	syncRate := benchmarkSyncProducer(brokers, defaultSyncConfig())
	fmt.Printf("Result: %.0f messages/sec | %.1f MB/sec\n\n", syncRate,
		syncRate*messageSize/1024/1024)

	// ── Benchmark 2: Sync producer with compression ───────────────────────────
	fmt.Println("═══ Benchmark 2: Sync Producer with Snappy compression ═══")
	syncCompressedRate := benchmarkSyncProducer(brokers, compressedSyncConfig())
	fmt.Printf("Result: %.0f messages/sec | %.1f MB/sec\n\n", syncCompressedRate,
		syncCompressedRate*messageSize/1024/1024)

	// ── Benchmark 3: Async producer (high throughput) ─────────────────────────
	fmt.Println("═══ Benchmark 3: Async Producer (batched + compressed) ═══")
	asyncRate := benchmarkAsyncProducer(brokers, highThroughputAsyncConfig())
	fmt.Printf("Result: %.0f messages/sec | %.1f MB/sec\n\n", asyncRate,
		asyncRate*messageSize/1024/1024)

	// ── Benchmark 4: Parallel async producers ────────────────────────────────
	fmt.Println("═══ Benchmark 4: 4× Parallel Async Producers ═══")
	parallelRate := benchmarkParallelAsync(brokers, 4)
	fmt.Printf("Result: %.0f messages/sec | %.1f MB/sec\n\n", parallelRate,
		parallelRate*messageSize/1024/1024)

	// ── Summary ───────────────────────────────────────────────────────────────
	fmt.Println("═══ Summary ═══════════════════════════════════════════════")
	fmt.Printf("  Sync (default):    %8.0f msg/s\n", syncRate)
	fmt.Printf("  Sync (compressed): %8.0f msg/s  (%.1fx)\n", syncCompressedRate, syncCompressedRate/syncRate)
	fmt.Printf("  Async (tuned):     %8.0f msg/s  (%.1fx)\n", asyncRate, asyncRate/syncRate)
	fmt.Printf("  Async (4x parallel):%7.0f msg/s  (%.1fx)\n", parallelRate, parallelRate/syncRate)
	fmt.Println()
	fmt.Println("Key insight: Async + compression + batching = dramatically higher throughput.")
	fmt.Println("For maximum throughput, use multiple async producer goroutines.")
}

// defaultSyncConfig is a baseline sync producer with no optimization.
func defaultSyncConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_6_0_0
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForLocal // Fastest acks for benchmarking
	return cfg
}

// compressedSyncConfig adds snappy compression to the sync producer.
func compressedSyncConfig() *sarama.Config {
	cfg := defaultSyncConfig()
	cfg.Producer.Compression = sarama.CompressionSnappy
	return cfg
}

// highThroughputAsyncConfig configures for maximum batch throughput.
func highThroughputAsyncConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_6_0_0
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	cfg.Producer.RequiredAcks = sarama.WaitForLocal

	// Compression: snappy reduces payload by ~60-80% for JSON workloads.
	// This means 5x less disk IO on the broker and 5x less network bandwidth.
	cfg.Producer.Compression = sarama.CompressionSnappy

	// Flush.Frequency: wait up to 10ms for more messages before sending a batch.
	// Small linger = lower latency; larger linger = bigger batches = higher throughput.
	cfg.Producer.Flush.Frequency = 10 * time.Millisecond

	// Flush.Bytes: send the batch when it reaches 512KB (even before linger expires).
	cfg.Producer.Flush.Bytes = 512 * 1024

	// Flush.Messages: or when N messages are buffered.
	cfg.Producer.Flush.Messages = 1000

	// MaxOpenRequests: number of in-flight requests per broker connection.
	// Higher = more pipeline parallelism = higher throughput.
	// But don't set > 1 when using idempotent producer (ordering guarantee requires 1).
	cfg.Net.MaxOpenRequests = 5

	// ChannelBufferSize: size of the internal message queue.
	// Larger = more buffering, more memory usage, but smoother throughput.
	cfg.ChannelBufferSize = 8192

	return cfg
}

// benchmarkSyncProducer measures sync producer throughput.
func benchmarkSyncProducer(brokers []string, cfg *sarama.Config) float64 {
	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		log.Printf("Skipping benchmark (Kafka not running): %v", err)
		return 0
	}
	defer producer.Close()

	payload := make([]byte, messageSize)
	for i := range payload {
		payload[i] = byte('A' + i%26)
	}

	start := time.Now()
	var errors int
	for i := 0; i < numMessages; i++ {
		_, _, err := producer.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(payload),
		})
		if err != nil {
			errors++
		}
	}
	elapsed := time.Since(start)

	rate := float64(numMessages-errors) / elapsed.Seconds()
	fmt.Printf("  Sent %d messages in %v (%d errors)\n", numMessages, elapsed.Round(time.Millisecond), errors)
	return rate
}

// benchmarkAsyncProducer measures async producer throughput.
func benchmarkAsyncProducer(brokers []string, cfg *sarama.Config) float64 {
	producer, err := sarama.NewAsyncProducer(brokers, cfg)
	if err != nil {
		log.Printf("Skipping benchmark (Kafka not running): %v", err)
		return 0
	}

	var successCount, errorCount int64

	// Drain goroutines are mandatory for async producers.
	var drainWg sync.WaitGroup
	drainWg.Add(2)
	go func() {
		defer drainWg.Done()
		for range producer.Successes() {
			atomic.AddInt64(&successCount, 1)
		}
	}()
	go func() {
		defer drainWg.Done()
		for range producer.Errors() {
			atomic.AddInt64(&errorCount, 1)
		}
	}()

	payload := make([]byte, messageSize)

	start := time.Now()
	for i := 0; i < numMessages; i++ {
		producer.Input() <- &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(payload),
		}
	}

	// AsyncClose signals the producer to flush remaining messages, then close.
	producer.AsyncClose()
	drainWg.Wait()

	elapsed := time.Since(start)
	sent := atomic.LoadInt64(&successCount)
	errs := atomic.LoadInt64(&errorCount)

	fmt.Printf("  Sent %d messages in %v (%d errors)\n", sent+errs, elapsed.Round(time.Millisecond), errs)
	return float64(sent) / elapsed.Seconds()
}

// benchmarkParallelAsync runs N async producers concurrently.
func benchmarkParallelAsync(brokers []string, n int) float64 {
	payload := make([]byte, messageSize)
	msgsPerProducer := numMessages / n

	var wg sync.WaitGroup
	var totalMessages int64
	start := time.Now()

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			cfg := highThroughputAsyncConfig()
			producer, err := sarama.NewAsyncProducer(brokers, cfg)
			if err != nil {
				return
			}

			var count int64
			var innerWg sync.WaitGroup
			innerWg.Add(2)
			go func() {
				defer innerWg.Done()
				for range producer.Successes() {
					atomic.AddInt64(&count, 1)
				}
			}()
			go func() {
				defer innerWg.Done()
				for range producer.Errors() {}
			}()

			for j := 0; j < msgsPerProducer; j++ {
				producer.Input() <- &sarama.ProducerMessage{
					Topic: topic,
					Value: sarama.ByteEncoder(payload),
				}
			}

			producer.AsyncClose()
			innerWg.Wait()
			atomic.AddInt64(&totalMessages, count)
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)

	total := atomic.LoadInt64(&totalMessages)
	fmt.Printf("  %d producers sent %d messages in %v\n", n, total, elapsed.Round(time.Millisecond))
	return float64(total) / elapsed.Seconds()
}
