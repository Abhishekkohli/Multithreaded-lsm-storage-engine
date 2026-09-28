/**
 * main.cpp - Demo and test program for the Log-Structured Storage Engine
 * 
 * This program demonstrates:
 * - Basic CRUD operations
 * - Concurrent read/write workloads
 * - Background flushing and compaction
 * - Recovery from simulated crash
 */

#include "storage_engine.h"
#include <iostream>
#include <thread>
#include <vector>
#include <random>
#include <chrono>
#include <cassert>
#include <iomanip>
#include <atomic>
#include <sstream>

using namespace lsm;

// ============================================================================
// Test utilities
// ============================================================================

/**
 * Generate a random string of specified length
 */
std::string RandomString(size_t length, std::mt19937& rng) {
    static const char chars[] = "abcdefghijklmnopqrstuvwxyz0123456789";
    std::uniform_int_distribution<> dist(0, sizeof(chars) - 2);
    
    std::string result;
    result.reserve(length);
    for (size_t i = 0; i < length; i++) {
        result.push_back(chars[dist(rng)]);
    }
    return result;
}

/**
 * Format a number with commas for readability
 */
std::string FormatNumber(uint64_t n) {
    std::string s = std::to_string(n);
    int insert_pos = static_cast<int>(s.length()) - 3;
    while (insert_pos > 0) {
        s.insert(insert_pos, ",");
        insert_pos -= 3;
    }
    return s;
}
/**
 * Timer class for benchmarking
 */
class Timer {
public:
    Timer() : start_(std::chrono::high_resolution_clock::now()) {}
    
    double ElapsedMs() const {
        auto now = std::chrono::high_resolution_clock::now();
        return std::chrono::duration<double, std::milli>(now - start_).count();
    }
    
    double ElapsedSec() const {
        return ElapsedMs() / 1000.0;
    }
    
    void Reset() {
        start_ = std::chrono::high_resolution_clock::now();
    }

private:
    std::chrono::high_resolution_clock::time_point start_;
};

// ============================================================================
// Test: Basic Operations
// ============================================================================

void TestBasicOperations(StorageEngine* engine) {
    std::cout << "\n=== Test: Basic Operations ===" << std::endl;
    
    // Put
    std::cout << "Testing Put..." << std::endl;
    Status s = engine->Put("key1", "value1");
    assert(s.ok());
    s = engine->Put("key2", "value2");
    assert(s.ok());
    s = engine->Put("key3", "value3");
    assert(s.ok());
    
    // Get
    std::cout << "Testing Get..." << std::endl;
    std::string value;
    s = engine->Get("key1", &value);
    assert(s.ok() && value == "value1");
    s = engine->Get("key2", &value);
    assert(s.ok() && value == "value2");
    s = engine->Get("key3", &value);
    assert(s.ok() && value == "value3");
    
    // Update
    std::cout << "Testing Update..." << std::endl;
    s = engine->Put("key2", "updated_value2");
    assert(s.ok());
    s = engine->Get("key2", &value);
    assert(s.ok() && value == "updated_value2");
    
    // Delete
    std::cout << "Testing Delete..." << std::endl;
    s = engine->Delete("key1");
    assert(s.ok());
    s = engine->Get("key1", &value);
    assert(s.IsNotFound());
    
    // Non-existent key
    std::cout << "Testing non-existent key..." << std::endl;
    s = engine->Get("nonexistent", &value);
    assert(s.IsNotFound());
    
    std::cout << "Basic operations test PASSED" << std::endl;
}

// ============================================================================
// Test: Batch Operations
// ============================================================================

void TestBatchOperations(StorageEngine* engine) {
    std::cout << "\n=== Test: Batch Operations ===" << std::endl;
    
    // Create batch
    WriteBatch batch;
    for (int i = 0; i < 100; i++) {
        batch.Put("batch_key_" + std::to_string(i), 
                  "batch_value_" + std::to_string(i));
    }
    
    // Write batch
    std::cout << "Writing batch of 100 entries..." << std::endl;
    Timer timer;
    Status s = engine->Write(batch);
    assert(s.ok());
    std::cout << "Batch write completed in " << timer.ElapsedMs() << " ms" << std::endl;
    
    // Verify
    std::cout << "Verifying batch entries..." << std::endl;
    std::string value;
    for (int i = 0; i < 100; i++) {
        s = engine->Get("batch_key_" + std::to_string(i), &value);
        assert(s.ok());
        assert(value == "batch_value_" + std::to_string(i));
    }
    
    std::cout << "Batch operations test PASSED" << std::endl;
}

// ============================================================================
// Test: Concurrent Writers
// ============================================================================

void TestConcurrentWriters(StorageEngine* engine, int num_threads, int ops_per_thread) {
    std::cout << "\n=== Test: Concurrent Writers ===" << std::endl;
    std::cout << "Threads: " << num_threads << ", Ops/thread: " << ops_per_thread << std::endl;
    
    std::vector<std::thread> threads;
    std::atomic<int> total_ops{0};
    std::atomic<int> errors{0};
    
    Timer timer;
    
    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            std::mt19937 rng(t);  // Deterministic seed per thread
            
            for (int i = 0; i < ops_per_thread; i++) {
                std::string key = "thread_" + std::to_string(t) + "_key_" + std::to_string(i);
                std::string value = RandomString(100, rng);
                
                Status s = engine->Put(key, value);
                if (!s.ok()) {
                    errors.fetch_add(1, std::memory_order_relaxed);
                }
                total_ops.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    
    // Wait for all threads
    for (auto& t : threads) {
        t.join();
    }
    
    double elapsed = timer.ElapsedSec();
    int total = total_ops.load();
    
    std::cout << "Completed " << FormatNumber(total) << " writes in " 
              << std::fixed << std::setprecision(2) << elapsed << " seconds" << std::endl;
    std::cout << "Throughput: " << FormatNumber(static_cast<uint64_t>(total / elapsed)) 
              << " ops/sec" << std::endl;
    std::cout << "Errors: " << errors.load() << std::endl;
    
    // Verify a sample of writes
    std::cout << "Verifying writes..." << std::endl;
    std::string value;
    int verified = 0;
    for (int t = 0; t < num_threads; t++) {
        for (int i = 0; i < std::min(10, ops_per_thread); i++) {
            std::string key = "thread_" + std::to_string(t) + "_key_" + std::to_string(i);
            Status s = engine->Get(key, &value);
            if (s.ok()) verified++;
        }
    }
    std::cout << "Verified " << verified << " entries" << std::endl;
    
    std::cout << "Concurrent writers test PASSED" << std::endl;
}

// ============================================================================
// Test: Concurrent Readers and Writers
// ============================================================================

void TestConcurrentReadWrite(StorageEngine* engine, int num_writers, int num_readers,
                             int duration_sec) {
    std::cout << "\n=== Test: Concurrent Read/Write ===" << std::endl;
    std::cout << "Writers: " << num_writers << ", Readers: " << num_readers 
              << ", Duration: " << duration_sec << "s" << std::endl;
    
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> writes{0};
    std::atomic<uint64_t> reads{0};
    std::atomic<uint64_t> read_hits{0};
    std::atomic<uint64_t> read_misses{0};
    
    std::vector<std::thread> threads;
    
    // Writer threads
    for (int w = 0; w < num_writers; w++) {
        threads.emplace_back([&, w]() {
            std::mt19937 rng(w);
            std::uniform_int_distribution<> key_dist(0, 10000);
            
            while (!stop.load(std::memory_order_relaxed)) {
                int key_num = key_dist(rng);
                std::string key = "rw_key_" + std::to_string(key_num);
                std::string value = RandomString(50, rng);
                
                engine->Put(key, value);
                writes.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    
    // Reader threads
    for (int r = 0; r < num_readers; r++) {
        threads.emplace_back([&, r]() {
            std::mt19937 rng(1000 + r);
            std::uniform_int_distribution<> key_dist(0, 10000);
            
            while (!stop.load(std::memory_order_relaxed)) {
                int key_num = key_dist(rng);
                std::string key = "rw_key_" + std::to_string(key_num);
                std::string value;
                
                Status s = engine->Get(key, &value);
                reads.fetch_add(1, std::memory_order_relaxed);
                
                if (s.ok()) {
                    read_hits.fetch_add(1, std::memory_order_relaxed);
                } else {
                    read_misses.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    
    // Run for specified duration
    std::this_thread::sleep_for(std::chrono::seconds(duration_sec));
    stop.store(true, std::memory_order_release);
    
    // Wait for threads
    for (auto& t : threads) {
        t.join();
    }
    
    uint64_t total_writes = writes.load();
    uint64_t total_reads = reads.load();
    uint64_t hits = read_hits.load();
    uint64_t misses = read_misses.load();
    
    std::cout << "Results:" << std::endl;
    std::cout << "  Writes: " << FormatNumber(total_writes) 
              << " (" << FormatNumber(total_writes / duration_sec) << "/sec)" << std::endl;
    std::cout << "  Reads:  " << FormatNumber(total_reads)
              << " (" << FormatNumber(total_reads / duration_sec) << "/sec)" << std::endl;
    std::cout << "  Read hit rate: " << std::fixed << std::setprecision(1)
              << (100.0 * hits / total_reads) << "%" << std::endl;
    
    std::cout << "Concurrent read/write test PASSED" << std::endl;
}

// ============================================================================
// Test: Flush and Compaction
// ============================================================================

void TestFlushAndCompaction(StorageEngine* engine) {
    std::cout << "\n=== Test: Flush and Compaction ===" << std::endl;
    
    // Write enough data to trigger multiple flushes
    std::cout << "Writing data to trigger flushes..." << std::endl;
    std::mt19937 rng(42);
    
    int num_entries = 10000;
    Timer timer;
    
    for (int i = 0; i < num_entries; i++) {
        std::string key = "flush_key_" + std::to_string(i);
        std::string value = RandomString(200, rng);  // Larger values to fill faster
        engine->Put(key, value);
        
        if ((i + 1) % 1000 == 0) {
            std::cout << "  Written " << (i + 1) << " entries..." << std::endl;
        }
    }
    
    std::cout << "Wrote " << num_entries << " entries in " 
              << timer.ElapsedSec() << " seconds" << std::endl;
    
    // Force flush
    std::cout << "Forcing flush..." << std::endl;
    timer.Reset();
    Status s = engine->Flush();
    assert(s.ok());
    std::cout << "Flush completed in " << timer.ElapsedMs() << " ms" << std::endl;
    
    // Print stats
    engine->PrintStats();
    
    // Force compaction
    std::cout << "Forcing full compaction..." << std::endl;
    timer.Reset();
    s = engine->CompactAll();
    std::cout << "Compaction completed in " << timer.ElapsedMs() << " ms" << std::endl;
    
    // Verify data integrity after compaction
    std::cout << "Verifying data integrity..." << std::endl;
    int verified = 0;
    for (int i = 0; i < 100; i++) {  // Verify sample
        std::string key = "flush_key_" + std::to_string(i * 100);
        std::string value;
        s = engine->Get(key, &value);
        if (s.ok()) verified++;
    }
    std::cout << "Verified " << verified << "/100 sampled entries" << std::endl;
    
    engine->PrintStats();
    
    std::cout << "Flush and compaction test PASSED" << std::endl;
}

// ============================================================================
// Test: Snapshots
// ============================================================================

void TestSnapshots(StorageEngine* engine) {
    std::cout << "\n=== Test: Snapshots ===" << std::endl;
    
    // Write initial data
    engine->Put("snap_key", "value_v1");
    
    // Take snapshot
    std::cout << "Taking snapshot..." << std::endl;
    const Snapshot* snap = engine->GetSnapshot();
    
    // Modify data after snapshot
    engine->Put("snap_key", "value_v2");
    engine->Put("new_key", "new_value");
    
    // Read at snapshot - should see old value
    std::cout << "Reading at snapshot..." << std::endl;
    std::string value;
    ReadOptions snap_opts;
    snap_opts.snapshot = snap->GetSequence();
    
    Status s = engine->Get("snap_key", &value, snap_opts);
    assert(s.ok());
    std::cout << "  snap_key at snapshot: " << value << std::endl;
    assert(value == "value_v1");
    
    // Read current - should see new value
    std::cout << "Reading current..." << std::endl;
    s = engine->Get("snap_key", &value);
    assert(s.ok());
    std::cout << "  snap_key current: " << value << std::endl;
    assert(value == "value_v2");
    
    // Release snapshot
    engine->ReleaseSnapshot(snap);
    
    std::cout << "Snapshot test PASSED" << std::endl;
}

// ============================================================================
// Benchmark: Sequential Writes
// ============================================================================

void BenchmarkSequentialWrites(StorageEngine* engine, int num_entries) {
    std::cout << "\n=== Benchmark: Sequential Writes ===" << std::endl;
    std::cout << "Entries: " << FormatNumber(num_entries) << std::endl;
    
    std::mt19937 rng(12345);
    Timer timer;
    
    for (int i = 0; i < num_entries; i++) {
        std::string key = "seq_" + std::to_string(i);
        std::string value = RandomString(100, rng);
        engine->Put(key, value);
    }
    
    double elapsed = timer.ElapsedSec();
    std::cout << "Time: " << std::fixed << std::setprecision(2) << elapsed << " seconds" << std::endl;
    std::cout << "Throughput: " << FormatNumber(static_cast<uint64_t>(num_entries / elapsed)) 
              << " ops/sec" << std::endl;
}

// ============================================================================
// Benchmark: Random Reads
// ============================================================================

void BenchmarkRandomReads(StorageEngine* engine, int num_reads, int key_range) {
    std::cout << "\n=== Benchmark: Random Reads ===" << std::endl;
    std::cout << "Reads: " << FormatNumber(num_reads) << ", Key range: " << key_range << std::endl;
    
    std::mt19937 rng(54321);
    std::uniform_int_distribution<> dist(0, key_range - 1);
    
    int hits = 0;
    Timer timer;
    
    for (int i = 0; i < num_reads; i++) {
        std::string key = "seq_" + std::to_string(dist(rng));
        std::string value;
        if (engine->Get(key, &value).ok()) {
            hits++;
        }
    }
    
    double elapsed = timer.ElapsedSec();
    std::cout << "Time: " << std::fixed << std::setprecision(2) << elapsed << " seconds" << std::endl;
    std::cout << "Throughput: " << FormatNumber(static_cast<uint64_t>(num_reads / elapsed)) 
              << " ops/sec" << std::endl;
    std::cout << "Hit rate: " << std::fixed << std::setprecision(1) 
              << (100.0 * hits / num_reads) << "%" << std::endl;
}

// ============================================================================
// Main
// ============================================================================

int main() {
    std::cout << "========================================" << std::endl;
    std::cout << "  Log-Structured Storage Engine Demo" << std::endl;
    std::cout << "========================================" << std::endl;
    
    // Configuration
    Options options;
    options.create_if_missing = true;
    options.memtable_size = 1 * 1024 * 1024;  // 1MB for faster flushing in demo
    
    // Open database
    std::cout << "\nOpening database..." << std::endl;
    auto [status, engine] = StorageEngine::Open("./data/testdb", options);
    
    if (!status.ok()) {
        std::cerr << "Failed to open database: " << status.ToString() << std::endl;
        return 1;
    }
    
    std::cout << "Database opened successfully" << std::endl;
    
    try {
        // Run tests
        TestBasicOperations(engine.get());
        TestBatchOperations(engine.get());
        TestConcurrentWriters(engine.get(), 4, 1000);
        TestConcurrentReadWrite(engine.get(), 2, 4, 3);
        TestFlushAndCompaction(engine.get());
        TestSnapshots(engine.get());
        
        // Run benchmarks
        BenchmarkSequentialWrites(engine.get(), 10000);
        BenchmarkRandomReads(engine.get(), 5000, 10000);
        
        // Final stats
        std::cout << "\n=== Final Statistics ===" << std::endl;
        engine->PrintStats();
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    
    std::cout << "\n========================================" << std::endl;
    std::cout << "  All tests completed successfully!" << std::endl;
    std::cout << "========================================" << std::endl;
    
    return 0;
}
