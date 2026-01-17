/**
 * storage_engine.h - Main interface for the log-structured storage engine
 * 
 * This is the primary interface for applications to interact with the storage
 * engine. It provides:
 * - Put/Get/Delete operations
 * - Batch writes for atomicity
 * - Iteration over key ranges
 * - Snapshot isolation for consistent reads
 * 
 * Architecture:
 * - Writes go to WAL (for durability) then MemTable (for fast reads)
 * - MemTables are flushed to L0 SSTables when full
 * - Background compaction merges SSTables down the level hierarchy
 */

#ifndef STORAGE_ENGINE_STORAGE_ENGINE_H
#define STORAGE_ENGINE_STORAGE_ENGINE_H

#include "common.h"
#include "memtable.h"
#include "wal.h"
#include "sstable.h"
#include "compaction.h"
#include "thread_pool.h"
#include <string>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <atomic>
#include <unordered_set>

namespace lsm {

// ============================================================================
// Options for opening the storage engine
// ============================================================================

struct Options {
    // Create database if it doesn't exist
    bool create_if_missing = true;
    
    // Fail if database already exists
    bool error_if_exists = false;
    
    // Write options
    bool sync_writes = false;  // fsync after each write
    
    // Memory limits
    size_t memtable_size = config::MEMTABLE_SIZE_LIMIT;
    
    // Thread pool sizes
    size_t num_write_threads = config::MAX_THREADS / 2;
    size_t num_flush_threads = config::FLUSH_THREADS;
    size_t num_compaction_threads = config::COMPACTION_THREADS;
    
    // Compaction options
    bool enable_compaction = true;
};

// ============================================================================
// Write options for individual operations
// ============================================================================

struct WriteOptions {
    // Sync to disk after write (slower but durable)
    bool sync = false;
};

// ============================================================================
// Read options for queries
// ============================================================================

struct ReadOptions {
    // Read at a specific snapshot (0 means latest)
    SequenceNumber snapshot = 0;
    
    // Fill cache with read data
    bool fill_cache = true;
    
    // Verify checksums during read
    bool verify_checksums = true;
};

// ============================================================================
// WriteBatch - Atomic batch of writes
// ============================================================================

/**
 * WriteBatch groups multiple writes into a single atomic operation.
 * All writes in a batch either succeed together or fail together.
 * 
 * Usage:
 *   WriteBatch batch;
 *   batch.Put("key1", "value1");
 *   batch.Put("key2", "value2");
 *   batch.Delete("key3");
 *   engine.Write(batch);
 */
class WriteBatch {
public:
    WriteBatch() = default;
    
    /**
     * Add a Put operation to the batch
     */
    void Put(const std::string& key, const std::string& value) {
        operations_.push_back({OpType::PUT, key, value});
    }
    
    /**
     * Add a Delete operation to the batch
     */
    void Delete(const std::string& key) {
        operations_.push_back({OpType::DELETE, key, ""});
    }
    
    /**
     * Clear all operations
     */
    void Clear() {
        operations_.clear();
    }
    
    /**
     * Get number of operations
     */
    size_t Count() const { return operations_.size(); }
    
    /**
     * Check if batch is empty
     */
    bool Empty() const { return operations_.empty(); }

private:
    friend class StorageEngine;
    
    enum class OpType { PUT, DELETE };
    
    struct Operation {
        OpType type;
        std::string key;
        std::string value;
    };
    
    std::vector<Operation> operations_;
};

// ============================================================================
// Snapshot - Consistent view of the database
// ============================================================================

/**
 * Snapshot provides a consistent point-in-time view of the database.
 * Reads using a snapshot see all writes committed before the snapshot
 * was created, and none of the writes after.
 * 
 * Snapshots prevent data from being garbage collected during compaction,
 * so they should be released when no longer needed.
 */
class Snapshot {
public:
    SequenceNumber GetSequence() const { return sequence_; }
    
private:
    friend class StorageEngine;
    
    explicit Snapshot(SequenceNumber seq) : sequence_(seq) {}
    SequenceNumber sequence_;
};

// ============================================================================
// Iterator - Scan over key range
// ============================================================================

/**
 * Iterator for scanning keys in sorted order.
 * Provides forward iteration with seeking capability.
 */
class DBIterator {
public:
    DBIterator(const StorageEngine* engine, ReadOptions options);
    ~DBIterator();
    
    /**
     * Check if iterator is positioned at a valid entry
     */
    bool Valid() const { return valid_; }
    
    /**
     * Position at the first key
     */
    void SeekToFirst();
    
    /**
     * Position at the first key >= target
     */
    void Seek(const std::string& target);
    
    /**
     * Move to the next key
     */
    void Next();
    
    /**
     * Get current key
     */
    std::string Key() const;
    
    /**
     * Get current value
     */
    std::string Value() const;
    
    /**
     * Get status of last operation
     */
    Status GetStatus() const { return status_; }

private:
    const StorageEngine* engine_;
    ReadOptions options_;
    
    // Merge iterator over MemTables and SSTables
    std::unique_ptr<MergeIterator> merge_iter_;
    
    // Track current position
    std::string current_key_;
    std::string current_value_;
    bool valid_;
    Status status_;
};

// ============================================================================
// StorageEngine - Main database interface
// ============================================================================

/**
 * StorageEngine is the main entry point for the log-structured storage.
 * 
 * Thread safety:
 * - Put/Get/Delete are thread-safe and can be called concurrently
 * - Background threads handle flushing and compaction
 * - Snapshots provide isolation for concurrent readers
 */
class StorageEngine {
public:
    /**
     * Open a database at the given path
     * @param path Directory to store database files
     * @param options Configuration options
     * @return Status and engine pointer
     */
    static std::pair<Status, std::unique_ptr<StorageEngine>> 
    Open(const std::string& path, const Options& options = Options());
    
    /**
     * Destructor - performs graceful shutdown
     */
    ~StorageEngine();
    
    StorageEngine(const StorageEngine&) = delete;
    StorageEngine& operator=(const StorageEngine&) = delete;
    
    // ========================================================================
    // Write Operations
    // ========================================================================
    
    /**
     * Put a key-value pair
     * @param key The key to store
     * @param value The value to associate with the key
     * @param options Write options
     */
    Status Put(const std::string& key, const std::string& value,
               const WriteOptions& options = WriteOptions());
    
    /**
     * Delete a key
     * @param key The key to delete
     * @param options Write options
     */
    Status Delete(const std::string& key,
                  const WriteOptions& options = WriteOptions());
    
    /**
     * Apply a batch of writes atomically
     * @param batch The batch of operations
     * @param options Write options
     */
    Status Write(const WriteBatch& batch,
                 const WriteOptions& options = WriteOptions());
    
    // ========================================================================
    // Read Operations
    // ========================================================================
    
    /**
     * Get the value for a key
     * @param key The key to look up
     * @param value Output parameter for the value
     * @param options Read options
     * @return Status::OK if found, Status::NotFound if not present
     */
    Status Get(const std::string& key, std::string* value,
               const ReadOptions& options = ReadOptions()) const;
    
    /**
     * Create an iterator for scanning keys
     */
    std::unique_ptr<DBIterator> NewIterator(
        const ReadOptions& options = ReadOptions()) const;
    
    // ========================================================================
    // Snapshot Operations
    // ========================================================================
    
    /**
     * Create a snapshot of the current database state
     * @return Snapshot that must be released when done
     */
    const Snapshot* GetSnapshot();
    
    /**
     * Release a snapshot
     */
    void ReleaseSnapshot(const Snapshot* snapshot);
    
    // ========================================================================
    // Administrative Operations
    // ========================================================================
    
    /**
     * Force flush of all MemTables to disk
     */
    Status Flush();
    
    /**
     * Force compaction of a level
     */
    Status CompactLevel(int level);
    
    /**
     * Force full compaction
     */
    Status CompactAll();
    
    /**
     * Get database path
     */
    const std::string& GetPath() const { return db_path_; }
    
    /**
     * Get current sequence number
     */
    SequenceNumber GetSequence() const {
        return sequence_.load(std::memory_order_acquire);
    }
    
    // ========================================================================
    // Statistics
    // ========================================================================
    
    struct Stats {
        uint64_t num_puts;
        uint64_t num_gets;
        uint64_t num_deletes;
        uint64_t memtable_size;
        uint64_t num_sstables;
        uint64_t total_sstable_size;
        std::vector<size_t> level_sizes;
    };
    
    /**
     * Get database statistics
     */
    Stats GetStats() const;
    
    /**
     * Print statistics to stdout
     */
    void PrintStats() const;

private:
    /**
     * Private constructor - use Open() to create
     */
    explicit StorageEngine(const std::string& path, const Options& options);
    
    /**
     * Initialize the database
     */
    Status Initialize();
    
    /**
     * Recover from WAL after crash
     */
    Status Recover();
    
    /**
     * Internal write implementation
     */
    Status WriteInternal(const std::vector<std::pair<InternalKey, std::string>>& entries,
                         bool sync);
    
    /**
     * Check if MemTable needs to be flushed and schedule if so
     */
    void MaybeScheduleFlush();
    
    /**
     * Callback when flush completes
     */
    void OnFlushComplete(uint64_t memtable_id, 
                         std::shared_ptr<SSTableMetadata> sstable);
    
    /**
     * Get the oldest snapshot sequence number
     */
    SequenceNumber GetOldestSnapshot() const;
    
    std::string db_path_;
    Options options_;
    
    // Sequence number for MVCC
    std::atomic<SequenceNumber> sequence_{0};
    
    // Write synchronization
    std::mutex write_mutex_;
    
    // MemTable management
    std::unique_ptr<MemTableList> memtables_;
    
    // WAL management
    std::unique_ptr<WALManager> wal_manager_;
    std::shared_ptr<WALWriter> current_wal_;
    std::mutex wal_mutex_;
    
    // SSTable management
    std::unique_ptr<SSTableManager> sstable_manager_;
    
    // Background processing
    std::unique_ptr<ThreadPoolManager> thread_pools_;
    std::unique_ptr<FlushManager> flush_manager_;
    std::unique_ptr<CompactionManager> compaction_manager_;
    
    // Snapshot tracking
    mutable std::mutex snapshot_mutex_;
    std::unordered_set<SequenceNumber> active_snapshots_;
    
    // Statistics
    mutable std::atomic<uint64_t> num_puts_{0};
    mutable std::atomic<uint64_t> num_gets_{0};
    mutable std::atomic<uint64_t> num_deletes_{0};
    
    // State
    std::atomic<bool> shutdown_{false};
};

}  // namespace lsm

#endif  // STORAGE_ENGINE_STORAGE_ENGINE_H
