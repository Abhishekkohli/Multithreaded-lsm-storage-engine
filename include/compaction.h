/**
 * compaction.h - Background compaction for LSM-tree
 * 
 * Compaction merges SSTables to:
 * 1. Reduce space amplification (remove duplicate/deleted keys)
 * 2. Reduce read amplification (fewer files to search)
 * 3. Improve read performance (better locality)
 * 
 * This implementation uses leveled compaction:
 * - Level 0: Flushed MemTables (may overlap)
 * - Levels 1-N: Non-overlapping, size increases by factor of 10
 * 
 * Compaction is triggered when:
 * - Level 0 has too many files (L0_COMPACTION_TRIGGER)
 * - A level exceeds its size target
 */

#ifndef STORAGE_ENGINE_COMPACTION_H
#define STORAGE_ENGINE_COMPACTION_H

#include "common.h"
#include "sstable.h"
#include "thread_pool.h"
#include <memory>
#include <vector>
#include <queue>
#include <condition_variable>

namespace lsm {

// Forward declarations
class StorageEngine;

// ============================================================================
// CompactionStats - Statistics for a compaction job
// ============================================================================

struct CompactionStats {
    Timestamp start_time;
    Timestamp end_time;
    
    int input_level;
    int output_level;
    
    size_t input_files;
    size_t output_files;
    
    size_t input_bytes;
    size_t output_bytes;
    
    size_t keys_read;
    size_t keys_written;
    size_t keys_dropped;  // Deleted/superseded keys
    
    /**
     * Calculate space savings
     */
    double SpaceSavings() const {
        if (input_bytes == 0) return 0;
        return 1.0 - static_cast<double>(output_bytes) / input_bytes;
    }
    
    /**
     * Calculate throughput in MB/s
     */
    double ThroughputMBps() const {
        double duration_sec = (end_time - start_time) / 1000000.0;
        if (duration_sec <= 0) return 0;
        return (input_bytes / 1024.0 / 1024.0) / duration_sec;
    }
};

// ============================================================================
// CompactionJob - Describes a compaction task
// ============================================================================

/**
 * CompactionJob represents a single compaction operation.
 * It specifies which files to compact and where to place output.
 */
struct CompactionJob {
    int input_level;
    int output_level;
    
    // Input SSTables to merge
    std::vector<std::shared_ptr<SSTableMetadata>> input_files;
    
    // Key range covered by this compaction
    std::string smallest_key;
    std::string largest_key;
    
    // Sequence number below which we can drop tombstones
    // (no snapshot needs keys older than this)
    SequenceNumber oldest_snapshot;
    
    // Statistics (filled during execution)
    CompactionStats stats;
    
    /**
     * Calculate total input size
     */
    size_t GetInputSize() const {
        size_t total = 0;
        for (const auto& f : input_files) {
            total += f->file_size;
        }
        return total;
    }
    
    /**
     * Get a descriptive string for logging
     */
    std::string ToString() const {
        char buf[256];
        snprintf(buf, sizeof(buf), 
                 "CompactionJob: L%d -> L%d, %zu files, %zu bytes",
                 input_level, output_level, 
                 input_files.size(), GetInputSize());
        return std::string(buf);
    }
};

// ============================================================================
// MergeIterator - Merges multiple sorted iterators
// ============================================================================

/**
 * MergeIterator combines multiple sorted iterators into a single sorted stream.
 * Uses a min-heap to efficiently select the smallest key at each step.
 * 
 * When multiple iterators have the same user key, we select based on:
 * 1. Highest sequence number (most recent write)
 * 2. Earlier iterator (for same sequence - shouldn't happen)
 */
class MergeIterator {
public:
    MergeIterator() = default;
    
    /**
     * Add an SSTable iterator to the merge
     */
    void AddIterator(std::unique_ptr<SSTableReader::Iterator> iter);
    
    /**
     * Position at first entry
     */
    void SeekToFirst();
    
    /**
     * Position at first entry >= target
     */
    void Seek(const InternalKey& target);
    
    /**
     * Move to next entry
     */
    void Next();
    
    /**
     * Check if positioned at valid entry
     */
    bool Valid() const { return !heap_.empty(); }
    
    /**
     * Get current key
     */
    const InternalKey& GetKey() const { return heap_.top().key; }
    
    /**
     * Get current value
     */
    const std::string& GetValue() const { 
        return children_[heap_.top().child_index]->GetValue(); 
    }

private:
    /**
     * Entry in the merge heap
     */
    struct HeapEntry {
        InternalKey key;
        size_t child_index;
        
        // Min-heap: smaller keys should come first
        bool operator>(const HeapEntry& other) const {
            return other.key < key;
        }
    };
    
    std::vector<std::unique_ptr<SSTableReader::Iterator>> children_;
    std::priority_queue<HeapEntry, std::vector<HeapEntry>, std::greater<HeapEntry>> heap_;
    
    /**
     * Add current position of child to heap
     */
    void AddToHeap(size_t child_index);
};

// ============================================================================
// CompactionPicker - Selects files for compaction
// ============================================================================

/**
 * CompactionPicker decides which files to compact.
 * 
 * Selection strategy:
 * 1. If L0 has too many files, compact L0 -> L1
 * 2. If a level exceeds its size target, compact to next level
 * 3. Pick files that overlap with fewest files in next level
 */
class CompactionPicker {
public:
    explicit CompactionPicker(SSTableManager* sstable_manager);
    
    /**
     * Check if compaction is needed
     */
    bool NeedsCompaction() const;
    
    /**
     * Pick the next compaction job
     * @return CompactionJob or nullptr if no compaction needed
     */
    std::unique_ptr<CompactionJob> PickCompaction();
    
    /**
     * Calculate the score for a level (>1.0 means needs compaction)
     */
    double GetLevelScore(int level) const;
    
    /**
     * Get target size for a level in bytes
     */
    size_t GetLevelTargetSize(int level) const;

private:
    /**
     * Find files in level+1 that overlap with the given key range
     */
    std::vector<std::shared_ptr<SSTableMetadata>> GetOverlappingFiles(
        int level, const std::string& smallest, const std::string& largest) const;
    
    /**
     * Expand compaction inputs to include all overlapping files
     */
    void ExpandInputs(CompactionJob* job) const;
    
    SSTableManager* sstable_manager_;
    
    // Track which level we last compacted from (for round-robin)
    int last_compaction_level_{0};
};

// ============================================================================
// CompactionExecutor - Executes compaction jobs
// ============================================================================

/**
 * CompactionExecutor performs the actual merge of SSTables.
 * 
 * Process:
 * 1. Open all input files
 * 2. Create merge iterator
 * 3. Iterate through merged stream, dropping duplicates/tombstones
 * 4. Write output SSTables
 * 5. Update metadata and delete old files
 */
class CompactionExecutor {
public:
    CompactionExecutor(SSTableManager* sstable_manager, 
                       const std::string& db_path);
    
    /**
     * Execute a compaction job
     * @param job The compaction job to execute
     * @return Status and list of new SSTable metadata
     */
    std::pair<Status, std::vector<std::shared_ptr<SSTableMetadata>>> 
    Execute(CompactionJob* job);

private:
    /**
     * Decide if a key should be dropped during compaction
     */
    bool ShouldDropKey(const InternalKey& key, 
                       SequenceNumber oldest_snapshot,
                       bool has_current_user_key,
                       const std::string& current_user_key) const;
    
    SSTableManager* sstable_manager_;
    std::string db_path_;
};

// ============================================================================
// CompactionManager - Coordinates background compaction
// ============================================================================

/**
 * CompactionManager runs compaction in background threads.
 * 
 * Features:
 * - Automatic compaction when thresholds exceeded
 * - Manual compaction triggers
 * - Compaction throttling to limit I/O impact
 * - Statistics and monitoring
 */
class CompactionManager {
public:
    CompactionManager(SSTableManager* sstable_manager,
                      const std::string& db_path,
                      ThreadPool* compaction_pool);
    ~CompactionManager();
    
    CompactionManager(const CompactionManager&) = delete;
    CompactionManager& operator=(const CompactionManager&) = delete;
    
    /**
     * Start background compaction
     */
    void Start();
    
    /**
     * Stop background compaction
     */
    void Stop();
    
    /**
     * Trigger a check for needed compaction
     */
    void MaybeScheduleCompaction();
    
    /**
     * Force compaction of a specific level
     */
    Status CompactLevel(int level);
    
    /**
     * Force full compaction (all levels)
     */
    Status CompactAll();
    
    /**
     * Wait for all pending compactions to complete
     */
    void WaitForCompaction();
    
    /**
     * Get number of pending compaction jobs
     */
    size_t GetPendingJobs() const;
    
    /**
     * Get compaction statistics
     */
    std::vector<CompactionStats> GetStats() const;
    
    /**
     * Check if compaction is currently running
     */
    bool IsRunning() const { return running_.load(std::memory_order_acquire); }

private:
    /**
     * Background compaction loop
     */
    void CompactionLoop();
    
    /**
     * Execute a single compaction job
     */
    void DoCompaction(std::unique_ptr<CompactionJob> job);
    
    /**
     * Callback after compaction completes
     */
    void OnCompactionComplete(CompactionJob* job,
                              const std::vector<std::shared_ptr<SSTableMetadata>>& outputs);
    
    SSTableManager* sstable_manager_;
    std::string db_path_;
    ThreadPool* compaction_pool_;
    
    std::unique_ptr<CompactionPicker> picker_;
    std::unique_ptr<CompactionExecutor> executor_;
    
    // Synchronization
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::atomic<bool> running_{false};
    std::atomic<bool> shutdown_{false};
    
    // Track in-progress compactions
    std::atomic<size_t> active_compactions_{0};
    
    // Compaction history
    std::vector<CompactionStats> history_;
    static constexpr size_t MAX_HISTORY = 100;
};

// ============================================================================
// FlushManager - Manages MemTable flushes
// ============================================================================

/**
 * FlushManager coordinates flushing MemTables to SSTables.
 * 
 * Responsibilities:
 * - Monitor MemTable sizes
 * - Schedule flushes on thread pool
 * - Coordinate with WAL for durability
 */
class FlushManager {
public:
    using FlushCallback = std::function<void(uint64_t memtable_id, 
                                             std::shared_ptr<SSTableMetadata>)>;
    
    FlushManager(SSTableManager* sstable_manager,
                 ThreadPool* flush_pool,
                 FlushCallback callback);
    ~FlushManager();
    
    FlushManager(const FlushManager&) = delete;
    FlushManager& operator=(const FlushManager&) = delete;
    
    /**
     * Schedule a MemTable for flushing
     * @param memtable The MemTable to flush
     * @param wal_file_number Associated WAL file number
     */
    void ScheduleFlush(std::shared_ptr<MemTable> memtable,
                       FileNumber wal_file_number);
    
    /**
     * Wait for all pending flushes to complete
     */
    void WaitForFlushes();
    
    /**
     * Get number of pending flushes
     */
    size_t GetPendingFlushes() const;
    
    /**
     * Shutdown the flush manager
     */
    void Shutdown();

private:
    /**
     * Execute a flush operation
     */
    void DoFlush(std::shared_ptr<MemTable> memtable, FileNumber wal_file_number);
    
    SSTableManager* sstable_manager_;
    ThreadPool* flush_pool_;
    FlushCallback callback_;
    
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::atomic<size_t> pending_flushes_{0};
    std::atomic<bool> shutdown_{false};
};

}  // namespace lsm

#endif  // STORAGE_ENGINE_COMPACTION_H
