/**
 * storage_engine.cpp - Main storage engine implementation
 * 
 * This file implements the main database interface, coordinating:
 * - Write path: WAL -> MemTable -> SSTable
 * - Read path: MemTable -> SSTables
 * - Background operations: Flush, Compaction
 */

#include "storage_engine.h"
#include <sys/stat.h>
#include <algorithm>
#include <iostream>
#include <iomanip>

namespace lsm {

// ============================================================================
// DBIterator Implementation
// ============================================================================

DBIterator::DBIterator(const StorageEngine* engine, ReadOptions options)
    : engine_(engine)
    , options_(options)
    , valid_(false) 
{
    // Create merge iterator over all data sources
    merge_iter_ = std::make_unique<MergeIterator>();
    
    // Add SSTable iterators
    // (In a full implementation, we'd add MemTable iterators too)
}

DBIterator::~DBIterator() = default;

void DBIterator::SeekToFirst() {
    merge_iter_->SeekToFirst();
    
    if (merge_iter_->Valid()) {
        // Find the first non-deleted key
        while (merge_iter_->Valid()) {
            const InternalKey& key = merge_iter_->GetKey();
            if (key.type != ValueType::DELETION) {
                current_key_ = key.user_key;
                current_value_ = merge_iter_->GetValue();
                valid_ = true;
                return;
            }
            merge_iter_->Next();
        }
    }
    
    valid_ = false;
}

void DBIterator::Seek(const std::string& target) {
    InternalKey lookup_key(target, options_.snapshot ? options_.snapshot : UINT64_MAX,
                           ValueType::VALUE);
    merge_iter_->Seek(lookup_key);
    
    if (merge_iter_->Valid()) {
        const InternalKey& key = merge_iter_->GetKey();
        if (key.user_key >= target && key.type != ValueType::DELETION) {
            current_key_ = key.user_key;
            current_value_ = merge_iter_->GetValue();
            valid_ = true;
            return;
        }
    }
    
    valid_ = false;
}

void DBIterator::Next() {
    if (!valid_) return;
    
    std::string prev_key = current_key_;
    
    // Skip over any entries with the same user key (older versions)
    while (merge_iter_->Valid()) {
        merge_iter_->Next();
        if (!merge_iter_->Valid()) break;
        
        const InternalKey& key = merge_iter_->GetKey();
        if (key.user_key != prev_key) {
            if (key.type != ValueType::DELETION) {
                current_key_ = key.user_key;
                current_value_ = merge_iter_->GetValue();
                return;
            }
            prev_key = key.user_key;
        }
    }
    
    valid_ = false;
}

std::string DBIterator::Key() const {
    return current_key_;
}

std::string DBIterator::Value() const {
    return current_value_;
}

// ============================================================================
// StorageEngine Implementation
// ============================================================================

StorageEngine::StorageEngine(const std::string& path, const Options& options)
    : db_path_(path)
    , options_(options) 
{
}

std::pair<Status, std::unique_ptr<StorageEngine>> 
StorageEngine::Open(const std::string& path, const Options& options) {
    // Create engine instance
    std::unique_ptr<StorageEngine> engine(new StorageEngine(path, options));
    
    // Initialize
    Status s = engine->Initialize();
    if (!s.ok()) {
        return {s, nullptr};
    }
    
    return {Status::OK(), std::move(engine)};
}

StorageEngine::~StorageEngine() {
    // Signal shutdown
    shutdown_.store(true, std::memory_order_release);
    
    // Stop background operations
    if (compaction_manager_) {
        compaction_manager_->Stop();
    }
    if (flush_manager_) {
        flush_manager_->Shutdown();
    }
    
    // Flush remaining data
    Flush();
    
    // Close WAL
    if (current_wal_) {
        current_wal_->Sync();
        current_wal_->Close();
    }
    
    // Shutdown thread pools
    if (thread_pools_) {
        thread_pools_->ShutdownAll();
    }
    
    std::cout << "Storage engine shutdown complete" << std::endl;
}

Status StorageEngine::Initialize() {
    // Create database directory if needed
    struct stat st;
    if (stat(db_path_.c_str(), &st) != 0) {
        if (options_.create_if_missing) {
            // Create parent directories if needed
            std::string parent = db_path_.substr(0, db_path_.rfind('/'));
            if (!parent.empty()) {
                mkdir(parent.c_str(), 0755);  // Ignore error if exists
            }
            
            if (mkdir(db_path_.c_str(), 0755) != 0) {
                return Status::IOError("Failed to create database directory");
            }
        } else {
            return Status::IOError("Database directory does not exist");
        }
    } else if (options_.error_if_exists) {
        return Status::InvalidArgument("Database already exists");
    }
    
    // Initialize thread pools
    thread_pools_ = std::make_unique<ThreadPoolManager>();
    
    // Initialize MemTable list
    memtables_ = std::make_unique<MemTableList>();
    
    // Initialize WAL manager
    wal_manager_ = std::make_unique<WALManager>(db_path_);
    Status s = wal_manager_->Init();
    if (!s.ok()) return s;
    
    // Initialize SSTable manager
    sstable_manager_ = std::make_unique<SSTableManager>(db_path_);
    s = sstable_manager_->Init();
    if (!s.ok()) return s;
    
    // Recover from WAL
    s = Recover();
    if (!s.ok()) return s;
    
    // Create current WAL
    current_wal_ = wal_manager_->CreateWAL();
    if (!current_wal_) {
        return Status::IOError("Failed to create WAL");
    }
    
    // Initialize flush manager
    flush_manager_ = std::make_unique<FlushManager>(
        sstable_manager_.get(),
        thread_pools_->GetFlushPool(),
        [this](uint64_t memtable_id, std::shared_ptr<SSTableMetadata> sstable) {
            OnFlushComplete(memtable_id, sstable);
        });
    
    // Initialize compaction manager
    if (options_.enable_compaction) {
        compaction_manager_ = std::make_unique<CompactionManager>(
            sstable_manager_.get(),
            db_path_,
            thread_pools_->GetCompactionPool());
        compaction_manager_->Start();
    }
    
    std::cout << "Storage engine initialized at: " << db_path_ << std::endl;
    return Status::OK();
}

Status StorageEngine::Recover() {
    // Replay WAL entries
    auto [status, max_seq] = wal_manager_->Recover(
        [this](const WALEntry& entry) -> Status {
            // Replay entry to MemTable
            auto memtable = memtables_->GetMutable();
            
            if (entry.op == WALEntry::OpType::PUT) {
                memtable->Put(entry.key, entry.value, entry.sequence);
            } else {
                memtable->Delete(entry.key, entry.sequence);
            }
            
            return Status::OK();
        });
    
    if (!status.ok()) {
        return status;
    }
    
    // Update sequence number
    if (max_seq > 0) {
        sequence_.store(max_seq + 1, std::memory_order_release);
        std::cout << "Recovered to sequence " << max_seq << std::endl;
    }
    
    // Delete old WAL files (data is now in MemTable)
    return wal_manager_->DeleteAllWALs();
}

Status StorageEngine::Put(const std::string& key, const std::string& value,
                          const WriteOptions& options) {
    WriteBatch batch;
    batch.Put(key, value);
    return Write(batch, options);
}

Status StorageEngine::Delete(const std::string& key,
                             const WriteOptions& options) {
    WriteBatch batch;
    batch.Delete(key);
    return Write(batch, options);
}

Status StorageEngine::Write(const WriteBatch& batch,
                            const WriteOptions& options) {
    if (batch.Empty()) {
        return Status::OK();
    }
    
    if (shutdown_.load(std::memory_order_acquire)) {
        return Status::Shutdown("Database is shutting down");
    }
    
    // Prepare entries with sequence numbers
    std::vector<std::pair<InternalKey, std::string>> entries;
    entries.reserve(batch.operations_.size());
    
    // Acquire write lock to ensure atomic sequence number assignment
    std::lock_guard<std::mutex> lock(write_mutex_);
    
    SequenceNumber seq = sequence_.fetch_add(batch.operations_.size(),
                                              std::memory_order_acq_rel);
    
    for (const auto& op : batch.operations_) {
        ValueType type = (op.type == WriteBatch::OpType::PUT) 
                         ? ValueType::VALUE : ValueType::DELETION;
        entries.emplace_back(InternalKey(op.key, seq++, type), op.value);
        
        // Update stats
        if (op.type == WriteBatch::OpType::PUT) {
            num_puts_.fetch_add(1, std::memory_order_relaxed);
        } else {
            num_deletes_.fetch_add(1, std::memory_order_relaxed);
        }
    }
    
    return WriteInternal(entries, options.sync);
}

Status StorageEngine::WriteInternal(
    const std::vector<std::pair<InternalKey, std::string>>& entries,
    bool sync) {
    
    // Write to WAL first (for durability)
    {
        std::lock_guard<std::mutex> wal_lock(wal_mutex_);
        
        for (const auto& [key, value] : entries) {
            WALEntry wal_entry;
            wal_entry.key = key.user_key;
            wal_entry.value = value;
            wal_entry.sequence = key.sequence;
            wal_entry.op = (key.type == ValueType::VALUE) 
                           ? WALEntry::OpType::PUT : WALEntry::OpType::DELETE;
            
            Status s = current_wal_->Append(wal_entry, sync);
            if (!s.ok()) {
                return s;
            }
        }
        
        if (sync) {
            Status s = current_wal_->Sync();
            if (!s.ok()) return s;
        }
    }
    
    // Write to MemTable
    auto memtable = memtables_->GetMutable();
    
    for (const auto& [key, value] : entries) {
        if (key.type == ValueType::VALUE) {
            memtable->Put(key.user_key, value, key.sequence);
        } else {
            memtable->Delete(key.user_key, key.sequence);
        }
    }
    
    // Check if we need to flush
    MaybeScheduleFlush();
    
    return Status::OK();
}

void StorageEngine::MaybeScheduleFlush() {
    auto memtable = memtables_->GetMutable();
    
    if (memtable->ShouldFlush()) {
        // Rotate MemTable (creates new mutable, old becomes immutable)
        auto immutable = memtables_->RotateMemTable();
        
        if (immutable) {
            // Create new WAL for new writes
            FileNumber old_wal_num = current_wal_->GetFileNumber();
            
            {
                std::lock_guard<std::mutex> wal_lock(wal_mutex_);
                current_wal_->Sync();
                current_wal_ = wal_manager_->CreateWAL();
            }
            
            // Schedule flush of immutable MemTable
            flush_manager_->ScheduleFlush(immutable, old_wal_num);
        }
    }
}

void StorageEngine::OnFlushComplete(uint64_t memtable_id,
                                    std::shared_ptr<SSTableMetadata> sstable) {
    // Remove flushed MemTable
    memtables_->RemoveImmutable(memtable_id);
    
    // Delete corresponding WAL
    // (In a full implementation, we'd track which WAL goes with which MemTable)
    
    // Trigger compaction check
    if (compaction_manager_) {
        compaction_manager_->MaybeScheduleCompaction();
    }
}

Status StorageEngine::Get(const std::string& key, std::string* value,
                          const ReadOptions& options) const {
    if (shutdown_.load(std::memory_order_acquire)) {
        return Status::Shutdown("Database is shutting down");
    }
    
    num_gets_.fetch_add(1, std::memory_order_relaxed);
    
    SequenceNumber read_seq = options.snapshot ? options.snapshot 
                              : sequence_.load(std::memory_order_acquire);
    
    // First check MemTables (most recent data)
    Status s = memtables_->Get(key, read_seq, value);
    if (s.ok() || !s.IsNotFound()) {
        return s;
    }
    
    // Then check SSTables
    return sstable_manager_->Get(key, read_seq, value);
}

std::unique_ptr<DBIterator> StorageEngine::NewIterator(
    const ReadOptions& options) const {
    return std::make_unique<DBIterator>(this, options);
}

const Snapshot* StorageEngine::GetSnapshot() {
    SequenceNumber seq = sequence_.load(std::memory_order_acquire);
    
    std::lock_guard<std::mutex> lock(snapshot_mutex_);
    active_snapshots_.insert(seq);
    
    return new Snapshot(seq);
}

void StorageEngine::ReleaseSnapshot(const Snapshot* snapshot) {
    if (!snapshot) return;
    
    {
        std::lock_guard<std::mutex> lock(snapshot_mutex_);
        active_snapshots_.erase(snapshot->GetSequence());
    }
    
    delete snapshot;
}

SequenceNumber StorageEngine::GetOldestSnapshot() const {
    std::lock_guard<std::mutex> lock(snapshot_mutex_);
    
    if (active_snapshots_.empty()) {
        return sequence_.load(std::memory_order_acquire);
    }
    
    return *std::min_element(active_snapshots_.begin(), active_snapshots_.end());
}

Status StorageEngine::Flush() {
    // Rotate current MemTable and wait for flush
    auto memtable = memtables_->GetMutable();
    
    if (memtable->Size() > 0) {
        auto immutable = memtables_->RotateMemTable();
        if (immutable) {
            FileNumber wal_num = current_wal_->GetFileNumber();
            
            {
                std::lock_guard<std::mutex> wal_lock(wal_mutex_);
                current_wal_->Sync();
                current_wal_ = wal_manager_->CreateWAL();
            }
            
            flush_manager_->ScheduleFlush(immutable, wal_num);
        }
    }
    
    // Wait for all pending flushes
    flush_manager_->WaitForFlushes();
    
    return Status::OK();
}

Status StorageEngine::CompactLevel(int level) {
    if (!compaction_manager_) {
        return Status::NotSupported("Compaction disabled");
    }
    return compaction_manager_->CompactLevel(level);
}

Status StorageEngine::CompactAll() {
    if (!compaction_manager_) {
        return Status::NotSupported("Compaction disabled");
    }
    return compaction_manager_->CompactAll();
}

StorageEngine::Stats StorageEngine::GetStats() const {
    Stats stats;
    stats.num_puts = num_puts_.load(std::memory_order_relaxed);
    stats.num_gets = num_gets_.load(std::memory_order_relaxed);
    stats.num_deletes = num_deletes_.load(std::memory_order_relaxed);
    stats.memtable_size = memtables_->TotalMemoryUsage();
    stats.num_sstables = sstable_manager_->GetTotalSSTables();
    
    size_t total_size = 0;
    for (int level = 0; level < static_cast<int>(config::MAX_LEVELS); level++) {
        size_t level_size = sstable_manager_->GetLevelSize(level);
        stats.level_sizes.push_back(level_size);
        total_size += level_size;
    }
    stats.total_sstable_size = total_size;
    
    return stats;
}

void StorageEngine::PrintStats() const {
    auto stats = GetStats();
    
    std::cout << "\n=== Storage Engine Statistics ===" << std::endl;
    std::cout << "Path: " << db_path_ << std::endl;
    std::cout << "Sequence: " << GetSequence() << std::endl;
    std::cout << std::endl;
    
    std::cout << "Operations:" << std::endl;
    std::cout << "  Puts:    " << stats.num_puts << std::endl;
    std::cout << "  Gets:    " << stats.num_gets << std::endl;
    std::cout << "  Deletes: " << stats.num_deletes << std::endl;
    std::cout << std::endl;
    
    std::cout << "Memory:" << std::endl;
    std::cout << "  MemTable size: " << (stats.memtable_size / 1024) << " KB" << std::endl;
    std::cout << "  Immutable count: " << memtables_->ImmutableCount() << std::endl;
    std::cout << std::endl;
    
    std::cout << "Disk:" << std::endl;
    std::cout << "  Total SSTables: " << stats.num_sstables << std::endl;
    std::cout << "  Total size: " << (stats.total_sstable_size / 1024 / 1024) << " MB" << std::endl;
    std::cout << std::endl;
    
    std::cout << "Levels:" << std::endl;
    for (size_t level = 0; level < stats.level_sizes.size(); level++) {
        size_t count = sstable_manager_->GetSSTables(level).size();
        if (stats.level_sizes[level] > 0 || count > 0) {
            std::cout << "  L" << level << ": " << count << " files, "
                      << (stats.level_sizes[level] / 1024) << " KB" << std::endl;
        }
    }
    
    if (thread_pools_) {
        std::cout << std::endl;
        thread_pools_->PrintStats();
    }
    
    std::cout << "=================================" << std::endl;
}

}  // namespace lsm
