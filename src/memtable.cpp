/**
 * memtable.cpp - Implementation of MemTable and MemTableList
 * 
 * This file implements the in-memory write buffer using a skip list.
 * Key implementation details:
 * - Skip list provides O(log n) operations with lock-free reads
 * - Sequence numbers enable MVCC for concurrent readers
 * - Memory usage tracking triggers flush when threshold reached
 */

#include "memtable.h"
#include <algorithm>

namespace lsm {

// ============================================================================
// MemTable Implementation
// ============================================================================

MemTable::MemTable(uint64_t id) 
    : id_(id)
    , table_() 
{
}

void MemTable::Put(const std::string& key, const std::string& value,
                   SequenceNumber sequence) {
    // Check immutability - should not happen in correct usage
    assert(!IsImmutable());
    
    // Create internal key with value type
    InternalKey internal_key(key, sequence, ValueType::VALUE);
    
    // Insert into skip list
    table_.Insert(internal_key, value);
    
    // Update memory usage estimate
    // Account for: key, value, sequence number, internal overhead
    size_t entry_size = key.size() + value.size() + 
                        sizeof(SequenceNumber) + sizeof(ValueType) + 
                        64;  // Estimated skip list node overhead
    memory_usage_.fetch_add(entry_size, std::memory_order_relaxed);
}

void MemTable::Delete(const std::string& key, SequenceNumber sequence) {
    assert(!IsImmutable());
    
    // Insert a tombstone marker (deletion record)
    // The tombstone has the same key but with DELETION type
    // During reads, finding a tombstone means the key was deleted
    InternalKey internal_key(key, sequence, ValueType::DELETION);
    
    table_.Insert(internal_key, "");  // Empty value for tombstone
    
    // Still accounts for memory
    size_t entry_size = key.size() + sizeof(SequenceNumber) + 
                        sizeof(ValueType) + 64;
    memory_usage_.fetch_add(entry_size, std::memory_order_relaxed);
}

Status MemTable::Get(const std::string& key, SequenceNumber sequence,
                     std::string* value) const {
    // Create a lookup key with the query sequence number
    // We want to find entries with user_key == key and sequence <= query_sequence
    // Since InternalKey comparison orders by sequence descending,
    // we look for the first entry >= (key, sequence)
    
    InternalKey lookup_key(key, sequence, ValueType::VALUE);
    
    auto iter = table_.LowerBound(lookup_key);
    
    if (iter.Valid()) {
        const InternalKey& found_key = iter.GetKey();
        
        // Check if user keys match
        if (found_key.user_key == key) {
            // Found an entry for this key
            // Check if it's visible (sequence <= query sequence)
            if (found_key.sequence <= sequence) {
                // Check if it's a deletion tombstone
                if (found_key.type == ValueType::DELETION) {
                    return Status::NotFound("Key was deleted");
                }
                
                // Found a valid value
                *value = iter.GetValue();
                return Status::OK();
            }
        }
    }
    
    return Status::NotFound();
}

// ============================================================================
// MemTable::Iterator Implementation
// ============================================================================

MemTable::Iterator::Iterator(const MemTable* memtable)
    : iter_(memtable->table_.Begin()) 
{
}

// ============================================================================
// MemTableList Implementation
// ============================================================================

MemTableList::MemTableList() {
    // Create initial mutable MemTable
    mutable_ = std::make_shared<MemTable>(next_id_.fetch_add(1));
}

std::shared_ptr<MemTable> MemTableList::GetMutable() {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    // Create if doesn't exist (shouldn't happen normally)
    if (!mutable_) {
        // Need to upgrade to exclusive lock
        lock.unlock();
        std::unique_lock<std::shared_mutex> write_lock(mutex_);
        if (!mutable_) {
            mutable_ = std::make_shared<MemTable>(
                next_id_.fetch_add(1, std::memory_order_relaxed));
        }
        return mutable_;
    }
    
    return mutable_;
}

std::shared_ptr<MemTable> MemTableList::RotateMemTable() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    if (!mutable_) {
        return nullptr;
    }
    
    // Mark current as immutable
    mutable_->MarkImmutable();
    
    // Move to immutables list
    std::shared_ptr<MemTable> immutable = std::move(mutable_);
    immutables_.push_back(immutable);
    
    // Create new mutable
    mutable_ = std::make_shared<MemTable>(
        next_id_.fetch_add(1, std::memory_order_relaxed));
    
    return immutable;
}

std::vector<std::shared_ptr<MemTable>> MemTableList::GetImmutables() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return immutables_;
}

void MemTableList::RemoveImmutable(uint64_t id) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    // Find and remove the MemTable with matching ID
    auto it = std::remove_if(immutables_.begin(), immutables_.end(),
                             [id](const std::shared_ptr<MemTable>& m) {
                                 return m->GetId() == id;
                             });
    
    if (it != immutables_.end()) {
        immutables_.erase(it, immutables_.end());
    }
}

Status MemTableList::Get(const std::string& key, SequenceNumber sequence,
                         std::string* value) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    // First check mutable MemTable (most recent writes)
    if (mutable_) {
        Status s = mutable_->Get(key, sequence, value);
        if (s.ok() || !s.IsNotFound()) {
            return s;  // Found or error
        }
    }
    
    // Then check immutables in reverse order (newer to older)
    // This ensures we find the most recent version first
    for (auto it = immutables_.rbegin(); it != immutables_.rend(); ++it) {
        Status s = (*it)->Get(key, sequence, value);
        if (s.ok() || !s.IsNotFound()) {
            return s;
        }
    }
    
    return Status::NotFound();
}

size_t MemTableList::TotalMemoryUsage() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    size_t total = 0;
    if (mutable_) {
        total += mutable_->ApproximateMemoryUsage();
    }
    for (const auto& imm : immutables_) {
        total += imm->ApproximateMemoryUsage();
    }
    return total;
}

size_t MemTableList::ImmutableCount() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return immutables_.size();
}

}  // namespace lsm
