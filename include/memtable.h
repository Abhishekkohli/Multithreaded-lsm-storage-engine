/**
 * memtable.h - In-memory write buffer using Skip List
 * 
 * The MemTable is an in-memory sorted structure that buffers writes before
 * they are flushed to disk as SSTables. This implementation uses a skip list
 * which provides:
 * - O(log n) insert, lookup, and delete
 * - Lock-free reads with fine-grained locking for writes
 * - Natural iteration in sorted order
 * 
 * The skip list is a probabilistic data structure that uses multiple levels
 * of linked lists. Higher levels "skip" over elements, providing fast traversal.
 */

#ifndef STORAGE_ENGINE_MEMTABLE_H
#define STORAGE_ENGINE_MEMTABLE_H

#include "common.h"
#include <random>
#include <atomic>
#include <mutex>
#include <shared_mutex>
#include <vector>
#include <cstdlib>

namespace lsm {

// ============================================================================
// Skip List Node
// ============================================================================

/**
 * SkipListNode represents a single node in the skip list.
 * Each node has an array of forward pointers, one for each level it participates in.
 * 
 * Memory layout:
 * [SkipListNode header][forward pointers array]
 * 
 * We use atomics for forward pointers to enable lock-free reads.
 */
template<typename Key, typename Value>
struct SkipListNode {
    Key key;
    Value value;
    int height;  // Number of levels this node participates in
    
    // Forward pointers are stored inline after the node
    // Using atomic pointers allows lock-free reads while writes hold a lock
    std::atomic<SkipListNode*> forward[1];  // Variable-length array
    
    /**
     * Allocate a new node with specified height
     * Height determines how many levels of forward pointers the node has
     */
    static SkipListNode* NewNode(const Key& k, const Value& v, int height) {
        // Calculate total size: base struct + extra forward pointers
        // We already have 1 forward pointer in the struct, so allocate (height-1) more
        size_t size = sizeof(SkipListNode) + 
                      sizeof(std::atomic<SkipListNode*>) * (height - 1);
        
        // Use posix_memalign for portable aligned allocation
        // Alignment must be power of 2 and >= sizeof(void*)
        size_t alignment = alignof(SkipListNode);
        if (alignment < sizeof(void*)) alignment = sizeof(void*);
        
        void* mem = nullptr;
        if (posix_memalign(&mem, alignment, size) != 0 || !mem) {
            return nullptr;
        }
        
        // Construct node in place
        SkipListNode* node = new (mem) SkipListNode();
        node->key = k;
        node->value = v;
        node->height = height;
        
        // Initialize all forward pointers to nullptr
        for (int i = 0; i < height; i++) {
            node->forward[i].store(nullptr, std::memory_order_relaxed);
        }
        
        return node;
    }
    
    /**
     * Get forward pointer at level i
     * Uses acquire semantics to synchronize with concurrent writes
     */
    SkipListNode* GetNext(int level) const {
        return forward[level].load(std::memory_order_acquire);
    }
    
    /**
     * Set forward pointer at level i
     * Uses release semantics to publish updates to concurrent readers
     */
    void SetNext(int level, SkipListNode* node) {
        forward[level].store(node, std::memory_order_release);
    }
    
    /**
     * Free node memory
     */
    static void DeleteNode(SkipListNode* node) {
        if (node) {
            node->~SkipListNode();
            free(node);
        }
    }

private:
    SkipListNode() = default;
};

// ============================================================================
// Skip List Implementation
// ============================================================================

/**
 * SkipList - A probabilistic sorted data structure
 * 
 * The skip list maintains multiple levels of linked lists. The bottom level
 * contains all elements. Higher levels contain a subset of elements, chosen
 * probabilistically. This allows O(log n) search by "skipping" over elements.
 * 
 * Concurrency model:
 * - Reads are lock-free using atomic forward pointers
 * - Writes acquire a mutex (single-writer)
 * - This asymmetric model works well for read-heavy workloads
 */
template<typename Key, typename Value, typename Comparator = std::less<Key>>
class SkipList {
public:
    using Node = SkipListNode<Key, Value>;
    
    /**
     * Create a skip list with specified comparator
     */
    explicit SkipList(int max_height = config::SKIPLIST_MAX_HEIGHT,
                      float probability = config::SKIPLIST_PROBABILITY,
                      Comparator cmp = Comparator())
        : max_height_(max_height)
        , probability_(probability)
        , comparator_(cmp)
        , current_height_(1)
        , size_(0)
        , rng_(std::random_device{}()) 
    {
        // Create sentinel head node with max height
        // The head is never deleted and allows uniform treatment of insertions
        head_ = Node::NewNode(Key{}, Value{}, max_height);
    }
    
    ~SkipList() {
        // Delete all nodes
        Node* current = head_;
        while (current) {
            Node* next = current->GetNext(0);
            Node::DeleteNode(current);
            current = next;
        }
    }
    
    // Non-copyable
    SkipList(const SkipList&) = delete;
    SkipList& operator=(const SkipList&) = delete;
    
    /**
     * Insert a key-value pair
     * If key already exists, the value is updated
     * @return true if new insertion, false if update
     */
    bool Insert(const Key& key, const Value& value) {
        std::lock_guard<std::mutex> lock(write_mutex_);
        
        // Vector to track predecessors at each level
        // These are the nodes whose forward pointers need updating
        std::vector<Node*> predecessors(max_height_);
        Node* current = head_;
        
        // Search for insertion point, recording predecessors
        // Start from the highest level for maximum skipping
        for (int level = current_height_ - 1; level >= 0; level--) {
            while (true) {
                Node* next = current->GetNext(level);
                if (next && comparator_(next->key, key)) {
                    current = next;  // Keep going right
                } else {
                    break;  // Go down to next level
                }
            }
            predecessors[level] = current;
        }
        
        // Check if key already exists
        Node* existing = current->GetNext(0);
        if (existing && !comparator_(key, existing->key) && 
            !comparator_(existing->key, key)) {
            // Key exists - update value
            existing->value = value;
            return false;
        }
        
        // Generate random height for new node
        int new_height = RandomHeight();
        
        // If new height exceeds current height, update predecessors
        if (new_height > current_height_) {
            for (int level = current_height_; level < new_height; level++) {
                predecessors[level] = head_;
            }
            current_height_ = new_height;
        }
        
        // Create new node
        Node* new_node = Node::NewNode(key, value, new_height);
        
        // Link new node into all levels it participates in
        // The release store in SetNext synchronizes with acquire loads in readers
        for (int level = 0; level < new_height; level++) {
            new_node->SetNext(level, predecessors[level]->GetNext(level));
            predecessors[level]->SetNext(level, new_node);
        }
        
        size_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }
    
    /**
     * Look up a key
     * @return optional containing value if found, empty otherwise
     */
    std::optional<Value> Get(const Key& key) const {
        // Lock-free read - no mutex needed
        Node* current = head_;
        
        // Search from top level down
        for (int level = current_height_ - 1; level >= 0; level--) {
            while (true) {
                Node* next = current->GetNext(level);
                if (next && comparator_(next->key, key)) {
                    current = next;
                } else {
                    break;
                }
            }
        }
        
        // Check if we found the key
        Node* result = current->GetNext(0);
        if (result && !comparator_(key, result->key) && 
            !comparator_(result->key, key)) {
            return result->value;
        }
        
        return std::nullopt;
    }
    
    /**
     * Check if key exists
     */
    bool Contains(const Key& key) const {
        return Get(key).has_value();
    }
    
    /**
     * Get number of elements
     */
    size_t Size() const {
        return size_.load(std::memory_order_relaxed);
    }
    
    bool Empty() const {
        return Size() == 0;
    }
    
    /**
     * Iterator for traversing the skip list in sorted order
     */
    class Iterator {
    public:
        Iterator(Node* node) : current_(node) {}
        
        bool Valid() const { return current_ != nullptr; }
        
        void Next() {
            if (current_) {
                current_ = current_->GetNext(0);
            }
        }
        
        const Key& GetKey() const { return current_->key; }
        const Value& GetValue() const { return current_->value; }
        
        bool operator==(const Iterator& other) const {
            return current_ == other.current_;
        }
        
        bool operator!=(const Iterator& other) const {
            return current_ != other.current_;
        }
        
    private:
        Node* current_;
    };
    
    /**
     * Get iterator to first element
     */
    Iterator Begin() const {
        return Iterator(head_->GetNext(0));
    }
    
    /**
     * Get iterator past the end
     */
    Iterator End() const {
        return Iterator(nullptr);
    }
    
    /**
     * Find first element >= key
     */
    Iterator LowerBound(const Key& key) const {
        Node* current = head_;
        
        for (int level = current_height_ - 1; level >= 0; level--) {
            while (true) {
                Node* next = current->GetNext(level);
                if (next && comparator_(next->key, key)) {
                    current = next;
                } else {
                    break;
                }
            }
        }
        
        return Iterator(current->GetNext(0));
    }

private:
    /**
     * Generate a random height for a new node
     * Uses geometric distribution with parameter p
     * Expected height = 1 / (1 - p)
     */
    int RandomHeight() {
        int height = 1;
        // Each level has probability p of being added
        // This gives O(log n) expected height
        while (height < max_height_ && 
               std::uniform_real_distribution<float>(0, 1)(rng_) < probability_) {
            height++;
        }
        return height;
    }
    
    const int max_height_;
    const float probability_;
    Comparator comparator_;
    
    Node* head_;
    int current_height_;
    std::atomic<size_t> size_;
    
    // Mutex for writes - ensures single-writer consistency
    std::mutex write_mutex_;
    
    // Random number generator for height selection
    // Thread-local would be better for high-contention writes
    std::mt19937 rng_;
};

// ============================================================================
// MemTable - In-memory sorted buffer for writes
// ============================================================================

/**
 * MemTable wraps a skip list with LSM-specific functionality:
 * - Sequence number tracking for MVCC
 * - Size tracking for flush triggers
 * - Immutable conversion for background flushing
 */
class MemTable {
public:
    /**
     * Create a new MemTable
     * @param id Unique identifier for this MemTable
     */
    explicit MemTable(uint64_t id = 0);
    ~MemTable() = default;
    
    // Non-copyable
    MemTable(const MemTable&) = delete;
    MemTable& operator=(const MemTable&) = delete;
    
    /**
     * Put a key-value pair into the MemTable
     * @param key User key
     * @param value Value to store
     * @param sequence Sequence number for versioning
     */
    void Put(const std::string& key, const std::string& value, 
             SequenceNumber sequence);
    
    /**
     * Mark a key as deleted (insert tombstone)
     * @param key User key to delete
     * @param sequence Sequence number for versioning
     */
    void Delete(const std::string& key, SequenceNumber sequence);
    
    /**
     * Get a value for a key
     * @param key User key to look up
     * @param sequence Maximum sequence number to consider
     * @param value Output parameter for the value
     * @return Status::OK if found, Status::NotFound if not found or deleted
     */
    Status Get(const std::string& key, SequenceNumber sequence,
               std::string* value) const;
    
    /**
     * Get approximate memory usage in bytes
     */
    size_t ApproximateMemoryUsage() const {
        return memory_usage_.load(std::memory_order_relaxed);
    }
    
    /**
     * Get number of entries
     */
    size_t Size() const {
        return table_.Size();
    }
    
    /**
     * Check if MemTable should be flushed
     */
    bool ShouldFlush() const {
        return ApproximateMemoryUsage() >= config::MEMTABLE_SIZE_LIMIT;
    }
    
    /**
     * Mark this MemTable as immutable (no more writes allowed)
     */
    void MarkImmutable() {
        immutable_.store(true, std::memory_order_release);
    }
    
    /**
     * Check if MemTable is immutable
     */
    bool IsImmutable() const {
        return immutable_.load(std::memory_order_acquire);
    }
    
    /**
     * Get MemTable ID
     */
    uint64_t GetId() const { return id_; }
    
    /**
     * Iterator for scanning MemTable contents
     */
    class Iterator {
    public:
        explicit Iterator(const MemTable* memtable);
        
        bool Valid() const { return iter_.Valid(); }
        void Next() { iter_.Next(); }
        
        const InternalKey& GetKey() const { return iter_.GetKey(); }
        const std::string& GetValue() const { return iter_.GetValue(); }
        
    private:
        SkipList<InternalKey, std::string>::Iterator iter_;
    };
    
    /**
     * Create an iterator over this MemTable
     */
    Iterator NewIterator() const {
        return Iterator(this);
    }
    
    /**
     * Get underlying skip list for iteration
     */
    const SkipList<InternalKey, std::string>& GetSkipList() const {
        return table_;
    }

private:
    uint64_t id_;
    SkipList<InternalKey, std::string> table_;
    std::atomic<size_t> memory_usage_{0};
    std::atomic<bool> immutable_{false};
};

// ============================================================================
// MemTableList - Manages active and immutable MemTables
// ============================================================================

/**
 * MemTableList manages the set of MemTables in the system:
 * - One active (mutable) MemTable receiving writes
 * - Zero or more immutable MemTables waiting to be flushed
 * 
 * Thread-safety:
 * - Uses a reader-writer lock for concurrent access
 * - Multiple readers can access simultaneously
 * - Writers (rotation) acquire exclusive access
 */
class MemTableList {
public:
    MemTableList();
    ~MemTableList() = default;
    
    /**
     * Get the active (mutable) MemTable
     * Creates a new one if none exists
     */
    std::shared_ptr<MemTable> GetMutable();
    
    /**
     * Rotate MemTables - make current active immutable and create new active
     * @return The newly immutable MemTable (ready for flushing)
     */
    std::shared_ptr<MemTable> RotateMemTable();
    
    /**
     * Get all immutable MemTables (for flushing or reads)
     */
    std::vector<std::shared_ptr<MemTable>> GetImmutables() const;
    
    /**
     * Remove an immutable MemTable after successful flush
     */
    void RemoveImmutable(uint64_t id);
    
    /**
     * Get value from all MemTables (mutable and immutable)
     * Checks newer MemTables first
     */
    Status Get(const std::string& key, SequenceNumber sequence,
               std::string* value) const;
    
    /**
     * Get total memory usage across all MemTables
     */
    size_t TotalMemoryUsage() const;
    
    /**
     * Get number of immutable MemTables pending flush
     */
    size_t ImmutableCount() const;

private:
    mutable std::shared_mutex mutex_;  // Protects all member variables
    
    std::shared_ptr<MemTable> mutable_;  // Current active MemTable
    std::vector<std::shared_ptr<MemTable>> immutables_;  // Waiting for flush
    
    std::atomic<uint64_t> next_id_{1};  // ID generator for MemTables
};

}  // namespace lsm

#endif  // STORAGE_ENGINE_MEMTABLE_H
