/**
 * sstable.h - Sorted String Table for persistent storage
 * 
 * SSTable (Sorted String Table) is the on-disk format for LSM-tree data.
 * Features:
 * - Sorted key-value pairs for efficient range scans
 * - Block-based organization for efficient I/O
 * - Index block for fast key lookup
 * - Bloom filter for negative lookups
 * - Footer with metadata
 * 
 * File format:
 * [Data Block 1][Data Block 2]...[Index Block][Bloom Filter][Footer]
 * 
 * Each data block contains sorted key-value pairs.
 * The index block maps keys to block offsets.
 * The bloom filter quickly identifies keys that don't exist.
 */

#ifndef STORAGE_ENGINE_SSTABLE_H
#define STORAGE_ENGINE_SSTABLE_H

#include "common.h"
#include "memtable.h"
#include <string>
#include <vector>
#include <memory>
#include <fstream>

namespace lsm {

// ============================================================================
// Bloom Filter - Probabilistic set membership
// ============================================================================

/**
 * BloomFilter provides fast negative lookups with no false negatives.
 * If a key is in the set, the filter always returns true.
 * If a key is not in the set, the filter may return true (false positive)
 * with a probability depending on the number of bits per key.
 * 
 * This is useful for avoiding disk reads for keys that definitely don't exist.
 */
class BloomFilter {
public:
    /**
     * Create a bloom filter for a given number of keys
     * @param num_keys Expected number of keys
     * @param bits_per_key Number of bits per key (affects false positive rate)
     */
    BloomFilter(size_t num_keys, size_t bits_per_key = config::BLOOM_FILTER_BITS_PER_KEY);
    
    /**
     * Create a bloom filter from serialized data
     */
    explicit BloomFilter(const std::string& data);
    
    /**
     * Add a key to the filter
     */
    void Add(const std::string& key);
    
    /**
     * Check if a key might be in the set
     * @return true if key may exist, false if key definitely doesn't exist
     */
    bool MayContain(const std::string& key) const;
    
    /**
     * Serialize the filter to a string
     */
    std::string Serialize() const;
    
    /**
     * Get the size of the filter in bytes
     */
    size_t Size() const { return bits_.size(); }

private:
    /**
     * Hash function for bloom filter
     * Uses double hashing to generate k hash values
     */
    uint32_t Hash(const std::string& key, uint32_t seed) const;
    
    std::vector<uint8_t> bits_;
    size_t num_hash_functions_;
    size_t num_bits_;
};

// ============================================================================
// Block - Basic unit of storage in SSTable
// ============================================================================

/**
 * Block contains sorted key-value entries with a restart array.
 * 
 * Format:
 * [entry 1][entry 2]...[entry n][restart offsets][num restarts (4 bytes)]
 * 
 * Each entry:
 * [shared_key_len (varint)][unshared_key_len (varint)][value_len (varint)]
 * [unshared_key_bytes][value_bytes]
 * 
 * Prefix compression: entries share common key prefixes with previous entry.
 * Restart points are entries with no prefix sharing (for binary search).
 */
class BlockBuilder {
public:
    explicit BlockBuilder(size_t restart_interval = 16);
    
    /**
     * Add a key-value pair to the block
     * Keys must be added in sorted order!
     */
    void Add(const InternalKey& key, const std::string& value);
    
    /**
     * Finish building the block and return contents
     */
    std::string Finish();
    
    /**
     * Reset the builder for reuse
     */
    void Reset();
    
    /**
     * Get current estimated size
     */
    size_t EstimatedSize() const { return buffer_.size() + restarts_.size() * 4 + 4; }
    
    /**
     * Check if block is empty
     */
    bool Empty() const { return num_entries_ == 0; }

private:
    std::string buffer_;
    std::vector<uint32_t> restarts_;  // Offsets of restart points
    size_t restart_interval_;
    int counter_;  // Entries since last restart
    size_t num_entries_;
    std::string last_key_;  // For prefix compression
};

/**
 * BlockReader reads entries from a block
 */
class BlockReader {
public:
    explicit BlockReader(const std::string& data);
    
    /**
     * Iterator for reading block entries
     */
    class Iterator {
    public:
        Iterator(const BlockReader* reader, size_t offset);
        
        bool Valid() const { return valid_; }
        void Next();
        
        // Seek to first key >= target
        void Seek(const InternalKey& target);
        // Seek to first entry
        void SeekToFirst();
        
        const InternalKey& GetKey() const { return key_; }
        const std::string& GetValue() const { return value_; }

    private:
        void ParseEntry();
        
        const BlockReader* reader_;
        size_t offset_;
        size_t restart_index_;
        
        InternalKey key_;
        std::string value_;
        bool valid_;
    };
    
    Iterator NewIterator() const;
    
    /**
     * Get number of restart points
     */
    size_t NumRestarts() const { return num_restarts_; }

private:
    friend class Iterator;
    
    std::string data_;
    std::vector<uint32_t> restarts_;
    size_t num_restarts_;
    size_t data_end_;  // End of entry data (before restarts)
};

// ============================================================================
// SSTable Footer - Metadata at end of file
// ============================================================================

/**
 * Footer contains pointers to the index block and bloom filter.
 * Fixed size for easy reading from end of file.
 */
struct SSTableFooter {
    uint64_t index_offset;       // Offset of index block
    uint64_t index_size;         // Size of index block
    uint64_t bloom_offset;       // Offset of bloom filter
    uint64_t bloom_size;         // Size of bloom filter
    uint64_t num_entries;        // Total number of entries
    uint64_t file_size;          // Total file size
    SequenceNumber min_sequence; // Minimum sequence number in table
    SequenceNumber max_sequence; // Maximum sequence number in table
    uint32_t magic;              // Magic number for validation
    uint32_t checksum;           // CRC of footer
    
    static constexpr uint32_t MAGIC = 0x534C534D;  // "SLSM"
    static constexpr size_t SIZE = 8 * 8 + 4 * 2;
    
    /**
     * Encode footer to bytes
     */
    std::string Encode() const;
    
    /**
     * Decode footer from bytes
     */
    static Status Decode(const std::string& data, SSTableFooter* footer);
};

// ============================================================================
// SSTableBuilder - Creates SSTable files
// ============================================================================

/**
 * SSTableBuilder writes an SSTable file from sorted key-value pairs.
 * 
 * Usage:
 *   SSTableBuilder builder(filename);
 *   builder.Open();
 *   for each (key, value):
 *     builder.Add(key, value);  // Must be sorted!
 *   builder.Finish();
 */
class SSTableBuilder {
public:
    /**
     * Create a builder for a new SSTable file
     * @param filename Path to the SSTable file
     * @param file_number Unique identifier for this SSTable
     * @param level LSM tree level for this SSTable
     */
    SSTableBuilder(const std::string& filename, FileNumber file_number, int level);
    ~SSTableBuilder();
    
    SSTableBuilder(const SSTableBuilder&) = delete;
    SSTableBuilder& operator=(const SSTableBuilder&) = delete;
    
    /**
     * Open the file for writing
     */
    Status Open();
    
    /**
     * Add a key-value pair (must be in sorted order!)
     */
    Status Add(const InternalKey& key, const std::string& value);
    
    /**
     * Finish writing the SSTable
     */
    Status Finish();
    
    /**
     * Abandon the SSTable (delete partially written file)
     */
    void Abandon();
    
    /**
     * Get file number
     */
    FileNumber GetFileNumber() const { return file_number_; }
    
    /**
     * Get number of entries added
     */
    size_t GetNumEntries() const { return num_entries_; }
    
    /**
     * Get current file size
     */
    size_t GetFileSize() const { return offset_; }

private:
    /**
     * Write a block to the file
     */
    Status WriteBlock(const std::string& data, uint64_t* offset, uint64_t* size);
    
    /**
     * Flush current data block if needed
     */
    Status FlushDataBlock();
    
    std::string filename_;
    FileNumber file_number_;
    int level_;
    int fd_;
    
    BlockBuilder data_block_;
    std::vector<std::pair<std::string, uint64_t>> index_entries_;  // last_key -> offset
    std::unique_ptr<BloomFilter> bloom_filter_;
    
    size_t offset_;
    size_t num_entries_;
    
    std::string smallest_key_;
    std::string largest_key_;
    SequenceNumber min_sequence_;
    SequenceNumber max_sequence_;
    
    bool finished_;
    bool abandoned_;
};

// ============================================================================
// SSTableReader - Reads from SSTable files
// ============================================================================

/**
 * SSTableReader provides read access to an SSTable file.
 * 
 * Features:
 * - Point lookups with bloom filter optimization
 * - Range iteration
 * - Caching of index and bloom filter
 */
class SSTableReader {
public:
    /**
     * Create a reader for an SSTable file
     */
    explicit SSTableReader(const std::string& filename);
    ~SSTableReader();
    
    SSTableReader(const SSTableReader&) = delete;
    SSTableReader& operator=(const SSTableReader&) = delete;
    
    /**
     * Open the SSTable file
     */
    Status Open();
    
    /**
     * Close the SSTable file
     */
    Status Close();
    
    /**
     * Get a value for a key
     * @param key Key to look up
     * @param value Output parameter
     * @return Status::OK if found, Status::NotFound if not found
     */
    Status Get(const InternalKey& key, std::string* value) const;
    
    /**
     * Check if a key may exist (using bloom filter)
     */
    bool MayContain(const std::string& user_key) const;
    
    /**
     * Iterator for scanning the SSTable
     */
    class Iterator {
    public:
        explicit Iterator(const SSTableReader* reader);
        ~Iterator();
        
        // Move constructor and assignment (enable make_unique)
        Iterator(Iterator&& other) noexcept;
        Iterator& operator=(Iterator&& other) noexcept;
        
        // Disable copy
        Iterator(const Iterator&) = delete;
        Iterator& operator=(const Iterator&) = delete;
        
        bool Valid() const { return valid_; }
        void Next();
        void SeekToFirst();
        void Seek(const InternalKey& target);
        
        const InternalKey& GetKey() const { return key_; }
        const std::string& GetValue() const { return value_; }

    private:
        void LoadBlock(size_t block_index);
        
        const SSTableReader* reader_;
        size_t current_block_;
        std::unique_ptr<BlockReader> block_reader_;
        std::unique_ptr<BlockReader::Iterator> block_iter_;
        
        InternalKey key_;
        std::string value_;
        bool valid_;
    };
    
    /**
     * Create an iterator over this SSTable
     */
    Iterator NewIterator() const;
    
    /**
     * Get file number
     */
    FileNumber GetFileNumber() const { return file_number_; }
    
    /**
     * Get filename
     */
    const std::string& GetFilename() const { return filename_; }
    
    /**
     * Get number of entries
     */
    size_t GetNumEntries() const { return footer_.num_entries; }
    
    /**
     * Get file size
     */
    size_t GetFileSize() const { return footer_.file_size; }
    
    /**
     * Get sequence number range
     */
    SequenceNumber GetMinSequence() const { return footer_.min_sequence; }
    SequenceNumber GetMaxSequence() const { return footer_.max_sequence; }
    
    /**
     * Get smallest key in the SSTable
     */
    const std::string& GetSmallestKey() const { return smallest_key_; }
    
    /**
     * Get largest key in the SSTable
     */
    const std::string& GetLargestKey() const { return largest_key_; }

private:
    friend class Iterator;
    
    /**
     * Read a block from the file
     */
    Status ReadBlock(uint64_t offset, uint64_t size, std::string* data) const;
    
    /**
     * Parse index block to build index
     */
    Status BuildIndex();
    
    std::string filename_;
    FileNumber file_number_;
    int fd_;
    
    SSTableFooter footer_;
    std::unique_ptr<BloomFilter> bloom_filter_;
    
    // Index: maps last key of each block to (offset, size)
    struct BlockHandle {
        uint64_t offset;
        uint64_t size;
    };
    std::vector<std::pair<std::string, BlockHandle>> block_index_;
    
    std::string smallest_key_;
    std::string largest_key_;
    
    bool opened_;
};

// ============================================================================
// SSTableManager - Manages all SSTable files
// ============================================================================

/**
 * SSTableMetadata contains information about an SSTable file.
 * Used for tracking SSTables across levels.
 */
struct SSTableMetadata {
    FileNumber file_number;
    int level;
    std::string filename;
    size_t file_size;
    size_t num_entries;
    std::string smallest_key;
    std::string largest_key;
    SequenceNumber min_sequence;
    SequenceNumber max_sequence;
    
    // Reference count for safe deletion during compaction
    std::atomic<int> refs{1};
};

/**
 * SSTableManager tracks all SSTables in the system.
 * Manages:
 * - SSTable file lifecycle
 * - Level organization
 * - Compaction selection
 */
class SSTableManager {
public:
    explicit SSTableManager(const std::string& db_path);
    ~SSTableManager();
    
    SSTableManager(const SSTableManager&) = delete;
    SSTableManager& operator=(const SSTableManager&) = delete;
    
    /**
     * Initialize - scan directory for existing SSTables
     */
    Status Init();
    
    /**
     * Create a new SSTable from a MemTable
     * @param memtable The MemTable to flush
     * @param level Target level (usually 0)
     * @return Metadata for the new SSTable
     */
    std::shared_ptr<SSTableMetadata> CreateFromMemTable(
        const MemTable& memtable, int level = 0);
    
    /**
     * Add an existing SSTable to tracking
     */
    void AddSSTable(std::shared_ptr<SSTableMetadata> meta);
    
    /**
     * Remove an SSTable (after compaction)
     */
    Status RemoveSSTable(FileNumber file_number);
    
    /**
     * Get all SSTables at a level
     */
    std::vector<std::shared_ptr<SSTableMetadata>> GetSSTables(int level) const;
    
    /**
     * Get total number of SSTables across all levels
     */
    size_t GetTotalSSTables() const;
    
    /**
     * Get size of a level in bytes
     */
    size_t GetLevelSize(int level) const;
    
    /**
     * Open an SSTable for reading
     */
    std::shared_ptr<SSTableReader> OpenSSTable(FileNumber file_number) const;
    
    /**
     * Get value from SSTables (searches all levels)
     * @param key Key to look up
     * @param sequence Maximum sequence number to consider
     * @param value Output parameter
     */
    Status Get(const std::string& key, SequenceNumber sequence,
               std::string* value) const;
    
    /**
     * Get next file number for new SSTable
     */
    FileNumber AllocateFileNumber();

private:
    std::string db_path_;
    
    mutable std::shared_mutex mutex_;
    
    // SSTables organized by level
    std::vector<std::vector<std::shared_ptr<SSTableMetadata>>> levels_;
    
    // Map from file number to metadata
    std::unordered_map<FileNumber, std::shared_ptr<SSTableMetadata>> sstables_;
    
    // Cache of open SSTable readers
    mutable std::unordered_map<FileNumber, std::shared_ptr<SSTableReader>> reader_cache_;
    
    std::atomic<FileNumber> next_file_number_{1};
};

}  // namespace lsm

#endif  // STORAGE_ENGINE_SSTABLE_H
