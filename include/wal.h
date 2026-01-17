/**
 * wal.h - Write-Ahead Log for durability
 * 
 * The Write-Ahead Log (WAL) ensures durability by persisting writes to disk
 * before they are acknowledged. This allows recovery after crashes:
 * - On startup, replay WAL to rebuild MemTable
 * - After flush, delete corresponding WAL
 * 
 * Record format:
 * [checksum (4 bytes)][length (4 bytes)][type (1 byte)][data (length bytes)]
 * 
 * The WAL uses buffered I/O with periodic syncs to balance durability and
 * performance. Critical writes can request immediate fsync.
 */

#ifndef STORAGE_ENGINE_WAL_H
#define STORAGE_ENGINE_WAL_H

#include "common.h"
#include <string>
#include <mutex>
#include <fstream>
#include <vector>
#include <memory>

namespace lsm {

// ============================================================================
// WAL Record Types
// ============================================================================

enum class WALRecordType : uint8_t {
    // Full record - entire data fits in one record
    FULL = 0,
    
    // First fragment of a split record
    FIRST = 1,
    
    // Middle fragment of a split record
    MIDDLE = 2,
    
    // Last fragment of a split record
    LAST = 3,
    
    // Zero padding at end of block
    ZERO = 4
};

// ============================================================================
// WAL Record Header
// ============================================================================

/**
 * WALRecordHeader is the binary header for each WAL record
 * Total size: 9 bytes
 */
struct WALRecordHeader {
    uint32_t checksum;   // CRC32 of type + data
    uint32_t length;     // Length of data portion
    uint8_t type;        // WALRecordType
    
    static constexpr size_t SIZE = sizeof(checksum) + sizeof(length) + sizeof(type);
};

// Block size for WAL - records don't span blocks to simplify recovery
constexpr size_t WAL_BLOCK_SIZE = 32 * 1024;  // 32KB blocks

// ============================================================================
// WAL Entry - Logical unit written to WAL
// ============================================================================

/**
 * WALEntry represents a single operation to be logged
 * Each write or delete becomes one WAL entry
 */
struct WALEntry {
    enum class OpType : uint8_t {
        PUT = 0,
        DELETE = 1
    };
    
    OpType op;
    std::string key;
    std::string value;  // Empty for DELETE
    SequenceNumber sequence;
    
    /**
     * Encode entry to binary format
     * Format: [op (1)][seq (8)][key_len (4)][key][value_len (4)][value]
     */
    std::string Encode() const {
        std::string result;
        
        // Reserve approximate size
        result.reserve(1 + 8 + 4 + key.size() + 4 + value.size());
        
        // Operation type
        result.push_back(static_cast<char>(op));
        
        // Sequence number
        result.append(reinterpret_cast<const char*>(&sequence), sizeof(sequence));
        
        // Key with length prefix
        uint32_t key_len = static_cast<uint32_t>(key.size());
        result.append(reinterpret_cast<const char*>(&key_len), sizeof(key_len));
        result.append(key);
        
        // Value with length prefix
        uint32_t value_len = static_cast<uint32_t>(value.size());
        result.append(reinterpret_cast<const char*>(&value_len), sizeof(value_len));
        result.append(value);
        
        return result;
    }
    
    /**
     * Decode entry from binary format
     */
    static Status Decode(const std::string& data, WALEntry* entry) {
        if (data.size() < 1 + 8 + 4 + 4) {
            return Status::Corruption("WAL entry too short");
        }
        
        const char* p = data.data();
        
        // Operation type
        entry->op = static_cast<OpType>(*p++);
        
        // Sequence number
        memcpy(&entry->sequence, p, sizeof(entry->sequence));
        p += sizeof(entry->sequence);
        
        // Key
        uint32_t key_len;
        memcpy(&key_len, p, sizeof(key_len));
        p += sizeof(key_len);
        
        if (p + key_len > data.data() + data.size()) {
            return Status::Corruption("WAL entry key truncated");
        }
        entry->key.assign(p, key_len);
        p += key_len;
        
        // Value
        uint32_t value_len;
        memcpy(&value_len, p, sizeof(value_len));
        p += sizeof(value_len);
        
        if (p + value_len > data.data() + data.size()) {
            return Status::Corruption("WAL entry value truncated");
        }
        entry->value.assign(p, value_len);
        
        return Status::OK();
    }
};

// ============================================================================
// WALWriter - Writes entries to the WAL file
// ============================================================================

/**
 * WALWriter manages writing to a single WAL file.
 * 
 * Features:
 * - Buffered writes with configurable sync policy
 * - CRC32 checksums for corruption detection
 * - Block-aligned records for simpler recovery
 */
class WALWriter {
public:
    /**
     * Create a WAL writer for the specified file
     * @param filename Path to the WAL file
     * @param file_number Unique identifier for this WAL
     */
    WALWriter(const std::string& filename, FileNumber file_number);
    ~WALWriter();
    
    // Non-copyable
    WALWriter(const WALWriter&) = delete;
    WALWriter& operator=(const WALWriter&) = delete;
    
    /**
     * Open the WAL file for writing
     */
    Status Open();
    
    /**
     * Append an entry to the WAL
     * @param entry The entry to write
     * @param sync If true, fsync after write
     */
    Status Append(const WALEntry& entry, bool sync = false);
    
    /**
     * Sync buffered data to disk
     */
    Status Sync();
    
    /**
     * Close the WAL file
     */
    Status Close();
    
    /**
     * Get the file number
     */
    FileNumber GetFileNumber() const { return file_number_; }
    
    /**
     * Get current file size
     */
    size_t GetFileSize() const { return file_size_; }
    
    /**
     * Get filename
     */
    const std::string& GetFilename() const { return filename_; }

private:
    /**
     * Write a physical record to the WAL
     * Handles block boundary splitting
     */
    Status WriteRecord(const std::string& data);
    
    /**
     * Flush the write buffer to the file
     */
    Status FlushBuffer();
    
    std::string filename_;
    FileNumber file_number_;
    
    int fd_;  // File descriptor (using POSIX I/O for control)
    
    // Write buffer
    std::vector<char> buffer_;
    size_t buffer_pos_;
    
    // Position within current block
    size_t block_offset_;
    
    // Total file size
    size_t file_size_;
    
    // Mutex for thread safety
    std::mutex mutex_;
    
    bool closed_;
};

// ============================================================================
// WALReader - Reads entries from a WAL file for recovery
// ============================================================================

/**
 * WALReader reads WAL entries for crash recovery.
 * 
 * On startup, the storage engine:
 * 1. Finds all WAL files
 * 2. Reads entries from each in sequence order
 * 3. Replays entries to rebuild MemTable
 */
class WALReader {
public:
    explicit WALReader(const std::string& filename);
    ~WALReader();
    
    WALReader(const WALReader&) = delete;
    WALReader& operator=(const WALReader&) = delete;
    
    /**
     * Open the WAL file for reading
     */
    Status Open();
    
    /**
     * Read the next entry from the WAL
     * @param entry Output parameter for the entry
     * @return Status::OK if entry read, Status::NotFound if EOF
     */
    Status ReadEntry(WALEntry* entry);
    
    /**
     * Check if there are more entries to read
     */
    bool HasMore() const { return !eof_; }
    
    /**
     * Close the WAL file
     */
    Status Close();
    
    /**
     * Get count of corrupted records encountered
     */
    size_t GetCorruptedCount() const { return corrupted_count_; }

private:
    /**
     * Read a physical record from the WAL
     * Handles reassembly of fragmented records
     */
    Status ReadRecord(std::string* data);
    
    /**
     * Read more data into the buffer
     */
    Status ReadMore();
    
    std::string filename_;
    int fd_;
    
    // Read buffer
    std::vector<char> buffer_;
    size_t buffer_pos_;
    size_t buffer_end_;
    
    // Current block info
    size_t block_offset_;
    
    bool eof_;
    size_t corrupted_count_;
};

// ============================================================================
// WALManager - Manages multiple WAL files
// ============================================================================

/**
 * WALManager handles WAL lifecycle:
 * - Creating new WAL files
 * - Tracking active WALs
 * - Deleting WALs after flush
 * - Recovery on startup
 */
class WALManager {
public:
    /**
     * Create a WAL manager for the specified directory
     */
    explicit WALManager(const std::string& db_path);
    ~WALManager();
    
    WALManager(const WALManager&) = delete;
    WALManager& operator=(const WALManager&) = delete;
    
    /**
     * Initialize - scan directory for existing WALs
     */
    Status Init();
    
    /**
     * Create a new WAL file
     * @return Shared pointer to the new WAL writer
     */
    std::shared_ptr<WALWriter> CreateWAL();
    
    /**
     * Mark a WAL as obsolete (MemTable has been flushed)
     * The WAL file will be deleted
     */
    Status ObsoleteWAL(FileNumber file_number);
    
    /**
     * Get list of WAL files for recovery (sorted by file number)
     */
    std::vector<std::string> GetWALFiles() const;
    
    /**
     * Recover from WAL files
     * @param callback Called for each entry to replay
     * @return Status and the highest sequence number seen
     */
    std::pair<Status, SequenceNumber> Recover(
        std::function<Status(const WALEntry&)> callback);
    
    /**
     * Delete all WAL files (after successful recovery)
     */
    Status DeleteAllWALs();

private:
    std::string db_path_;
    
    std::mutex mutex_;
    std::atomic<FileNumber> next_file_number_{1};
    
    // Currently active WAL files (not yet flushed)
    std::vector<std::shared_ptr<WALWriter>> active_wals_;
};

}  // namespace lsm

#endif  // STORAGE_ENGINE_WAL_H
