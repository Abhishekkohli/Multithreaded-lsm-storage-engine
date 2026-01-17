/**
 * common.h - Common types, constants, and utilities for the storage engine
 * 
 * This header provides foundational types and utilities used across all
 * components of the log-structured storage engine.
 */

#ifndef STORAGE_ENGINE_COMMON_H
#define STORAGE_ENGINE_COMMON_H

#include <cstdint>
#include <cstddef>
#include <string>
#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <atomic>
#include <cassert>
#include <thread>
#include <cstring>

namespace lsm {

// ============================================================================
// Type Aliases
// ============================================================================

using Timestamp = uint64_t;
using SequenceNumber = uint64_t;
using FileNumber = uint64_t;

// ============================================================================
// Constants - Tunable parameters for the storage engine
// ============================================================================

namespace config {
    // MemTable configuration
    constexpr size_t MEMTABLE_SIZE_LIMIT = 4 * 1024 * 1024;  // 4MB before flush
    constexpr size_t SKIPLIST_MAX_HEIGHT = 12;                // Max skip list levels
    constexpr float SKIPLIST_PROBABILITY = 0.25f;             // Level promotion probability
    
    // SSTable configuration
    constexpr size_t BLOCK_SIZE = 4 * 1024;                   // 4KB block size
    constexpr size_t BLOOM_FILTER_BITS_PER_KEY = 10;          // Bloom filter sizing
    
    // WAL configuration
    constexpr size_t WAL_BUFFER_SIZE = 64 * 1024;             // 64KB write buffer
    constexpr size_t WAL_SYNC_INTERVAL_MS = 100;              // Sync interval
    
    // Thread pool configuration
    constexpr size_t MIN_THREADS = 2;
    constexpr size_t MAX_THREADS = 16;
    constexpr size_t FLUSH_THREADS = 2;
    constexpr size_t COMPACTION_THREADS = 2;
    
    // Compaction configuration
    constexpr size_t L0_COMPACTION_TRIGGER = 4;               // Files before L0 compaction
    constexpr size_t MAX_LEVELS = 7;                          // LSM tree depth
    constexpr size_t LEVEL_SIZE_MULTIPLIER = 10;              // Size ratio between levels
    constexpr size_t L1_TARGET_SIZE = 10 * 1024 * 1024;       // 10MB target for L1
}

// ============================================================================
// Result type for operations that can fail
// ============================================================================

enum class StatusCode {
    OK = 0,
    NOT_FOUND,
    CORRUPTION,
    NOT_SUPPORTED,
    INVALID_ARGUMENT,
    IO_ERROR,
    BUSY,
    SHUTDOWN
};

class Status {
public:
    Status() : code_(StatusCode::OK) {}
    explicit Status(StatusCode code) : code_(code) {}
    Status(StatusCode code, const std::string& msg) : code_(code), message_(msg) {}
    
    static Status OK() { return Status(); }
    static Status NotFound(const std::string& msg = "") { 
        return Status(StatusCode::NOT_FOUND, msg); 
    }
    static Status Corruption(const std::string& msg = "") { 
        return Status(StatusCode::CORRUPTION, msg); 
    }
    static Status IOError(const std::string& msg = "") { 
        return Status(StatusCode::IO_ERROR, msg); 
    }
    static Status InvalidArgument(const std::string& msg = "") { 
        return Status(StatusCode::INVALID_ARGUMENT, msg); 
    }
    static Status Busy(const std::string& msg = "") {
        return Status(StatusCode::BUSY, msg);
    }
    static Status Shutdown(const std::string& msg = "") {
        return Status(StatusCode::SHUTDOWN, msg);
    }
    static Status NotSupported(const std::string& msg = "") {
        return Status(StatusCode::NOT_SUPPORTED, msg);
    }
    
    bool ok() const { return code_ == StatusCode::OK; }
    bool IsNotFound() const { return code_ == StatusCode::NOT_FOUND; }
    bool IsCorruption() const { return code_ == StatusCode::CORRUPTION; }
    bool IsIOError() const { return code_ == StatusCode::IO_ERROR; }
    bool IsShutdown() const { return code_ == StatusCode::SHUTDOWN; }
    
    StatusCode code() const { return code_; }
    const std::string& message() const { return message_; }
    
    std::string ToString() const {
        static const char* names[] = {
            "OK", "NotFound", "Corruption", "NotSupported",
            "InvalidArgument", "IOError", "Busy", "Shutdown"
        };
        std::string result = names[static_cast<int>(code_)];
        if (!message_.empty()) {
            result += ": " + message_;
        }
        return result;
    }

private:
    StatusCode code_;
    std::string message_;
};

// ============================================================================
// Key-Value entry with metadata
// ============================================================================

enum class ValueType : uint8_t {
    VALUE = 0,      // Normal key-value pair
    DELETION = 1    // Tombstone marker for deleted keys
};

/**
 * InternalKey wraps a user key with sequence number and type.
 * Format: [user_key][sequence_number (7 bytes)][type (1 byte)]
 * 
 * Sequence numbers enable MVCC - higher sequence numbers are newer.
 * During reads, we return the value with the highest sequence number <= read_seq.
 */
struct InternalKey {
    std::string user_key;
    SequenceNumber sequence;
    ValueType type;
    
    InternalKey() : sequence(0), type(ValueType::VALUE) {}
    InternalKey(const std::string& key, SequenceNumber seq, ValueType t)
        : user_key(key), sequence(seq), type(t) {}
    
    // Comparator: primary by user_key (ascending), secondary by sequence (descending)
    // This ordering means newer versions of the same key come first
    bool operator<(const InternalKey& other) const {
        int cmp = user_key.compare(other.user_key);
        if (cmp != 0) return cmp < 0;
        // Higher sequence numbers should come first (more recent)
        return sequence > other.sequence;
    }
    
    bool operator==(const InternalKey& other) const {
        return user_key == other.user_key && sequence == other.sequence;
    }
    
    // Encode to binary format for persistence
    std::string Encode() const {
        std::string result;
        result.reserve(user_key.size() + 8);
        result.append(user_key);
        
        // Pack sequence and type into 8 bytes: (sequence << 8) | type
        uint64_t packed = (sequence << 8) | static_cast<uint8_t>(type);
        result.append(reinterpret_cast<const char*>(&packed), sizeof(packed));
        return result;
    }
    
    // Decode from binary format
    static InternalKey Decode(const std::string& encoded) {
        assert(encoded.size() >= 8);
        InternalKey key;
        key.user_key = encoded.substr(0, encoded.size() - 8);
        
        uint64_t packed;
        memcpy(&packed, encoded.data() + encoded.size() - 8, sizeof(packed));
        key.sequence = packed >> 8;
        key.type = static_cast<ValueType>(packed & 0xFF);
        return key;
    }
};

/**
 * KeyValue represents a complete entry in the storage engine.
 */
struct KeyValue {
    InternalKey key;
    std::string value;
    
    KeyValue() = default;
    KeyValue(const InternalKey& k, const std::string& v) : key(k), value(v) {}
    KeyValue(const std::string& user_key, SequenceNumber seq, 
             ValueType type, const std::string& val)
        : key(user_key, seq, type), value(val) {}
    
    size_t Size() const {
        return key.user_key.size() + value.size() + sizeof(SequenceNumber) + 1;
    }
};

// ============================================================================
// Utility functions
// ============================================================================

namespace util {

/**
 * Get current timestamp in microseconds since epoch
 */
inline Timestamp NowMicros() {
    auto now = std::chrono::high_resolution_clock::now();
    auto duration = now.time_since_epoch();
    return std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
}

/**
 * CRC32 table generator - called once during static initialization
 */
inline const uint32_t* GetCRC32Table() {
    static uint32_t table[256] = {};
    static bool initialized = []() {
        for (uint32_t i = 0; i < 256; i++) {
            uint32_t crc = i;
            for (int j = 0; j < 8; j++) {
                crc = (crc >> 1) ^ ((crc & 1) ? 0xEDB88320U : 0);
            }
            table[i] = crc;
        }
        return true;
    }();
    (void)initialized;  // Suppress unused warning
    return table;
}

/**
 * Simple CRC32 for data integrity checks
 * Using the polynomial 0xEDB88320 (IEEE 802.3)
 */
inline uint32_t CRC32(const void* data, size_t length) {
    const uint32_t* table = GetCRC32Table();
    
    uint32_t crc = 0xFFFFFFFF;
    const uint8_t* bytes = static_cast<const uint8_t*>(data);
    for (size_t i = 0; i < length; i++) {
        crc = table[(crc ^ bytes[i]) & 0xFF] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFF;
}

/**
 * Encode a variable-length integer (varint)
 * Each byte uses 7 bits for data, 1 bit to indicate continuation
 */
inline size_t EncodeVarint32(char* dst, uint32_t value) {
    uint8_t* ptr = reinterpret_cast<uint8_t*>(dst);
    while (value >= 0x80) {
        *ptr++ = (value & 0x7F) | 0x80;  // Set continuation bit
        value >>= 7;
    }
    *ptr++ = static_cast<uint8_t>(value);
    return ptr - reinterpret_cast<uint8_t*>(dst);
}

/**
 * Decode a variable-length integer
 */
inline uint32_t DecodeVarint32(const char* src, size_t* bytes_read) {
    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(src);
    uint32_t result = 0;
    int shift = 0;
    while (true) {
        uint32_t byte = *ptr++;
        result |= (byte & 0x7F) << shift;
        if ((byte & 0x80) == 0) break;  // No continuation bit
        shift += 7;
    }
    *bytes_read = ptr - reinterpret_cast<const uint8_t*>(src);
    return result;
}

/**
 * Format file path for SSTable
 */
inline std::string SSTableFileName(const std::string& db_path, FileNumber number, int level) {
    char buf[256];
    snprintf(buf, sizeof(buf), "%s/L%d_%06lu.sst", 
             db_path.c_str(), level, static_cast<unsigned long>(number));
    return std::string(buf);
}

/**
 * Format file path for WAL
 */
inline std::string WALFileName(const std::string& db_path, FileNumber number) {
    char buf[256];
    snprintf(buf, sizeof(buf), "%s/%06lu.wal", 
             db_path.c_str(), static_cast<unsigned long>(number));
    return std::string(buf);
}

}  // namespace util

// ============================================================================
// Spin lock for low-contention scenarios
// Uses atomic operations for lock-free acquisition when possible
// ============================================================================

class SpinLock {
public:
    SpinLock() : locked_(false) {}
    
    // Non-copyable
    SpinLock(const SpinLock&) = delete;
    SpinLock& operator=(const SpinLock&) = delete;
    
    void Lock() {
        // Spin with exponential backoff
        int spins = 0;
        while (locked_.exchange(true, std::memory_order_acquire)) {
            // Use pause instruction to reduce CPU consumption
            while (locked_.load(std::memory_order_relaxed)) {
                if (++spins > 1000) {
                    // After many spins, yield to other threads
                    std::this_thread::yield();
                    spins = 0;
                }
                // x86 pause intrinsic - reduces power and improves spin-wait
                #if defined(__x86_64__) || defined(__i386__)
                    __builtin_ia32_pause();
                #endif
            }
        }
    }
    
    bool TryLock() {
        return !locked_.exchange(true, std::memory_order_acquire);
    }
    
    void Unlock() {
        locked_.store(false, std::memory_order_release);
    }

private:
    std::atomic<bool> locked_;
};

/**
 * RAII guard for SpinLock
 */
class SpinLockGuard {
public:
    explicit SpinLockGuard(SpinLock& lock) : lock_(lock) {
        lock_.Lock();
    }
    ~SpinLockGuard() {
        lock_.Unlock();
    }
    
    SpinLockGuard(const SpinLockGuard&) = delete;
    SpinLockGuard& operator=(const SpinLockGuard&) = delete;

private:
    SpinLock& lock_;
};

}  // namespace lsm

#endif  // STORAGE_ENGINE_COMMON_H
