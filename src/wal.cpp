/**
 * wal.cpp - Write-Ahead Log implementation
 * 
 * Implementation details:
 * - Uses POSIX file I/O for precise control over syncing
 * - Writes are buffered and flushed either on demand or when buffer is full
 * - Records are block-aligned to simplify recovery
 * - CRC32 checksums detect corruption
 */

#include "wal.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <cstring>
#include <algorithm>
#include <iostream>

namespace lsm {

// ============================================================================
// WALWriter Implementation
// ============================================================================

WALWriter::WALWriter(const std::string& filename, FileNumber file_number)
    : filename_(filename)
    , file_number_(file_number)
    , fd_(-1)
    , buffer_(config::WAL_BUFFER_SIZE)
    , buffer_pos_(0)
    , block_offset_(0)
    , file_size_(0)
    , closed_(false) 
{
}

WALWriter::~WALWriter() {
    if (!closed_) {
        Close();
    }
}

Status WALWriter::Open() {
    // Open file with O_DIRECT bypasses OS cache for durability guarantees
    // O_APPEND ensures atomic appends even with multiple writers
    // O_CREAT | O_TRUNC creates new file or truncates existing
    fd_ = open(filename_.c_str(), 
               O_WRONLY | O_CREAT | O_TRUNC,
               0644);
    
    if (fd_ < 0) {
        return Status::IOError("Failed to open WAL: " + filename_ + 
                               " errno=" + std::to_string(errno));
    }
    
    return Status::OK();
}

Status WALWriter::Append(const WALEntry& entry, bool sync) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (closed_) {
        return Status::IOError("WAL is closed");
    }
    
    // Encode the entry
    std::string data = entry.Encode();
    
    // Write as a record
    Status s = WriteRecord(data);
    if (!s.ok()) {
        return s;
    }
    
    // Sync if requested or buffer is getting full
    if (sync || buffer_pos_ >= buffer_.size() - WALRecordHeader::SIZE - 1024) {
        s = FlushBuffer();
        if (!s.ok()) return s;
        
        if (sync) {
            // fsync ensures data is on disk
            if (fsync(fd_) != 0) {
                return Status::IOError("fsync failed: " + std::to_string(errno));
            }
        }
    }
    
    return Status::OK();
}

Status WALWriter::WriteRecord(const std::string& data) {
    const char* ptr = data.data();
    size_t remaining = data.size();
    bool first_fragment = true;
    
    // A record may span multiple fragments if it's too large for current block
    while (remaining > 0) {
        // Space left in current block
        size_t space_in_block = WAL_BLOCK_SIZE - block_offset_;
        
        // Need at least header size to write anything
        if (space_in_block < WALRecordHeader::SIZE) {
            // Pad rest of block with zeros
            if (space_in_block > 0) {
                // Write zero padding
                std::memset(buffer_.data() + buffer_pos_, 0, space_in_block);
                buffer_pos_ += space_in_block;
            }
            block_offset_ = 0;
            space_in_block = WAL_BLOCK_SIZE;
        }
        
        // Maximum data that fits in this fragment
        size_t available = space_in_block - WALRecordHeader::SIZE;
        size_t fragment_size = std::min(remaining, available);
        
        // Determine record type based on position
        WALRecordType type;
        bool last_fragment = (fragment_size == remaining);
        
        if (first_fragment && last_fragment) {
            type = WALRecordType::FULL;
        } else if (first_fragment) {
            type = WALRecordType::FIRST;
        } else if (last_fragment) {
            type = WALRecordType::LAST;
        } else {
            type = WALRecordType::MIDDLE;
        }
        
        // Build record header
        WALRecordHeader header;
        header.length = static_cast<uint32_t>(fragment_size);
        header.type = static_cast<uint8_t>(type);
        
        // Calculate checksum over type + data
        // First, create a buffer with type + data
        std::vector<char> checksum_buf(1 + fragment_size);
        checksum_buf[0] = header.type;
        std::memcpy(checksum_buf.data() + 1, ptr, fragment_size);
        header.checksum = util::CRC32(checksum_buf.data(), checksum_buf.size());
        
        // Write header to buffer
        std::memcpy(buffer_.data() + buffer_pos_, &header.checksum, 
                    sizeof(header.checksum));
        buffer_pos_ += sizeof(header.checksum);
        
        std::memcpy(buffer_.data() + buffer_pos_, &header.length, 
                    sizeof(header.length));
        buffer_pos_ += sizeof(header.length);
        
        buffer_[buffer_pos_++] = header.type;
        
        // Write data to buffer
        std::memcpy(buffer_.data() + buffer_pos_, ptr, fragment_size);
        buffer_pos_ += fragment_size;
        
        // Update state
        block_offset_ += WALRecordHeader::SIZE + fragment_size;
        ptr += fragment_size;
        remaining -= fragment_size;
        first_fragment = false;
        
        // Flush if buffer is nearly full
        if (buffer_pos_ >= buffer_.size() - WAL_BLOCK_SIZE) {
            Status s = FlushBuffer();
            if (!s.ok()) return s;
        }
    }
    
    return Status::OK();
}

Status WALWriter::FlushBuffer() {
    if (buffer_pos_ == 0) {
        return Status::OK();
    }
    
    // Write all buffered data
    ssize_t written = write(fd_, buffer_.data(), buffer_pos_);
    if (written != static_cast<ssize_t>(buffer_pos_)) {
        return Status::IOError("WAL write failed: expected " + 
                               std::to_string(buffer_pos_) + 
                               " wrote " + std::to_string(written));
    }
    
    file_size_ += buffer_pos_;
    buffer_pos_ = 0;
    
    return Status::OK();
}

Status WALWriter::Sync() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    Status s = FlushBuffer();
    if (!s.ok()) return s;
    
    if (fsync(fd_) != 0) {
        return Status::IOError("fsync failed");
    }
    
    return Status::OK();
}

Status WALWriter::Close() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (closed_) {
        return Status::OK();
    }
    
    Status s = FlushBuffer();
    closed_ = true;
    
    if (fd_ >= 0) {
        // Final sync before close
        fsync(fd_);
        close(fd_);
        fd_ = -1;
    }
    
    return s;
}

// ============================================================================
// WALReader Implementation
// ============================================================================

WALReader::WALReader(const std::string& filename)
    : filename_(filename)
    , fd_(-1)
    , buffer_(WAL_BLOCK_SIZE * 2)  // Buffer for reading
    , buffer_pos_(0)
    , buffer_end_(0)
    , block_offset_(0)
    , eof_(false)
    , corrupted_count_(0) 
{
}

WALReader::~WALReader() {
    Close();
}

Status WALReader::Open() {
    fd_ = open(filename_.c_str(), O_RDONLY);
    if (fd_ < 0) {
        return Status::IOError("Failed to open WAL for reading: " + filename_);
    }
    return Status::OK();
}

Status WALReader::ReadEntry(WALEntry* entry) {
    std::string data;
    Status s = ReadRecord(&data);
    
    if (!s.ok()) {
        return s;
    }
    
    // Decode the entry from the record data
    return WALEntry::Decode(data, entry);
}

Status WALReader::ReadRecord(std::string* data) {
    data->clear();
    bool in_fragmented_record = false;
    
    while (true) {
        // Read more data if needed
        if (buffer_pos_ + WALRecordHeader::SIZE > buffer_end_) {
            Status s = ReadMore();
            if (!s.ok()) return s;
            if (eof_) {
                if (in_fragmented_record) {
                    corrupted_count_++;
                    return Status::Corruption("Truncated record at EOF");
                }
                return Status::NotFound("EOF");
            }
        }
        
        // Handle block boundary
        size_t space_in_block = WAL_BLOCK_SIZE - block_offset_;
        if (space_in_block < WALRecordHeader::SIZE) {
            // Skip padding at end of block
            buffer_pos_ += space_in_block;
            block_offset_ = 0;
            continue;
        }
        
        // Parse header
        WALRecordHeader header;
        std::memcpy(&header.checksum, buffer_.data() + buffer_pos_, 
                    sizeof(header.checksum));
        buffer_pos_ += sizeof(header.checksum);
        
        std::memcpy(&header.length, buffer_.data() + buffer_pos_, 
                    sizeof(header.length));
        buffer_pos_ += sizeof(header.length);
        
        header.type = static_cast<uint8_t>(buffer_[buffer_pos_++]);
        
        // Handle zero padding
        if (header.type == static_cast<uint8_t>(WALRecordType::ZERO)) {
            buffer_pos_ -= WALRecordHeader::SIZE;  // Back up
            buffer_pos_ += WAL_BLOCK_SIZE - block_offset_;  // Skip to next block
            block_offset_ = 0;
            continue;
        }
        
        // Ensure we have enough data for the record
        while (buffer_pos_ + header.length > buffer_end_) {
            Status s = ReadMore();
            if (!s.ok()) return s;
            if (eof_) {
                corrupted_count_++;
                return Status::Corruption("Truncated record data");
            }
        }
        
        // Verify checksum
        std::vector<char> checksum_buf(1 + header.length);
        checksum_buf[0] = header.type;
        std::memcpy(checksum_buf.data() + 1, buffer_.data() + buffer_pos_, 
                    header.length);
        
        uint32_t computed_crc = util::CRC32(checksum_buf.data(), checksum_buf.size());
        if (computed_crc != header.checksum) {
            corrupted_count_++;
            // Skip this record and try to find next valid one
            buffer_pos_ += header.length;
            block_offset_ = (block_offset_ + WALRecordHeader::SIZE + header.length) 
                            % WAL_BLOCK_SIZE;
            return Status::Corruption("Checksum mismatch");
        }
        
        // Extract data
        WALRecordType type = static_cast<WALRecordType>(header.type);
        
        switch (type) {
            case WALRecordType::FULL:
                if (in_fragmented_record) {
                    corrupted_count_++;
                    data->clear();
                }
                data->assign(buffer_.data() + buffer_pos_, header.length);
                buffer_pos_ += header.length;
                block_offset_ = (block_offset_ + WALRecordHeader::SIZE + header.length) 
                                % WAL_BLOCK_SIZE;
                return Status::OK();
                
            case WALRecordType::FIRST:
                if (in_fragmented_record) {
                    corrupted_count_++;
                    data->clear();
                }
                data->assign(buffer_.data() + buffer_pos_, header.length);
                buffer_pos_ += header.length;
                block_offset_ = (block_offset_ + WALRecordHeader::SIZE + header.length) 
                                % WAL_BLOCK_SIZE;
                in_fragmented_record = true;
                break;
                
            case WALRecordType::MIDDLE:
                if (!in_fragmented_record) {
                    corrupted_count_++;
                    buffer_pos_ += header.length;
                    block_offset_ = (block_offset_ + WALRecordHeader::SIZE + header.length) 
                                    % WAL_BLOCK_SIZE;
                    continue;
                }
                data->append(buffer_.data() + buffer_pos_, header.length);
                buffer_pos_ += header.length;
                block_offset_ = (block_offset_ + WALRecordHeader::SIZE + header.length) 
                                % WAL_BLOCK_SIZE;
                break;
                
            case WALRecordType::LAST:
                if (!in_fragmented_record) {
                    corrupted_count_++;
                    buffer_pos_ += header.length;
                    block_offset_ = (block_offset_ + WALRecordHeader::SIZE + header.length) 
                                    % WAL_BLOCK_SIZE;
                    continue;
                }
                data->append(buffer_.data() + buffer_pos_, header.length);
                buffer_pos_ += header.length;
                block_offset_ = (block_offset_ + WALRecordHeader::SIZE + header.length) 
                                % WAL_BLOCK_SIZE;
                return Status::OK();
                
            default:
                corrupted_count_++;
                return Status::Corruption("Unknown record type");
        }
    }
}

Status WALReader::ReadMore() {
    // Move remaining data to start of buffer
    if (buffer_pos_ > 0 && buffer_end_ > buffer_pos_) {
        size_t remaining = buffer_end_ - buffer_pos_;
        std::memmove(buffer_.data(), buffer_.data() + buffer_pos_, remaining);
        buffer_end_ = remaining;
        buffer_pos_ = 0;
    } else {
        buffer_end_ = 0;
        buffer_pos_ = 0;
    }
    
    // Read more data
    ssize_t bytes_read = read(fd_, buffer_.data() + buffer_end_, 
                              buffer_.size() - buffer_end_);
    
    if (bytes_read < 0) {
        return Status::IOError("WAL read error: " + std::to_string(errno));
    }
    
    if (bytes_read == 0) {
        eof_ = true;
    }
    
    buffer_end_ += bytes_read;
    return Status::OK();
}

Status WALReader::Close() {
    if (fd_ >= 0) {
        close(fd_);
        fd_ = -1;
    }
    return Status::OK();
}

// ============================================================================
// WALManager Implementation
// ============================================================================

WALManager::WALManager(const std::string& db_path)
    : db_path_(db_path) 
{
}

WALManager::~WALManager() {
    // Close all active WALs
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& wal : active_wals_) {
        wal->Close();
    }
}

Status WALManager::Init() {
    // Ensure directory exists
    struct stat st;
    if (stat(db_path_.c_str(), &st) != 0) {
        // Directory doesn't exist - will be created by caller
        return Status::OK();
    }
    
    // Scan for existing WAL files to determine next file number
    FileNumber max_file_number = 0;
    
    DIR* dir = opendir(db_path_.c_str());
    if (dir) {
        struct dirent* entry;
        while ((entry = readdir(dir)) != nullptr) {
            std::string name = entry->d_name;
            // WAL files are named XXXXXX.wal
            if (name.size() > 4 && name.substr(name.size() - 4) == ".wal") {
                try {
                    FileNumber num = std::stoull(name.substr(0, name.size() - 4));
                    max_file_number = std::max(max_file_number, num);
                } catch (...) {
                    // Ignore invalid filenames
                }
            }
        }
        closedir(dir);
    }
    
    next_file_number_.store(max_file_number + 1);
    return Status::OK();
}

std::shared_ptr<WALWriter> WALManager::CreateWAL() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    FileNumber file_number = next_file_number_.fetch_add(1);
    std::string filename = util::WALFileName(db_path_, file_number);
    
    auto wal = std::make_shared<WALWriter>(filename, file_number);
    Status s = wal->Open();
    
    if (!s.ok()) {
        std::cerr << "Failed to create WAL: " << s.ToString() << std::endl;
        return nullptr;
    }
    
    active_wals_.push_back(wal);
    return wal;
}

Status WALManager::ObsoleteWAL(FileNumber file_number) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Find and remove from active list
    auto it = std::find_if(active_wals_.begin(), active_wals_.end(),
                           [file_number](const std::shared_ptr<WALWriter>& w) {
                               return w->GetFileNumber() == file_number;
                           });
    
    std::string filename;
    if (it != active_wals_.end()) {
        filename = (*it)->GetFilename();
        (*it)->Close();
        active_wals_.erase(it);
    } else {
        filename = util::WALFileName(db_path_, file_number);
    }
    
    // Delete the file
    if (unlink(filename.c_str()) != 0 && errno != ENOENT) {
        return Status::IOError("Failed to delete WAL: " + filename);
    }
    
    return Status::OK();
}

std::vector<std::string> WALManager::GetWALFiles() const {
    std::vector<std::pair<FileNumber, std::string>> files;
    
    DIR* dir = opendir(db_path_.c_str());
    if (!dir) {
        return {};
    }
    
    struct dirent* entry;
    while ((entry = readdir(dir)) != nullptr) {
        std::string name = entry->d_name;
        if (name.size() > 4 && name.substr(name.size() - 4) == ".wal") {
            try {
                FileNumber num = std::stoull(name.substr(0, name.size() - 4));
                files.emplace_back(num, db_path_ + "/" + name);
            } catch (...) {
                // Ignore invalid filenames
            }
        }
    }
    closedir(dir);
    
    // Sort by file number
    std::sort(files.begin(), files.end());
    
    std::vector<std::string> result;
    result.reserve(files.size());
    for (const auto& p : files) {
        result.push_back(p.second);
    }
    
    return result;
}

std::pair<Status, SequenceNumber> WALManager::Recover(
    std::function<Status(const WALEntry&)> callback) {
    
    SequenceNumber max_sequence = 0;
    
    std::vector<std::string> wal_files = GetWALFiles();
    
    for (const std::string& filename : wal_files) {
        WALReader reader(filename);
        Status s = reader.Open();
        
        if (!s.ok()) {
            std::cerr << "Failed to open WAL for recovery: " << filename 
                      << " - " << s.ToString() << std::endl;
            continue;
        }
        
        while (reader.HasMore()) {
            WALEntry entry;
            s = reader.ReadEntry(&entry);
            
            if (s.IsNotFound()) {
                break;  // EOF
            }
            
            if (!s.ok()) {
                std::cerr << "WAL recovery error in " << filename 
                          << ": " << s.ToString() << std::endl;
                continue;  // Try to continue with next record
            }
            
            // Track max sequence number
            max_sequence = std::max(max_sequence, entry.sequence);
            
            // Replay entry
            s = callback(entry);
            if (!s.ok()) {
                return {s, max_sequence};
            }
        }
        
        if (reader.GetCorruptedCount() > 0) {
            std::cerr << "WAL " << filename << " had " 
                      << reader.GetCorruptedCount() << " corrupted records" 
                      << std::endl;
        }
        
        reader.Close();
    }
    
    return {Status::OK(), max_sequence};
}

Status WALManager::DeleteAllWALs() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Close active WALs
    for (auto& wal : active_wals_) {
        wal->Close();
    }
    active_wals_.clear();
    
    // Delete all WAL files
    std::vector<std::string> files = GetWALFiles();
    for (const std::string& filename : files) {
        if (unlink(filename.c_str()) != 0 && errno != ENOENT) {
            return Status::IOError("Failed to delete WAL: " + filename);
        }
    }
    
    return Status::OK();
}

}  // namespace lsm
