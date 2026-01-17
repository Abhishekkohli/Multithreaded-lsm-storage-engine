/**
 * sstable.cpp - Sorted String Table implementation
 * 
 * Implementation of the on-disk storage format:
 * - Bloom filter for fast negative lookups
 * - Block-based storage with prefix compression
 * - Index for block-level seeking
 */

#include "sstable.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <cstring>
#include <algorithm>
#include <iostream>
#include <regex>

namespace lsm {

// ============================================================================
// BloomFilter Implementation
// ============================================================================

BloomFilter::BloomFilter(size_t num_keys, size_t bits_per_key) {
    // Calculate optimal number of bits and hash functions
    // k = (m/n) * ln(2) where k = hash functions, m = bits, n = keys
    num_bits_ = num_keys * bits_per_key;
    if (num_bits_ < 64) num_bits_ = 64;  // Minimum size
    
    // Calculate optimal number of hash functions
    num_hash_functions_ = static_cast<size_t>(bits_per_key * 0.69);  // ln(2) ≈ 0.69
    if (num_hash_functions_ < 1) num_hash_functions_ = 1;
    if (num_hash_functions_ > 30) num_hash_functions_ = 30;
    
    // Allocate bit array
    bits_.resize((num_bits_ + 7) / 8, 0);
}

BloomFilter::BloomFilter(const std::string& data) {
    if (data.size() < 8) {
        bits_.resize(8, 0);
        num_bits_ = 64;
        num_hash_functions_ = 4;
        return;
    }
    
    // Deserialize: [num_hash_functions (4)][bits...]
    memcpy(&num_hash_functions_, data.data(), sizeof(uint32_t));
    num_bits_ = (data.size() - sizeof(uint32_t)) * 8;
    bits_.assign(data.begin() + sizeof(uint32_t), data.end());
}

void BloomFilter::Add(const std::string& key) {
    // Use double hashing: h(i) = h1 + i * h2
    uint32_t h1 = Hash(key, 0);
    uint32_t h2 = Hash(key, h1);
    
    for (size_t i = 0; i < num_hash_functions_; i++) {
        uint32_t bit_pos = (h1 + i * h2) % num_bits_;
        bits_[bit_pos / 8] |= (1 << (bit_pos % 8));
    }
}

bool BloomFilter::MayContain(const std::string& key) const {
    uint32_t h1 = Hash(key, 0);
    uint32_t h2 = Hash(key, h1);
    
    for (size_t i = 0; i < num_hash_functions_; i++) {
        uint32_t bit_pos = (h1 + i * h2) % num_bits_;
        if ((bits_[bit_pos / 8] & (1 << (bit_pos % 8))) == 0) {
            return false;  // Definitely not in set
        }
    }
    return true;  // Might be in set
}

std::string BloomFilter::Serialize() const {
    std::string result;
    result.reserve(sizeof(uint32_t) + bits_.size());
    
    uint32_t k = static_cast<uint32_t>(num_hash_functions_);
    result.append(reinterpret_cast<const char*>(&k), sizeof(k));
    result.append(reinterpret_cast<const char*>(bits_.data()), bits_.size());
    
    return result;
}

uint32_t BloomFilter::Hash(const std::string& key, uint32_t seed) const {
    // MurmurHash3-inspired hash
    const uint32_t c1 = 0xcc9e2d51;
    const uint32_t c2 = 0x1b873593;
    
    uint32_t h = seed;
    const uint8_t* data = reinterpret_cast<const uint8_t*>(key.data());
    size_t len = key.size();
    
    // Process 4 bytes at a time
    while (len >= 4) {
        uint32_t k;
        memcpy(&k, data, 4);
        k *= c1;
        k = (k << 15) | (k >> 17);
        k *= c2;
        
        h ^= k;
        h = (h << 13) | (h >> 19);
        h = h * 5 + 0xe6546b64;
        
        data += 4;
        len -= 4;
    }
    
    // Process remaining bytes
    uint32_t k = 0;
    switch (len) {
        case 3: k ^= data[2] << 16; [[fallthrough]];
        case 2: k ^= data[1] << 8;  [[fallthrough]];
        case 1: k ^= data[0];
                k *= c1;
                k = (k << 15) | (k >> 17);
                k *= c2;
                h ^= k;
    }
    
    // Finalization
    h ^= static_cast<uint32_t>(key.size());
    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;
    
    return h;
}

// ============================================================================
// BlockBuilder Implementation
// ============================================================================

BlockBuilder::BlockBuilder(size_t restart_interval)
    : restart_interval_(restart_interval)
    , counter_(0)
    , num_entries_(0) 
{
    restarts_.push_back(0);  // First entry is always a restart
}

void BlockBuilder::Add(const InternalKey& key, const std::string& value) {
    std::string encoded_key = key.Encode();
    
    size_t shared = 0;
    if (counter_ < static_cast<int>(restart_interval_)) {
        // Calculate shared prefix with previous key
        size_t min_len = std::min(last_key_.size(), encoded_key.size());
        while (shared < min_len && last_key_[shared] == encoded_key[shared]) {
            shared++;
        }
    } else {
        // Restart point - no prefix sharing
        restarts_.push_back(static_cast<uint32_t>(buffer_.size()));
        counter_ = 0;
    }
    
    size_t unshared = encoded_key.size() - shared;
    
    // Encode entry: [shared (varint)][unshared (varint)][value_len (varint)]
    //               [unshared_key_data][value_data]
    char varint_buf[15];
    
    size_t bytes = util::EncodeVarint32(varint_buf, static_cast<uint32_t>(shared));
    buffer_.append(varint_buf, bytes);
    
    bytes = util::EncodeVarint32(varint_buf, static_cast<uint32_t>(unshared));
    buffer_.append(varint_buf, bytes);
    
    bytes = util::EncodeVarint32(varint_buf, static_cast<uint32_t>(value.size()));
    buffer_.append(varint_buf, bytes);
    
    buffer_.append(encoded_key.data() + shared, unshared);
    buffer_.append(value);
    
    last_key_ = encoded_key;
    counter_++;
    num_entries_++;
}

std::string BlockBuilder::Finish() {
    // Append restart offsets
    for (uint32_t restart : restarts_) {
        buffer_.append(reinterpret_cast<const char*>(&restart), sizeof(restart));
    }
    
    // Append number of restarts
    uint32_t num_restarts = static_cast<uint32_t>(restarts_.size());
    buffer_.append(reinterpret_cast<const char*>(&num_restarts), sizeof(num_restarts));
    
    return buffer_;
}

void BlockBuilder::Reset() {
    buffer_.clear();
    restarts_.clear();
    restarts_.push_back(0);
    counter_ = 0;
    num_entries_ = 0;
    last_key_.clear();
}

// ============================================================================
// BlockReader Implementation
// ============================================================================

BlockReader::BlockReader(const std::string& data) : data_(data) {
    if (data_.size() < sizeof(uint32_t)) {
        num_restarts_ = 0;
        data_end_ = 0;
        return;
    }
    
    // Read number of restarts from end
    memcpy(&num_restarts_, data_.data() + data_.size() - sizeof(uint32_t),
           sizeof(uint32_t));
    
    if (num_restarts_ == 0 || 
        data_.size() < sizeof(uint32_t) * (1 + num_restarts_)) {
        num_restarts_ = 0;
        data_end_ = 0;
        return;
    }
    
    // Read restart offsets
    size_t restart_offset = data_.size() - sizeof(uint32_t) * (1 + num_restarts_);
    data_end_ = restart_offset;
    
    restarts_.resize(num_restarts_);
    memcpy(restarts_.data(), data_.data() + restart_offset,
           sizeof(uint32_t) * num_restarts_);
}

BlockReader::Iterator BlockReader::NewIterator() const {
    return Iterator(this, 0);
}

// ============================================================================
// BlockReader::Iterator Implementation
// ============================================================================

BlockReader::Iterator::Iterator(const BlockReader* reader, size_t offset)
    : reader_(reader)
    , offset_(offset)
    , restart_index_(0)
    , valid_(false) 
{
}

void BlockReader::Iterator::SeekToFirst() {
    offset_ = 0;
    restart_index_ = 0;
    ParseEntry();
}

void BlockReader::Iterator::Next() {
    if (!valid_) return;
    ParseEntry();
}

void BlockReader::Iterator::Seek(const InternalKey& target) {
    // Binary search on restart points
    size_t left = 0;
    size_t right = reader_->num_restarts_;
    
    while (left < right) {
        size_t mid = left + (right - left) / 2;
        offset_ = reader_->restarts_[mid];
        ParseEntry();
        
        if (!valid_) {
            // Corrupted block
            return;
        }
        
        if (key_ < target) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    
    // Linear scan from restart point
    if (left > 0) {
        restart_index_ = left - 1;
        offset_ = reader_->restarts_[restart_index_];
    } else {
        restart_index_ = 0;
        offset_ = 0;
    }
    
    // Scan forward to find target
    while (true) {
        ParseEntry();
        if (!valid_) return;
        if (!(key_ < target)) return;  // Found or passed target
    }
}

void BlockReader::Iterator::ParseEntry() {
    if (offset_ >= reader_->data_end_) {
        valid_ = false;
        return;
    }
    
    const char* p = reader_->data_.data() + offset_;
    const char* limit = reader_->data_.data() + reader_->data_end_;
    
    // Decode shared length
    size_t bytes_read;
    uint32_t shared = util::DecodeVarint32(p, &bytes_read);
    p += bytes_read;
    if (p >= limit) { valid_ = false; return; }
    
    // Decode unshared length
    uint32_t unshared = util::DecodeVarint32(p, &bytes_read);
    p += bytes_read;
    if (p >= limit) { valid_ = false; return; }
    
    // Decode value length
    uint32_t value_len = util::DecodeVarint32(p, &bytes_read);
    p += bytes_read;
    if (p + unshared + value_len > limit) { valid_ = false; return; }
    
    // Reconstruct key
    std::string encoded_key;
    if (shared > 0) {
        encoded_key = key_.Encode().substr(0, shared);
    }
    encoded_key.append(p, unshared);
    key_ = InternalKey::Decode(encoded_key);
    p += unshared;
    
    // Read value
    value_.assign(p, value_len);
    p += value_len;
    
    offset_ = p - reader_->data_.data();
    valid_ = true;
}

// ============================================================================
// SSTableFooter Implementation
// ============================================================================

std::string SSTableFooter::Encode() const {
    std::string result;
    result.reserve(SIZE);
    
    result.append(reinterpret_cast<const char*>(&index_offset), 8);
    result.append(reinterpret_cast<const char*>(&index_size), 8);
    result.append(reinterpret_cast<const char*>(&bloom_offset), 8);
    result.append(reinterpret_cast<const char*>(&bloom_size), 8);
    result.append(reinterpret_cast<const char*>(&num_entries), 8);
    result.append(reinterpret_cast<const char*>(&file_size), 8);
    result.append(reinterpret_cast<const char*>(&min_sequence), 8);
    result.append(reinterpret_cast<const char*>(&max_sequence), 8);
    result.append(reinterpret_cast<const char*>(&magic), 4);
    
    // Calculate checksum of everything except the checksum itself
    uint32_t crc = util::CRC32(result.data(), result.size());
    result.append(reinterpret_cast<const char*>(&crc), 4);
    
    return result;
}

Status SSTableFooter::Decode(const std::string& data, SSTableFooter* footer) {
    if (data.size() != SIZE) {
        return Status::Corruption("Invalid footer size");
    }
    
    const char* p = data.data();
    
    memcpy(&footer->index_offset, p, 8); p += 8;
    memcpy(&footer->index_size, p, 8); p += 8;
    memcpy(&footer->bloom_offset, p, 8); p += 8;
    memcpy(&footer->bloom_size, p, 8); p += 8;
    memcpy(&footer->num_entries, p, 8); p += 8;
    memcpy(&footer->file_size, p, 8); p += 8;
    memcpy(&footer->min_sequence, p, 8); p += 8;
    memcpy(&footer->max_sequence, p, 8); p += 8;
    memcpy(&footer->magic, p, 4); p += 4;
    memcpy(&footer->checksum, p, 4);
    
    // Verify magic
    if (footer->magic != MAGIC) {
        return Status::Corruption("Invalid SSTable magic number");
    }
    
    // Verify checksum
    uint32_t expected_crc = util::CRC32(data.data(), SIZE - 4);
    if (footer->checksum != expected_crc) {
        return Status::Corruption("Footer checksum mismatch");
    }
    
    return Status::OK();
}

// ============================================================================
// SSTableBuilder Implementation
// ============================================================================

SSTableBuilder::SSTableBuilder(const std::string& filename, 
                               FileNumber file_number, int level)
    : filename_(filename)
    , file_number_(file_number)
    , level_(level)
    , fd_(-1)
    , offset_(0)
    , num_entries_(0)
    , min_sequence_(UINT64_MAX)
    , max_sequence_(0)
    , finished_(false)
    , abandoned_(false) 
{
}

SSTableBuilder::~SSTableBuilder() {
    if (!finished_ && !abandoned_) {
        Abandon();
    }
}

Status SSTableBuilder::Open() {
    fd_ = open(filename_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd_ < 0) {
        return Status::IOError("Failed to create SSTable: " + filename_);
    }
    
    // Initialize bloom filter with estimated capacity
    bloom_filter_ = std::make_unique<BloomFilter>(10000);  // Estimate
    
    return Status::OK();
}

Status SSTableBuilder::Add(const InternalKey& key, const std::string& value) {
    if (finished_ || abandoned_) {
        return Status::InvalidArgument("SSTable already finished/abandoned");
    }
    
    // Track key range
    std::string encoded = key.Encode();
    if (num_entries_ == 0) {
        smallest_key_ = encoded;
    }
    largest_key_ = encoded;
    
    // Track sequence number range
    min_sequence_ = std::min(min_sequence_, key.sequence);
    max_sequence_ = std::max(max_sequence_, key.sequence);
    
    // Add to bloom filter
    bloom_filter_->Add(key.user_key);
    
    // Add to current data block
    data_block_.Add(key, value);
    num_entries_++;
    
    // Flush block if too large
    if (data_block_.EstimatedSize() >= config::BLOCK_SIZE) {
        return FlushDataBlock();
    }
    
    return Status::OK();
}

Status SSTableBuilder::FlushDataBlock() {
    if (data_block_.Empty()) {
        return Status::OK();
    }
    
    std::string block_data = data_block_.Finish();
    
    uint64_t block_offset, block_size;
    Status s = WriteBlock(block_data, &block_offset, &block_size);
    if (!s.ok()) return s;
    
    // Add to index: last key in block -> offset
    index_entries_.emplace_back(largest_key_, block_offset);
    
    data_block_.Reset();
    return Status::OK();
}

Status SSTableBuilder::WriteBlock(const std::string& data, 
                                  uint64_t* offset, uint64_t* size) {
    // Block format: [data][checksum (4 bytes)][size (4 bytes)]
    uint32_t crc = util::CRC32(data.data(), data.size());
    uint32_t data_size = static_cast<uint32_t>(data.size());
    
    *offset = offset_;
    
    // Write data
    ssize_t written = write(fd_, data.data(), data.size());
    if (written != static_cast<ssize_t>(data.size())) {
        return Status::IOError("Failed to write block data");
    }
    offset_ += data.size();
    
    // Write checksum
    written = write(fd_, &crc, sizeof(crc));
    if (written != sizeof(crc)) {
        return Status::IOError("Failed to write block checksum");
    }
    offset_ += sizeof(crc);
    
    // Write size
    written = write(fd_, &data_size, sizeof(data_size));
    if (written != sizeof(data_size)) {
        return Status::IOError("Failed to write block size");
    }
    offset_ += sizeof(data_size);
    
    *size = data.size() + sizeof(crc) + sizeof(data_size);
    return Status::OK();
}

Status SSTableBuilder::Finish() {
    if (finished_ || abandoned_) {
        return Status::InvalidArgument("SSTable already finished/abandoned");
    }
    
    // Flush remaining data block
    Status s = FlushDataBlock();
    if (!s.ok()) return s;
    
    // Build and write index block
    BlockBuilder index_block;
    for (const auto& entry : index_entries_) {
        InternalKey index_key = InternalKey::Decode(entry.first);
        char offset_buf[8];
        memcpy(offset_buf, &entry.second, sizeof(uint64_t));
        index_block.Add(index_key, std::string(offset_buf, 8));
    }
    
    std::string index_data = index_block.Finish();
    uint64_t index_offset, index_size;
    s = WriteBlock(index_data, &index_offset, &index_size);
    if (!s.ok()) return s;
    
    // Write bloom filter
    std::string bloom_data = bloom_filter_->Serialize();
    uint64_t bloom_offset = offset_;
    uint64_t bloom_size = bloom_data.size();
    ssize_t written = write(fd_, bloom_data.data(), bloom_data.size());
    if (written != static_cast<ssize_t>(bloom_data.size())) {
        return Status::IOError("Failed to write bloom filter");
    }
    offset_ += bloom_data.size();
    
    // Write footer
    SSTableFooter footer;
    footer.index_offset = index_offset;
    footer.index_size = index_size;
    footer.bloom_offset = bloom_offset;
    footer.bloom_size = bloom_size;
    footer.num_entries = num_entries_;
    footer.file_size = offset_ + SSTableFooter::SIZE;
    footer.min_sequence = min_sequence_;
    footer.max_sequence = max_sequence_;
    footer.magic = SSTableFooter::MAGIC;
    
    std::string footer_data = footer.Encode();
    written = write(fd_, footer_data.data(), footer_data.size());
    if (written != static_cast<ssize_t>(footer_data.size())) {
        return Status::IOError("Failed to write footer");
    }
    offset_ += footer_data.size();
    
    // Sync and close
    fsync(fd_);
    close(fd_);
    fd_ = -1;
    
    finished_ = true;
    return Status::OK();
}

void SSTableBuilder::Abandon() {
    abandoned_ = true;
    if (fd_ >= 0) {
        close(fd_);
        fd_ = -1;
    }
    // Delete partial file
    unlink(filename_.c_str());
}

// ============================================================================
// SSTableReader Implementation
// ============================================================================

SSTableReader::SSTableReader(const std::string& filename)
    : filename_(filename)
    , file_number_(0)
    , fd_(-1)
    , opened_(false) 
{
    // Extract file number from filename
    std::regex pattern("L\\d+_(\\d+)\\.sst$");
    std::smatch match;
    if (std::regex_search(filename, match, pattern)) {
        file_number_ = std::stoull(match[1].str());
    }
}

SSTableReader::~SSTableReader() {
    Close();
}

Status SSTableReader::Open() {
    fd_ = open(filename_.c_str(), O_RDONLY);
    if (fd_ < 0) {
        return Status::IOError("Failed to open SSTable: " + filename_);
    }
    
    // Get file size
    struct stat st;
    if (fstat(fd_, &st) != 0) {
        close(fd_);
        fd_ = -1;
        return Status::IOError("Failed to stat SSTable: " + filename_);
    }
    
    // Read footer
    if (static_cast<size_t>(st.st_size) < SSTableFooter::SIZE) {
        close(fd_);
        fd_ = -1;
        return Status::Corruption("SSTable too small: " + filename_);
    }
    
    std::string footer_data(SSTableFooter::SIZE, '\0');
    ssize_t bytes = pread(fd_, &footer_data[0], SSTableFooter::SIZE,
                          st.st_size - SSTableFooter::SIZE);
    if (bytes != SSTableFooter::SIZE) {
        close(fd_);
        fd_ = -1;
        return Status::IOError("Failed to read footer");
    }
    
    Status s = SSTableFooter::Decode(footer_data, &footer_);
    if (!s.ok()) {
        close(fd_);
        fd_ = -1;
        return s;
    }
    
    // Read bloom filter
    std::string bloom_data(footer_.bloom_size, '\0');
    bytes = pread(fd_, &bloom_data[0], footer_.bloom_size, footer_.bloom_offset);
    if (bytes != static_cast<ssize_t>(footer_.bloom_size)) {
        close(fd_);
        fd_ = -1;
        return Status::IOError("Failed to read bloom filter");
    }
    bloom_filter_ = std::make_unique<BloomFilter>(bloom_data);
    
    // Build index
    s = BuildIndex();
    if (!s.ok()) {
        close(fd_);
        fd_ = -1;
        return s;
    }
    
    opened_ = true;
    return Status::OK();
}

Status SSTableReader::Close() {
    if (fd_ >= 0) {
        close(fd_);
        fd_ = -1;
    }
    opened_ = false;
    return Status::OK();
}

Status SSTableReader::BuildIndex() {
    // Read index block
    std::string index_data;
    Status s = ReadBlock(footer_.index_offset, footer_.index_size, &index_data);
    if (!s.ok()) return s;
    
    // Remove checksum and size suffix from block data
    if (index_data.size() < 8) {
        return Status::Corruption("Index block too small");
    }
    index_data.resize(index_data.size() - 8);  // Remove checksum + size
    
    // Parse index block
    BlockReader reader(index_data);
    auto iter = reader.NewIterator();
    iter.SeekToFirst();
    
    while (iter.Valid()) {
        // Value is block offset
        uint64_t offset = 0;
        if (iter.GetValue().size() >= 8) {
            memcpy(&offset, iter.GetValue().data(), 8);
        }
        
        BlockHandle handle;
        handle.offset = offset;
        handle.size = config::BLOCK_SIZE + 8;  // Approximate
        
        block_index_.emplace_back(iter.GetKey().Encode(), handle);
        
        iter.Next();
    }
    
    // Set key range
    if (!block_index_.empty()) {
        smallest_key_ = block_index_.front().first;
        largest_key_ = block_index_.back().first;
    }
    
    return Status::OK();
}

Status SSTableReader::ReadBlock(uint64_t offset, uint64_t size, 
                                std::string* data) const {
    data->resize(size);
    ssize_t bytes = pread(fd_, &(*data)[0], size, offset);
    if (bytes != static_cast<ssize_t>(size)) {
        return Status::IOError("Failed to read block");
    }
    return Status::OK();
}

bool SSTableReader::MayContain(const std::string& user_key) const {
    return bloom_filter_ && bloom_filter_->MayContain(user_key);
}

Status SSTableReader::Get(const InternalKey& key, std::string* value) const {
    if (!opened_) {
        return Status::IOError("SSTable not opened");
    }
    
    // Check bloom filter first
    if (!MayContain(key.user_key)) {
        return Status::NotFound();
    }
    
    // Binary search in index to find potential block
    std::string encoded_key = key.Encode();
    
    auto it = std::lower_bound(block_index_.begin(), block_index_.end(),
                               encoded_key,
                               [](const auto& entry, const std::string& k) {
                                   return entry.first < k;
                               });
    
    if (it == block_index_.end()) {
        return Status::NotFound();
    }
    
    // Read and search block
    std::string block_data;
    Status s = ReadBlock(it->second.offset, it->second.size, &block_data);
    if (!s.ok()) return s;
    
    // Remove checksum and size suffix
    if (block_data.size() >= 8) {
        block_data.resize(block_data.size() - 8);
    }
    
    BlockReader reader(block_data);
    auto iter = reader.NewIterator();
    iter.Seek(key);
    
    if (iter.Valid() && iter.GetKey().user_key == key.user_key) {
        if (iter.GetKey().type == ValueType::DELETION) {
            return Status::NotFound("Key was deleted");
        }
        *value = iter.GetValue();
        return Status::OK();
    }
    
    return Status::NotFound();
}

// ============================================================================
// SSTableReader::Iterator Implementation
// ============================================================================

SSTableReader::Iterator::Iterator(const SSTableReader* reader)
    : reader_(reader)
    , current_block_(0)
    , valid_(false) 
{
}

SSTableReader::Iterator::~Iterator() = default;

SSTableReader::Iterator::Iterator(Iterator&& other) noexcept
    : reader_(other.reader_)
    , current_block_(other.current_block_)
    , block_reader_(std::move(other.block_reader_))
    , block_iter_(std::move(other.block_iter_))
    , key_(std::move(other.key_))
    , value_(std::move(other.value_))
    , valid_(other.valid_) 
{
    other.reader_ = nullptr;
    other.valid_ = false;
}

SSTableReader::Iterator& SSTableReader::Iterator::operator=(Iterator&& other) noexcept {
    if (this != &other) {
        reader_ = other.reader_;
        current_block_ = other.current_block_;
        block_reader_ = std::move(other.block_reader_);
        block_iter_ = std::move(other.block_iter_);
        key_ = std::move(other.key_);
        value_ = std::move(other.value_);
        valid_ = other.valid_;
        other.reader_ = nullptr;
        other.valid_ = false;
    }
    return *this;
}

void SSTableReader::Iterator::SeekToFirst() {
    if (reader_->block_index_.empty()) {
        valid_ = false;
        return;
    }
    
    current_block_ = 0;
    LoadBlock(0);
    
    if (block_iter_) {
        block_iter_->SeekToFirst();
        if (block_iter_->Valid()) {
            key_ = block_iter_->GetKey();
            value_ = block_iter_->GetValue();
            valid_ = true;
        }
    }
}

void SSTableReader::Iterator::Seek(const InternalKey& target) {
    std::string encoded = target.Encode();
    
    // Find block containing target
    auto it = std::lower_bound(reader_->block_index_.begin(),
                               reader_->block_index_.end(),
                               encoded,
                               [](const auto& entry, const std::string& k) {
                                   return entry.first < k;
                               });
    
    if (it == reader_->block_index_.end()) {
        valid_ = false;
        return;
    }
    
    current_block_ = it - reader_->block_index_.begin();
    LoadBlock(current_block_);
    
    if (block_iter_) {
        block_iter_->Seek(target);
        if (block_iter_->Valid()) {
            key_ = block_iter_->GetKey();
            value_ = block_iter_->GetValue();
            valid_ = true;
        } else {
            Next();  // Try next block
        }
    }
}

void SSTableReader::Iterator::Next() {
    if (!valid_) return;
    
    if (block_iter_) {
        block_iter_->Next();
        if (block_iter_->Valid()) {
            key_ = block_iter_->GetKey();
            value_ = block_iter_->GetValue();
            return;
        }
    }
    
    // Move to next block
    current_block_++;
    if (current_block_ >= reader_->block_index_.size()) {
        valid_ = false;
        return;
    }
    
    LoadBlock(current_block_);
    if (block_iter_) {
        block_iter_->SeekToFirst();
        if (block_iter_->Valid()) {
            key_ = block_iter_->GetKey();
            value_ = block_iter_->GetValue();
        } else {
            valid_ = false;
        }
    }
}

void SSTableReader::Iterator::LoadBlock(size_t block_index) {
    if (block_index >= reader_->block_index_.size()) {
        block_reader_.reset();
        block_iter_.reset();
        return;
    }
    
    const auto& handle = reader_->block_index_[block_index].second;
    std::string block_data;
    Status s = reader_->ReadBlock(handle.offset, handle.size, &block_data);
    
    if (!s.ok()) {
        block_reader_.reset();
        block_iter_.reset();
        valid_ = false;
        return;
    }
    
    // Remove checksum and size
    if (block_data.size() >= 8) {
        block_data.resize(block_data.size() - 8);
    }
    
    block_reader_ = std::make_unique<BlockReader>(block_data);
    block_iter_ = std::make_unique<BlockReader::Iterator>(block_reader_->NewIterator());
}

// ============================================================================
// SSTableManager Implementation
// ============================================================================

SSTableManager::SSTableManager(const std::string& db_path)
    : db_path_(db_path) 
{
    levels_.resize(config::MAX_LEVELS);
}

SSTableManager::~SSTableManager() = default;

Status SSTableManager::Init() {
    // Scan directory for existing SSTables
    DIR* dir = opendir(db_path_.c_str());
    if (!dir) {
        return Status::OK();  // Directory doesn't exist yet
    }
    
    FileNumber max_file_number = 0;
    std::regex pattern("L(\\d+)_(\\d+)\\.sst$");
    
    struct dirent* entry;
    while ((entry = readdir(dir)) != nullptr) {
        std::string name = entry->d_name;
        std::smatch match;
        
        if (std::regex_search(name, match, pattern)) {
            int level = std::stoi(match[1].str());
            FileNumber file_num = std::stoull(match[2].str());
            max_file_number = std::max(max_file_number, file_num);
            
            // Open and read SSTable metadata
            std::string filename = db_path_ + "/" + name;
            auto reader = std::make_shared<SSTableReader>(filename);
            Status s = reader->Open();
            
            if (s.ok()) {
                auto meta = std::make_shared<SSTableMetadata>();
                meta->file_number = file_num;
                meta->level = level;
                meta->filename = filename;
                meta->file_size = reader->GetFileSize();
                meta->num_entries = reader->GetNumEntries();
                meta->smallest_key = reader->GetSmallestKey();
                meta->largest_key = reader->GetLargestKey();
                meta->min_sequence = reader->GetMinSequence();
                meta->max_sequence = reader->GetMaxSequence();
                
                AddSSTable(meta);
            }
        }
    }
    
    closedir(dir);
    next_file_number_.store(max_file_number + 1);
    
    return Status::OK();
}

std::shared_ptr<SSTableMetadata> SSTableManager::CreateFromMemTable(
    const MemTable& memtable, int level) {
    
    FileNumber file_num = AllocateFileNumber();
    std::string filename = util::SSTableFileName(db_path_, file_num, level);
    
    SSTableBuilder builder(filename, file_num, level);
    Status s = builder.Open();
    if (!s.ok()) {
        return nullptr;
    }
    
    // Iterate through MemTable and write to SSTable
    auto iter = memtable.NewIterator();
    while (iter.Valid()) {
        s = builder.Add(iter.GetKey(), iter.GetValue());
        if (!s.ok()) {
            builder.Abandon();
            return nullptr;
        }
        iter.Next();
    }
    
    s = builder.Finish();
    if (!s.ok()) {
        return nullptr;
    }
    
    // Create metadata
    auto meta = std::make_shared<SSTableMetadata>();
    meta->file_number = file_num;
    meta->level = level;
    meta->filename = filename;
    meta->file_size = builder.GetFileSize();
    meta->num_entries = builder.GetNumEntries();
    
    // Open to get key range
    auto reader = std::make_shared<SSTableReader>(filename);
    s = reader->Open();
    if (s.ok()) {
        meta->smallest_key = reader->GetSmallestKey();
        meta->largest_key = reader->GetLargestKey();
        meta->min_sequence = reader->GetMinSequence();
        meta->max_sequence = reader->GetMaxSequence();
    }
    
    AddSSTable(meta);
    return meta;
}

void SSTableManager::AddSSTable(std::shared_ptr<SSTableMetadata> meta) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    if (meta->level >= static_cast<int>(levels_.size())) {
        levels_.resize(meta->level + 1);
    }
    
    levels_[meta->level].push_back(meta);
    sstables_[meta->file_number] = meta;
}

Status SSTableManager::RemoveSSTable(FileNumber file_number) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    auto it = sstables_.find(file_number);
    if (it == sstables_.end()) {
        return Status::NotFound("SSTable not found");
    }
    
    auto meta = it->second;
    
    // Remove from level
    auto& level_tables = levels_[meta->level];
    level_tables.erase(
        std::remove_if(level_tables.begin(), level_tables.end(),
                       [file_number](const auto& m) {
                           return m->file_number == file_number;
                       }),
        level_tables.end());
    
    // Remove from map
    sstables_.erase(it);
    
    // Remove from cache
    reader_cache_.erase(file_number);
    
    // Delete file
    if (unlink(meta->filename.c_str()) != 0 && errno != ENOENT) {
        return Status::IOError("Failed to delete SSTable: " + meta->filename);
    }
    
    return Status::OK();
}

std::vector<std::shared_ptr<SSTableMetadata>> SSTableManager::GetSSTables(
    int level) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    if (level >= static_cast<int>(levels_.size())) {
        return {};
    }
    return levels_[level];
}

size_t SSTableManager::GetTotalSSTables() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return sstables_.size();
}

size_t SSTableManager::GetLevelSize(int level) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    if (level >= static_cast<int>(levels_.size())) {
        return 0;
    }
    
    size_t total = 0;
    for (const auto& meta : levels_[level]) {
        total += meta->file_size;
    }
    return total;
}

std::shared_ptr<SSTableReader> SSTableManager::OpenSSTable(
    FileNumber file_number) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    // Check cache first
    auto cache_it = reader_cache_.find(file_number);
    if (cache_it != reader_cache_.end()) {
        return cache_it->second;
    }
    
    // Find metadata
    auto it = sstables_.find(file_number);
    if (it == sstables_.end()) {
        return nullptr;
    }
    
    // Open reader
    auto reader = std::make_shared<SSTableReader>(it->second->filename);
    Status s = reader->Open();
    if (!s.ok()) {
        return nullptr;
    }
    
    // Cache it (needs write lock, but we're being lazy here)
    const_cast<std::unordered_map<FileNumber, std::shared_ptr<SSTableReader>>&>(
        reader_cache_)[file_number] = reader;
    
    return reader;
}

Status SSTableManager::Get(const std::string& key, SequenceNumber sequence,
                           std::string* value) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    InternalKey lookup_key(key, sequence, ValueType::VALUE);
    
    // Search levels from 0 (newest) to max
    for (size_t level = 0; level < levels_.size(); level++) {
        const auto& tables = levels_[level];
        
        if (level == 0) {
            // Level 0: tables may overlap, check all from newest to oldest
            // Sort by file number descending (newer files first)
            std::vector<std::shared_ptr<SSTableMetadata>> sorted_tables = tables;
            std::sort(sorted_tables.begin(), sorted_tables.end(),
                      [](const auto& a, const auto& b) {
                          return a->file_number > b->file_number;
                      });
            
            for (const auto& meta : sorted_tables) {
                // Check key range
                if (lookup_key.Encode() < meta->smallest_key ||
                    lookup_key.Encode() > meta->largest_key) {
                    continue;
                }
                
                auto reader = OpenSSTable(meta->file_number);
                if (!reader) continue;
                
                Status s = reader->Get(lookup_key, value);
                if (s.ok() || !s.IsNotFound()) {
                    return s;
                }
            }
        } else {
            // Other levels: tables don't overlap, binary search
            if (tables.empty()) continue;
            
            // Find table that might contain key
            auto it = std::lower_bound(tables.begin(), tables.end(),
                                       lookup_key.Encode(),
                                       [](const auto& meta, const std::string& k) {
                                           return meta->largest_key < k;
                                       });
            
            if (it == tables.end()) continue;
            if (lookup_key.Encode() < (*it)->smallest_key) continue;
            
            auto reader = OpenSSTable((*it)->file_number);
            if (!reader) continue;
            
            Status s = reader->Get(lookup_key, value);
            if (s.ok() || !s.IsNotFound()) {
                return s;
            }
        }
    }
    
    return Status::NotFound();
}

FileNumber SSTableManager::AllocateFileNumber() {
    return next_file_number_.fetch_add(1, std::memory_order_relaxed);
}

}  // namespace lsm
