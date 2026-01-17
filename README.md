# Multithreaded Log-Structured Storage Engine

A high-performance, thread-safe key-value storage engine implemented in C/C++, inspired by log-structured storage designs like LevelDB and RocksDB.

## Features

- **Log-Structured Merge Tree (LSM)**: Optimized for write-heavy workloads
- **Concurrent Access**: Lock-free reads with fine-grained locking for writes
- **Durability**: Write-Ahead Log (WAL) ensures data survives crashes
- **Background Operations**: Automatic flushing and compaction via thread pools
- **MVCC**: Snapshot isolation for consistent concurrent reads
- **Bloom Filters**: Fast negative lookups to avoid unnecessary disk reads

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Application                               │
├─────────────────────────────────────────────────────────────────┤
│                      Storage Engine API                          │
│                    Put / Get / Delete / Scan                     │
├──────────────────────┬──────────────────────────────────────────┤
│    Write Path        │              Read Path                    │
│                      │                                           │
│  ┌─────────────┐    │    ┌─────────────┐                        │
│  │    WAL      │    │    │  MemTable   │──┐                     │
│  │(Durability) │    │    │ (Skip List) │  │                     │
│  └──────┬──────┘    │    └─────────────┘  │                     │
│         │           │           │         │ Search              │
│         ▼           │           ▼         │ Order               │
│  ┌─────────────┐    │    ┌─────────────┐  │                     │
│  │  MemTable   │    │    │  Immutable  │  │                     │
│  │ (Skip List) │    │    │  MemTables  │──┤                     │
│  └──────┬──────┘    │    └─────────────┘  │                     │
│         │           │           │         │                     │
│    (Flush when      │           ▼         │                     │
│     full)           │    ┌─────────────┐  │                     │
│         │           │    │  L0 SSTs    │──┤                     │
│         ▼           │    └─────────────┘  │                     │
│  ┌─────────────┐    │           │         │                     │
│  │  SSTable    │    │           ▼         │                     │
│  │  (L0-L6)    │◄───│    ┌─────────────┐  │                     │
│  └─────────────┘    │    │  L1-L6 SSTs │──┘                     │
│         │           │    └─────────────┘                        │
│    (Background      │                                           │
│     Compaction)     │                                           │
└──────────────────────┴──────────────────────────────────────────┘
```

## Components

### 1. MemTable (`include/memtable.h`, `src/memtable.cpp`)
In-memory sorted buffer using a skip list data structure.
- O(log n) insert/lookup/delete
- Lock-free reads with mutex for writes
- Automatic flush trigger when size limit reached

### 2. Write-Ahead Log (`include/wal.h`, `src/wal.cpp`)
Append-only log for durability.
- CRC32 checksums for corruption detection
- Block-aligned records for recovery
- Buffered I/O with configurable sync policy

### 3. SSTable (`include/sstable.h`, `src/sstable.cpp`)
Sorted String Table - immutable on-disk storage format.
- Block-based with prefix compression
- Bloom filter for fast negative lookups
- Index block for efficient seeking

### 4. Thread Pool (`include/thread_pool.h`, `src/thread_pool.cpp`)
Custom thread pool with lifecycle management.
- Priority task queue
- Graceful shutdown with task draining
- Separate pools for writes, flushes, and compaction

### 5. Compaction (`include/compaction.h`, `src/compaction.cpp`)
Background merge of SSTables.
- Leveled compaction strategy
- Merge iterator for efficient multi-way merge
- Automatic triggering based on level scores

### 6. Storage Engine (`include/storage_engine.h`, `src/storage_engine.cpp`)
Main interface tying all components together.
- Atomic batch writes
- Snapshot isolation
- Statistics and monitoring

## Building

### Requirements
- C++17 compatible compiler (GCC 7+, Clang 5+)
- POSIX-compatible system (Linux, macOS)
- pthread library

### Build Commands

```bash
# Build release version (optimized)
make

# Build debug version (with sanitizers)
make debug

# Clean build artifacts
make clean

# Build and run
make run

# Show help
make help
```

## Usage

### Basic Example

```cpp
#include "storage_engine.h"
using namespace lsm;

int main() {
    // Open database
    Options options;
    options.create_if_missing = true;
    
    auto [status, engine] = StorageEngine::Open("./mydb", options);
    if (!status.ok()) {
        std::cerr << "Failed: " << status.ToString() << std::endl;
        return 1;
    }
    
    // Put
    engine->Put("key", "value");
    
    // Get
    std::string value;
    status = engine->Get("key", &value);
    if (status.ok()) {
        std::cout << "Value: " << value << std::endl;
    }
    
    // Delete
    engine->Delete("key");
    
    return 0;
}
```

### Batch Writes

```cpp
WriteBatch batch;
batch.Put("key1", "value1");
batch.Put("key2", "value2");
batch.Delete("key3");

WriteOptions options;
options.sync = true;  // Force fsync
engine->Write(batch, options);
```

### Snapshots

```cpp
// Take snapshot
const Snapshot* snap = engine->GetSnapshot();

// Modify data
engine->Put("key", "new_value");

// Read at snapshot (sees old value)
ReadOptions opts;
opts.snapshot = snap->GetSequence();
engine->Get("key", &value, opts);

// Release snapshot
engine->ReleaseSnapshot(snap);
```

## Thread Safety

- **Reads**: Fully concurrent, lock-free
- **Writes**: Serialized through mutex (atomic batch guarantee)
- **Background ops**: Run on separate thread pools

## Synchronization Primitives Used

1. **Atomics** (`std::atomic`)
   - Sequence number counter
   - Statistics counters
   - Shutdown flags
   
2. **Mutexes** (`std::mutex`)
   - Write serialization
   - WAL access
   
3. **Read-Write Locks** (`std::shared_mutex`)
   - MemTable list access
   - SSTable metadata
   
4. **Condition Variables** (`std::condition_variable`)
   - Thread pool task waiting
   - Flush/compaction completion

5. **Custom SpinLock**
   - Low-contention, short-duration locks

## Configuration

Key parameters in `include/common.h`:

```cpp
// MemTable
MEMTABLE_SIZE_LIMIT = 4MB     // Flush trigger

// Thread Pools
MAX_THREADS = 16              // Write pool size
FLUSH_THREADS = 2             // Flush pool size
COMPACTION_THREADS = 2        // Compaction pool size

// Compaction
L0_COMPACTION_TRIGGER = 4     // L0 files before compaction
LEVEL_SIZE_MULTIPLIER = 10    // Size ratio between levels
```

## File Format

### WAL Record
```
[checksum (4B)][length (4B)][type (1B)][data...]
```

### SSTable
```
[Data Block 1][Data Block 2]...[Index Block][Bloom Filter][Footer]
```

### Data Block
```
[entry 1][entry 2]...[restart offsets][num restarts (4B)]
```

## Performance Characteristics

- **Write**: O(log n) MemTable insert + O(1) WAL append
- **Read**: O(log n) MemTable + O(log n) per SSTable level
- **Space Amplification**: ~10x (with leveled compaction)
- **Write Amplification**: ~10x per level

## Testing

The `main.cpp` includes comprehensive tests:

1. **Basic Operations**: Put, Get, Delete, Update
2. **Batch Operations**: Atomic multi-key writes
3. **Concurrent Writers**: Multi-threaded write stress test
4. **Read/Write Mix**: Concurrent readers and writers
5. **Flush/Compaction**: Background operation verification
6. **Snapshots**: MVCC isolation testing

Run tests:
```bash
make run
```

## Project Structure

```
.
├── include/
│   ├── common.h          # Types, constants, utilities
│   ├── thread_pool.h     # Thread pool and sync primitives
│   ├── memtable.h        # Skip list MemTable
│   ├── wal.h             # Write-ahead log
│   ├── sstable.h         # SSTable format
│   ├── compaction.h      # Background compaction
│   └── storage_engine.h  # Main interface
├── src/
│   ├── thread_pool.cpp
│   ├── memtable.cpp
│   ├── wal.cpp
│   ├── sstable.cpp
│   ├── compaction.cpp
│   ├── storage_engine.cpp
│   └── main.cpp          # Demo/test program
├── Makefile
└── README.md
```

## Future Improvements

- [ ] Compression (Snappy, LZ4)
- [ ] Block cache for read optimization
- [ ] Parallel compaction
- [ ] Range deletion
- [ ] Column families
- [ ] Transactions
- [ ] Replication

## References

- [LevelDB Implementation](https://github.com/google/leveldb)
- [RocksDB](https://rocksdb.org/)
- [LSM-tree Paper](https://www.cs.umb.edu/~poneil/lsmtree.pdf)

## License

This project is for educational purposes, demonstrating systems programming concepts in C/C++.
