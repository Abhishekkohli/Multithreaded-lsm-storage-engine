/**
 * thread_pool.h - Custom thread pool with explicit lifecycle management
 * 
 * This thread pool implementation provides:
 * - Task queuing with priority support
 * - Graceful shutdown with task draining
 * - Named thread pools for different workloads (flush, compaction, etc.)
 * - Thread lifecycle hooks for monitoring
 * - Condition variables for efficient waiting
 */

#ifndef STORAGE_ENGINE_THREAD_POOL_H
#define STORAGE_ENGINE_THREAD_POOL_H

#include "common.h"
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <vector>
#include <functional>
#include <future>
#include <atomic>

namespace lsm {

// ============================================================================
// Task Priority Levels
// ============================================================================

enum class TaskPriority {
    LOW = 0,
    NORMAL = 1,
    HIGH = 2,
    CRITICAL = 3  // Used for shutdown-related tasks
};

// ============================================================================
// Task wrapper with priority and metadata
// ============================================================================

class Task {
public:
    using TaskFunction = std::function<void()>;
    
    Task() : priority_(TaskPriority::NORMAL), id_(0) {}
    
    Task(TaskFunction fn, TaskPriority priority = TaskPriority::NORMAL, 
         uint64_t id = 0, const std::string& name = "")
        : function_(std::move(fn))
        , priority_(priority)
        , id_(id)
        , name_(name)
        , enqueue_time_(util::NowMicros()) {}
    
    void Execute() {
        if (function_) {
            function_();
        }
    }
    
    TaskPriority GetPriority() const { return priority_; }
    uint64_t GetId() const { return id_; }
    const std::string& GetName() const { return name_; }
    Timestamp GetEnqueueTime() const { return enqueue_time_; }
    
    // Comparison for priority queue - higher priority comes first
    bool operator<(const Task& other) const {
        return priority_ < other.priority_;  // Lower value = lower priority
    }

private:
    TaskFunction function_;
    TaskPriority priority_;
    uint64_t id_;
    std::string name_;
    Timestamp enqueue_time_;
};

// ============================================================================
// Thread lifecycle callbacks for monitoring and debugging
// ============================================================================

struct ThreadCallbacks {
    std::function<void(size_t thread_id)> on_thread_start;
    std::function<void(size_t thread_id)> on_thread_stop;
    std::function<void(size_t thread_id, const Task& task)> on_task_start;
    std::function<void(size_t thread_id, const Task& task, Timestamp duration_us)> on_task_complete;
};

// ============================================================================
// ThreadPool - Custom thread pool implementation
// ============================================================================

class ThreadPool {
public:
    /**
     * Create a thread pool with specified number of threads
     * @param num_threads Number of worker threads
     * @param name Name for this pool (for debugging)
     * @param callbacks Optional lifecycle callbacks
     */
    explicit ThreadPool(size_t num_threads, 
                        const std::string& name = "worker",
                        ThreadCallbacks callbacks = {});
    
    // Non-copyable, non-movable
    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;
    
    /**
     * Destructor - initiates graceful shutdown
     */
    ~ThreadPool();
    
    /**
     * Submit a task to the thread pool
     * @param fn Function to execute
     * @param priority Task priority
     * @param name Optional task name for debugging
     * @return Task ID
     */
    uint64_t Submit(Task::TaskFunction fn, 
                    TaskPriority priority = TaskPriority::NORMAL,
                    const std::string& name = "");
    
    /**
     * Submit a task and get a future for the result
     * @param fn Function returning T
     * @return std::future<T> for the result
     */
    template<typename F, typename... Args>
    auto SubmitWithFuture(F&& fn, Args&&... args) 
        -> std::future<typename std::invoke_result<F, Args...>::type>;
    
    /**
     * Wait for all currently queued tasks to complete
     */
    void WaitForAll();
    
    /**
     * Initiate graceful shutdown - completes queued tasks then stops
     */
    void Shutdown();
    
    /**
     * Initiate immediate shutdown - discards pending tasks
     */
    void ShutdownNow();
    
    /**
     * Check if shutdown has been initiated
     */
    bool IsShutdown() const { return shutdown_.load(std::memory_order_acquire); }
    
    /**
     * Get number of worker threads
     */
    size_t GetNumThreads() const { return workers_.size(); }
    
    /**
     * Get number of pending tasks
     */
    size_t GetPendingTasks() const;
    
    /**
     * Get number of active (running) tasks
     */
    size_t GetActiveTasks() const { return active_tasks_.load(std::memory_order_relaxed); }
    
    /**
     * Get total tasks completed since pool creation
     */
    uint64_t GetCompletedTasks() const { return completed_tasks_.load(std::memory_order_relaxed); }
    
    /**
     * Get pool name
     */
    const std::string& GetName() const { return name_; }

private:
    /**
     * Worker thread main loop
     * Continuously pulls tasks from queue and executes them
     */
    void WorkerLoop(size_t thread_id);
    
    /**
     * Internal method to add task to queue
     */
    void EnqueueTask(Task task);
    
    std::string name_;
    std::vector<std::thread> workers_;
    
    // Task queue protected by mutex
    // Using priority_queue to respect task priorities
    mutable std::mutex queue_mutex_;
    std::priority_queue<Task> task_queue_;
    
    // Condition variable for worker threads to wait on
    std::condition_variable queue_cv_;
    
    // Condition variable for WaitForAll() callers
    std::condition_variable completion_cv_;
    
    // Atomic flags for shutdown coordination
    std::atomic<bool> shutdown_{false};           // Graceful shutdown initiated
    std::atomic<bool> shutdown_immediate_{false}; // Immediate shutdown (discard tasks)
    
    // Statistics (atomic for lock-free reads)
    std::atomic<size_t> active_tasks_{0};
    std::atomic<uint64_t> completed_tasks_{0};
    std::atomic<uint64_t> next_task_id_{1};
    
    // Lifecycle callbacks
    ThreadCallbacks callbacks_;
};

// ============================================================================
// Template implementation for SubmitWithFuture
// ============================================================================

template<typename F, typename... Args>
auto ThreadPool::SubmitWithFuture(F&& fn, Args&&... args) 
    -> std::future<typename std::invoke_result<F, Args...>::type> 
{
    using return_type = typename std::invoke_result<F, Args...>::type;
    
    // Create a packaged_task to wrap the function
    auto task = std::make_shared<std::packaged_task<return_type()>>(
        std::bind(std::forward<F>(fn), std::forward<Args>(args)...)
    );
    
    std::future<return_type> result = task->get_future();
    
    // Wrap in a void function and submit
    Submit([task]() { (*task)(); }, TaskPriority::NORMAL);
    
    return result;
}

// ============================================================================
// ThreadPoolManager - Manages multiple pools for different workloads
// ============================================================================

class ThreadPoolManager {
public:
    /**
     * Initialize thread pools for the storage engine
     * Creates separate pools for:
     * - Write ingestion (high-priority, latency-sensitive)
     * - Flush operations (medium-priority)
     * - Compaction (low-priority, background)
     */
    ThreadPoolManager();
    ~ThreadPoolManager();
    
    ThreadPoolManager(const ThreadPoolManager&) = delete;
    ThreadPoolManager& operator=(const ThreadPoolManager&) = delete;
    
    /**
     * Get the write pool for ingesting data
     */
    ThreadPool* GetWritePool() { return write_pool_.get(); }
    
    /**
     * Get the flush pool for flushing MemTables to disk
     */
    ThreadPool* GetFlushPool() { return flush_pool_.get(); }
    
    /**
     * Get the compaction pool for background compaction
     */
    ThreadPool* GetCompactionPool() { return compaction_pool_.get(); }
    
    /**
     * Shutdown all pools gracefully
     */
    void ShutdownAll();
    
    /**
     * Wait for all pools to complete pending work
     */
    void WaitForAll();
    
    /**
     * Print pool statistics
     */
    void PrintStats() const;

private:
    std::unique_ptr<ThreadPool> write_pool_;
    std::unique_ptr<ThreadPool> flush_pool_;
    std::unique_ptr<ThreadPool> compaction_pool_;
};

// ============================================================================
// Synchronization utilities
// ============================================================================

/**
 * CountDownLatch - Allows threads to wait for a count to reach zero
 * Useful for waiting for multiple concurrent operations to complete
 */
class CountDownLatch {
public:
    explicit CountDownLatch(size_t count) : count_(count) {}
    
    /**
     * Decrement the count by one
     */
    void CountDown() {
        std::unique_lock<std::mutex> lock(mutex_);
        if (count_ > 0) {
            --count_;
            if (count_ == 0) {
                cv_.notify_all();
            }
        }
    }
    
    /**
     * Wait until count reaches zero
     */
    void Wait() {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this]() { return count_ == 0; });
    }
    
    /**
     * Wait with timeout
     * @return true if count reached zero, false if timed out
     */
    bool WaitFor(std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, timeout, [this]() { return count_ == 0; });
    }
    
    /**
     * Get current count
     */
    size_t GetCount() const {
        std::unique_lock<std::mutex> lock(mutex_);
        return count_;
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    size_t count_;
};

/**
 * Semaphore - Limits concurrent access to a resource
 */
class Semaphore {
public:
    explicit Semaphore(size_t initial_count) : count_(initial_count) {}
    
    /**
     * Acquire one permit (blocks if none available)
     */
    void Acquire() {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this]() { return count_ > 0; });
        --count_;
    }
    
    /**
     * Try to acquire without blocking
     * @return true if acquired, false otherwise
     */
    bool TryAcquire() {
        std::unique_lock<std::mutex> lock(mutex_);
        if (count_ > 0) {
            --count_;
            return true;
        }
        return false;
    }
    
    /**
     * Release one permit
     */
    void Release() {
        std::unique_lock<std::mutex> lock(mutex_);
        ++count_;
        cv_.notify_one();
    }
    
    /**
     * Get current count
     */
    size_t GetCount() const {
        std::unique_lock<std::mutex> lock(mutex_);
        return count_;
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    size_t count_;
};

/**
 * ReadWriteLock - Allows multiple readers or single writer
 * Writers have priority to prevent starvation
 */
class ReadWriteLock {
public:
    ReadWriteLock() : readers_(0), writer_(false), pending_writers_(0) {}
    
    void ReadLock() {
        std::unique_lock<std::mutex> lock(mutex_);
        // Wait if there's a writer or pending writers (writer priority)
        cv_.wait(lock, [this]() { 
            return !writer_ && pending_writers_ == 0; 
        });
        ++readers_;
    }
    
    void ReadUnlock() {
        std::unique_lock<std::mutex> lock(mutex_);
        --readers_;
        if (readers_ == 0) {
            cv_.notify_all();
        }
    }
    
    void WriteLock() {
        std::unique_lock<std::mutex> lock(mutex_);
        ++pending_writers_;
        cv_.wait(lock, [this]() { 
            return !writer_ && readers_ == 0; 
        });
        --pending_writers_;
        writer_ = true;
    }
    
    void WriteUnlock() {
        std::unique_lock<std::mutex> lock(mutex_);
        writer_ = false;
        cv_.notify_all();
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    size_t readers_;
    bool writer_;
    size_t pending_writers_;
};

/**
 * RAII guards for ReadWriteLock
 */
class ReadLockGuard {
public:
    explicit ReadLockGuard(ReadWriteLock& lock) : lock_(lock) {
        lock_.ReadLock();
    }
    ~ReadLockGuard() { lock_.ReadUnlock(); }
    
    ReadLockGuard(const ReadLockGuard&) = delete;
    ReadLockGuard& operator=(const ReadLockGuard&) = delete;

private:
    ReadWriteLock& lock_;
};

class WriteLockGuard {
public:
    explicit WriteLockGuard(ReadWriteLock& lock) : lock_(lock) {
        lock_.WriteLock();
    }
    ~WriteLockGuard() { lock_.WriteUnlock(); }
    
    WriteLockGuard(const WriteLockGuard&) = delete;
    WriteLockGuard& operator=(const WriteLockGuard&) = delete;

private:
    ReadWriteLock& lock_;
};

}  // namespace lsm

#endif  // STORAGE_ENGINE_THREAD_POOL_H
