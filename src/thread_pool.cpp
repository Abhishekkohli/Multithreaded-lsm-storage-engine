/**
 * thread_pool.cpp - Implementation of custom thread pool
 * 
 * This implementation provides:
 * - Worker threads that continuously pull from a priority task queue
 * - Graceful shutdown with task completion
 * - Statistics tracking for monitoring
 * - Lifecycle callbacks for debugging
 */

#include "thread_pool.h"
#include <iostream>
#include <sstream>
#include <iomanip>

namespace lsm {

// ============================================================================
// ThreadPool Implementation
// ============================================================================

ThreadPool::ThreadPool(size_t num_threads, 
                       const std::string& name,
                       ThreadCallbacks callbacks)
    : name_(name)
    , callbacks_(std::move(callbacks)) 
{
    // Spawn worker threads
    // Each thread runs the WorkerLoop function which continuously
    // pulls tasks from the queue and executes them
    workers_.reserve(num_threads);
    for (size_t i = 0; i < num_threads; ++i) {
        workers_.emplace_back(&ThreadPool::WorkerLoop, this, i);
    }
}

ThreadPool::~ThreadPool() {
    // Ensure clean shutdown on destruction
    if (!shutdown_.load(std::memory_order_acquire)) {
        Shutdown();
    }
    
    // Join all worker threads
    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

uint64_t ThreadPool::Submit(Task::TaskFunction fn, 
                            TaskPriority priority,
                            const std::string& name) {
    // Check if pool is accepting new tasks
    if (shutdown_.load(std::memory_order_acquire)) {
        throw std::runtime_error("Cannot submit task to shutdown thread pool");
    }
    
    // Generate unique task ID
    uint64_t task_id = next_task_id_.fetch_add(1, std::memory_order_relaxed);
    
    // Create and enqueue the task
    EnqueueTask(Task(std::move(fn), priority, task_id, name));
    
    return task_id;
}

void ThreadPool::EnqueueTask(Task task) {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        task_queue_.push(std::move(task));
    }
    // Wake up one waiting worker thread
    // Using notify_one is more efficient than notify_all since only
    // one thread can process the task anyway
    queue_cv_.notify_one();
}

void ThreadPool::WaitForAll() {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    
    // Wait until queue is empty AND no tasks are actively running
    // This ensures all submitted work is complete
    completion_cv_.wait(lock, [this]() {
        return task_queue_.empty() && 
               active_tasks_.load(std::memory_order_relaxed) == 0;
    });
}

void ThreadPool::Shutdown() {
    // Set the shutdown flag - workers will drain queue then exit
    shutdown_.store(true, std::memory_order_release);
    
    // Wake all waiting workers so they can check shutdown flag
    queue_cv_.notify_all();
}

void ThreadPool::ShutdownNow() {
    // Set immediate shutdown - workers will exit without draining
    shutdown_immediate_.store(true, std::memory_order_release);
    shutdown_.store(true, std::memory_order_release);
    
    // Clear the task queue under lock
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        while (!task_queue_.empty()) {
            task_queue_.pop();
        }
    }
    
    // Wake all workers
    queue_cv_.notify_all();
    completion_cv_.notify_all();
}

size_t ThreadPool::GetPendingTasks() const {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    return task_queue_.size();
}

void ThreadPool::WorkerLoop(size_t thread_id) {
    // Set thread name for debugging (platform-specific)
    #if defined(__linux__)
        std::string tname = name_ + "_" + std::to_string(thread_id);
        pthread_setname_np(pthread_self(), tname.substr(0, 15).c_str());
    #elif defined(__APPLE__)
        std::string tname = name_ + "_" + std::to_string(thread_id);
        pthread_setname_np(tname.substr(0, 63).c_str());
    #endif
    
    // Invoke startup callback if provided
    if (callbacks_.on_thread_start) {
        callbacks_.on_thread_start(thread_id);
    }
    
    while (true) {
        Task task;
        
        // ================================================================
        // Task acquisition phase - wait for work or shutdown signal
        // ================================================================
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            
            // Wait until: (1) there's a task, OR (2) shutdown requested
            // The predicate prevents spurious wakeups from causing issues
            queue_cv_.wait(lock, [this]() {
                return !task_queue_.empty() || 
                       shutdown_.load(std::memory_order_acquire);
            });
            
            // Check for immediate shutdown - exit without processing more tasks
            if (shutdown_immediate_.load(std::memory_order_acquire)) {
                break;
            }
            
            // Check for graceful shutdown with empty queue
            if (task_queue_.empty()) {
                if (shutdown_.load(std::memory_order_acquire)) {
                    break;
                }
                continue;  // Spurious wakeup, go back to waiting
            }
            
            // Dequeue the highest-priority task
            task = std::move(const_cast<Task&>(task_queue_.top()));
            task_queue_.pop();
        }
        
        // ================================================================
        // Task execution phase - run outside the lock
        // ================================================================
        
        // Increment active task count
        active_tasks_.fetch_add(1, std::memory_order_relaxed);
        
        Timestamp start_time = util::NowMicros();
        
        // Invoke pre-execution callback
        if (callbacks_.on_task_start) {
            callbacks_.on_task_start(thread_id, task);
        }
        
        // Execute the task
        // Wrap in try-catch to prevent exceptions from killing the worker
        try {
            task.Execute();
        } catch (const std::exception& e) {
            // Log exception but continue processing other tasks
            std::cerr << "[" << name_ << "_" << thread_id << "] "
                      << "Task exception: " << e.what() << std::endl;
        } catch (...) {
            std::cerr << "[" << name_ << "_" << thread_id << "] "
                      << "Task threw unknown exception" << std::endl;
        }
        
        Timestamp duration = util::NowMicros() - start_time;
        
        // Invoke post-execution callback
        if (callbacks_.on_task_complete) {
            callbacks_.on_task_complete(thread_id, task, duration);
        }
        
        // Update statistics
        active_tasks_.fetch_sub(1, std::memory_order_relaxed);
        completed_tasks_.fetch_add(1, std::memory_order_relaxed);
        
        // Notify WaitForAll() if queue is now empty and no active tasks
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            if (task_queue_.empty() && 
                active_tasks_.load(std::memory_order_relaxed) == 0) {
                completion_cv_.notify_all();
            }
        }
    }
    
    // Invoke shutdown callback
    if (callbacks_.on_thread_stop) {
        callbacks_.on_thread_stop(thread_id);
    }
}

// ============================================================================
// ThreadPoolManager Implementation
// ============================================================================

ThreadPoolManager::ThreadPoolManager() {
    // Create write pool - handles incoming write requests
    // Uses more threads since writes are latency-sensitive
    ThreadCallbacks write_callbacks;
    write_callbacks.on_thread_start = [](size_t /*id*/) {
        // Could set thread priority here for real-time scheduling
    };
    write_pool_ = std::make_unique<ThreadPool>(
        config::MAX_THREADS / 2, "write", write_callbacks);
    
    // Create flush pool - handles MemTable -> SSTable flushing
    // Fewer threads since flushes are I/O-bound
    flush_pool_ = std::make_unique<ThreadPool>(
        config::FLUSH_THREADS, "flush");
    
    // Create compaction pool - handles background compaction
    // Low thread count since compaction is CPU and I/O intensive
    compaction_pool_ = std::make_unique<ThreadPool>(
        config::COMPACTION_THREADS, "compact");
}

ThreadPoolManager::~ThreadPoolManager() {
    ShutdownAll();
}

void ThreadPoolManager::ShutdownAll() {
    // Shutdown in reverse order of priority
    // Compaction first (least important), writes last (most important)
    if (compaction_pool_) {
        compaction_pool_->Shutdown();
    }
    if (flush_pool_) {
        flush_pool_->Shutdown();
    }
    if (write_pool_) {
        write_pool_->Shutdown();
    }
}

void ThreadPoolManager::WaitForAll() {
    if (write_pool_) write_pool_->WaitForAll();
    if (flush_pool_) flush_pool_->WaitForAll();
    if (compaction_pool_) compaction_pool_->WaitForAll();
}

void ThreadPoolManager::PrintStats() const {
    auto print_pool = [](const std::string& name, const ThreadPool* pool) {
        if (!pool) return;
        std::cout << std::setw(12) << name << ": "
                  << "threads=" << pool->GetNumThreads()
                  << ", pending=" << pool->GetPendingTasks()
                  << ", active=" << pool->GetActiveTasks()
                  << ", completed=" << pool->GetCompletedTasks()
                  << std::endl;
    };
    
    std::cout << "=== Thread Pool Statistics ===" << std::endl;
    print_pool("Write", write_pool_.get());
    print_pool("Flush", flush_pool_.get());
    print_pool("Compaction", compaction_pool_.get());
}

}  // namespace lsm
