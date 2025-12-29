/**
 * @file Barrier.h
 * @brief Thread synchronization barrier implementation
 *
 * This file defines a Barrier class that provides a synchronization point
 * for multiple threads. All threads must reach the barrier before any can proceed.
 */

#ifndef BARRIER_H
#define BARRIER_H
#include <mutex>
#include <condition_variable>

/**
 * @class Barrier
 * @brief A synchronization primitive for coordinating multiple threads
 *
 * The Barrier class allows multiple threads to wait at a synchronization point
 * until all threads have reached it. Once all threads arrive, they are all released
 * to continue execution. This is useful in parallel algorithms where threads need
 * to complete a phase before moving to the next one.
 */
class Barrier {
public:
    /**
     * @brief Construct a new Barrier object
     * @param numThreads The number of threads that must reach the barrier before releasing
     */
    explicit Barrier(int numThreads);

    /**
     * @brief Default destructor
     */
    ~Barrier() = default;

    /**
     * @brief Wait at the barrier until all threads arrive
     *
     * This function blocks the calling thread until numThreads threads have called it.
     * Once the last thread arrives, all waiting threads are released simultaneously.
     * The barrier can be reused for multiple synchronization points.
     */
    void barrier();

private:
    std::mutex mutex;                    // Protects access to count and generation
    std::condition_variable cv;          // Used to block and wake threads
    int count;                           // Current number of threads at the barrier
    int generation;                      // Generation counter to handle barrier reuse
    const int numThreads;                // Total number of threads that must arrive
};

#endif // BARRIER_H