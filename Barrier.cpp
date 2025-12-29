/**
 * @file Barrier.cpp
 * @brief Implementation of the Barrier synchronization class
 */

#include "Barrier.h"

/**
 * @brief Constructor initializes the barrier with the specified number of threads
 * @param numThreads The number of threads that must reach the barrier
 */
Barrier::Barrier(int numThreads)
        : count(0)              // Initialize count of arrived threads to 0
        , generation(0)         // Initialize generation counter to 0
        , numThreads(numThreads) // Set the required number of threads
{ }

/**
 * @brief Synchronization point where threads wait for each other
 *
 * This function implements a reusable barrier using the generation pattern.
 * Each time all threads pass through the barrier, the generation is incremented,
 * allowing the barrier to be reused without interference from threads that
 * haven't yet woken from a previous barrier call.
 *
 * Algorithm:
 * 1. Lock the mutex to safely access shared state
 * 2. Save the current generation number
 * 3. Increment the count of arrived threads
 * 4. If not all threads have arrived yet:
 *    - Wait on the condition variable until the generation changes
 * 5. If this is the last thread to arrive:
 *    - Reset count to 0 for the next barrier use
 *    - Increment generation to release waiting threads
 *    - Notify all waiting threads
 */
void Barrier::barrier() {
    std::unique_lock<std::mutex> lock(mutex);
    int gen = generation; // Save current generation to detect when barrier is crossed

    if (++count < numThreads) {
        // Not all threads have arrived yet - wait for generation to change
        // The lambda checks if we've moved to the next generation
        cv.wait(lock, [this, gen] { return gen != generation; });
    } else {
        // Last thread to arrive - release all waiting threads
        count = 0;           // Reset counter for next barrier use
        generation++;        // Move to next generation
        cv.notify_all();     // Wake up all waiting threads
    }
}