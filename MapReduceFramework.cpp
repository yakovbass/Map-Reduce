/**
 * @file MapReduceFramework.cpp
 * @brief Implementation of the MapReduce framework
 *
 * This file implements a multi-threaded MapReduce framework that processes
 * data in three phases: Map, Shuffle, and Reduce. The framework manages
 * thread synchronization, work distribution, and progress tracking.
 */

#include "MapReduceFramework.h"
#include "Barrier.h"

#include <thread>
#include <atomic>
#include <algorithm>
#include <iostream>

/**
 * @struct JobContext
 * @brief Contains all shared state for a MapReduce job
 *
 * This structure holds all the data needed to manage a MapReduce job,
 * including thread handles, synchronization primitives, and progress counters.
 */
struct JobContext {
    std::vector<std::thread> threads;                  // Worker threads
    const MapReduceClient &client;                     // Client implementation
    const InputVec &inputVec;                          // Input data
    OutputVec &outputVec;                              // Output results
    std::vector<IntermediateVec> intermidiate_queue;   // Shuffled intermediate data
    std::vector<IntermediateVec> intermediateVecBuf;   // Per-thread intermediate buffers
    std::vector<size_t> max_indexes;                   // Helper for shuffle phase
    std::atomic<JobState> state;                       // Current job state

    // Mutexes for thread-safe operations
    std::mutex mutexEmit2;      // Protects emit2 operations (currently unused)
    std::mutex mutexEmit3;      // Protects emit3 operations
    std::mutex mutexState;      // Protects state updates

    Barrier barrier;            // Synchronizes threads between phases

    // Atomic counters for progress tracking
    std::atomic<int> *map_counter;                     // Number of mapped input pairs
    std::atomic<int> *shuffle_process;                 // Number of shuffled intermediate pairs
    std::atomic<int> *reduce_counter;                  // Number of reduced output pairs
    std::atomic<int> *numOfIntermidaryElements;        // Total intermediate pairs created
    std::atomic<int> *vector_after_shuffle_counter;    // Number of unique keys after shuffle

    // Flags to ensure one-time initialization
    std::atomic_flag waitForJobFlag;    // Ensures waitForJob is called only once
    std::atomic_flag mapStageSet;       // Ensures map stage is set once
    std::atomic_flag shuffleStageSet;   // Ensures shuffle stage is set once
    std::atomic_flag reduceStageSet;    // Ensures reduce stage is set once

    /**
     * @brief Constructor initializes job context with necessary resources
     * @param client MapReduce client implementation
     * @param inputVec Input data to process
     * @param outputVec Vector for storing output
     * @param multiThreadLevel Number of worker threads
     */
    JobContext(const MapReduceClient &client, const InputVec &inputVec, OutputVec &outputVec, int multiThreadLevel) :
            client(client), inputVec(inputVec), outputVec(outputVec), state({UNDEFINED_STAGE, 0}), barrier(multiThreadLevel) {
        this->threads = std::vector<std::thread>(multiThreadLevel);
        this->intermediateVecBuf = std::vector<IntermediateVec>(multiThreadLevel);

        // Allocate atomic counters dynamically
        try {
            this->numOfIntermidaryElements = new std::atomic<int>(0);
            this->vector_after_shuffle_counter = new std::atomic<int>(0);
            this->map_counter = new std::atomic<int>(0);
            this->shuffle_process = new std::atomic<int>(0);
            this->reduce_counter = new std::atomic<int>(0);
        } catch (const std::bad_alloc &e) {
            std::cout << "system error: " << e.what() << std::endl;
            exit(1);
        }

        // Initialize atomic flags to clear state
        this->waitForJobFlag.clear();
        this->mapStageSet.clear();
        this->shuffleStageSet.clear();
        this->reduceStageSet.clear();
    }
};

/**
 * @struct ThreadContext
 * @brief Contains thread-specific context information
 *
 * Each worker thread receives its own ThreadContext with a pointer
 * to the shared JobContext and its unique thread ID.
 */
struct ThreadContext {
    JobContext *jc;      // Pointer to shared job context
    size_t threadID;     // Unique ID for this thread
};

/**
 * @brief Emit an intermediate key-value pair (called from map function)
 *
 * This function adds an intermediate pair to the thread's local buffer.
 * Each thread has its own buffer to avoid contention during the map phase.
 *
 * @param key Intermediate key (K2*)
 * @param value Intermediate value (V2*)
 * @param context Thread context
 */
void emit2(K2 *key, V2 *value, void *context) {
    auto *tc = static_cast<ThreadContext *>(context);
    // Add pair to this thread's intermediate buffer
    tc->jc->intermediateVecBuf[tc->threadID].emplace_back(key, value);
    // Increment global counter for progress tracking
    ++(*tc->jc->numOfIntermidaryElements);
}

/**
 * @brief Emit an output key-value pair (called from reduce function)
 *
 * This function adds an output pair to the shared output vector.
 * Access is protected by a mutex since multiple threads may call this concurrently.
 *
 * @param key Output key (K3*)
 * @param value Output value (V3*)
 * @param context Thread context
 */
void emit3(K3 *key, V3 *value, void *context) {
    auto *tc = static_cast<ThreadContext *>(context);
    tc->jc->mutexEmit3.lock();
    OutputPair pair = {key, value};
    ++(*tc->jc->reduce_counter);
    tc->jc->outputVec.emplace_back(pair);
    tc->jc->mutexEmit3.unlock();
}

/**
 * @brief Check if all intermediate vectors are empty
 * @param vec Vector of intermediate vectors to check
 * @return true if all vectors are empty, false otherwise
 */
bool isEmptyVec(const std::vector<IntermediateVec> &vec) {
    for (const auto &vector: vec) {
        if (!vector.empty()) {
            return false;
        }
    }
    return true;
}

/**
 * @brief Execute the map phase for a range of input data
 *
 * This function processes a subset of the input data by calling the client's
 * map function for each input pair. Results are stored in a thread-local buffer
 * and then sorted by key.
 *
 * @param i Thread ID
 * @param start Start index in input vector (inclusive)
 * @param end End index in input vector (exclusive)
 * @param threadCtx Thread context
 */
void mapStage(size_t i, size_t start, size_t end, ThreadContext *threadCtx) {
    // Set job state to MAP_STAGE (only once, by first thread to arrive)
    if (!threadCtx->jc->mapStageSet.test_and_set()) {
        JobState s = threadCtx->jc->state.load();
        s.stage = MAP_STAGE;
        s.percentage = 0;
        threadCtx->jc->state.store(s);
    }

    // Process assigned input pairs
    for (size_t j = start; j < end; j++) {
        InputPair pair = threadCtx->jc->inputVec[j];
        // Call client's map function
        threadCtx->jc->client.map(pair.first, pair.second, threadCtx);
        ++(*threadCtx->jc->map_counter);

        // Update progress percentage
        threadCtx->jc->mutexState.lock();
        float done;
        try {
            done = threadCtx->jc->map_counter->load();
        }
        catch (const std::exception &e) {
            std::cout << "system error: " << e.what() << std::endl;
            exit(1);
        }
        JobState s1 = threadCtx->jc->state.load();
        float total = threadCtx->jc->inputVec.size();
        float ptc = done / total * 100.0f;
        s1.percentage = ptc;
        threadCtx->jc->state.store(s1);
        threadCtx->jc->mutexState.unlock();
    }

    // Sort this thread's intermediate pairs by key
    auto &buf = threadCtx->jc->intermediateVecBuf[i];
    std::sort(buf.begin(), buf.end(),
              [](const IntermediatePair &a, const IntermediatePair &b) {
                  return *a.first < *b.first;
              });
}

/**
 * @brief Execute the shuffle phase
 *
 * This function groups all intermediate pairs by key. It repeatedly finds
 * the maximum key across all thread buffers and collects all pairs with that key.
 * The result is a vector of intermediate vectors, where each vector contains
 * all pairs for a unique key.
 *
 * Only thread 0 executes this phase while other threads wait at the barrier.
 *
 * @param threadCtx Thread context
 */
void shuffleStage(ThreadContext *threadCtx) {
    // Set job state to SHUFFLE_STAGE (only once)
    if (!threadCtx->jc->shuffleStageSet.test_and_set()) {
        JobState s2 = threadCtx->jc->state.load();
        s2.stage = SHUFFLE_STAGE;
        s2.percentage = 0;
        threadCtx->jc->state.store(s2);
    }

    // Process all intermediate pairs
    while (!isEmptyVec(threadCtx->jc->intermediateVecBuf)) {
        // Find maximum key across all thread buffers
        K2 *max_key = nullptr;
        for (size_t k = 0; k < threadCtx->jc->intermediateVecBuf.size(); k++) {
            if (!threadCtx->jc->intermediateVecBuf[k].empty()) {
                K2 *current_K = threadCtx->jc->intermediateVecBuf[k].back().first;
                if (max_key == nullptr || *max_key < *current_K) {
                    // Found a new maximum key
                    max_key = current_K;
                    threadCtx->jc->max_indexes.clear();
                    threadCtx->jc->max_indexes.push_back(k);
                    continue;
                }
                // Check if current key equals max key (using equivalence relation)
                if (!(*max_key < *current_K) && !(*current_K < *max_key)) {
                    threadCtx->jc->max_indexes.push_back(k);
                }
            }
        }

        // Collect all pairs with the maximum key
        IntermediateVec current_intermediate;
        for (size_t maxInd: threadCtx->jc->max_indexes) {
            auto &y = threadCtx->jc->intermediateVecBuf[maxInd];
            if (!y.empty()) {
                IntermediatePair current_pair = y.back();
                // Pop all pairs with this key from this buffer
                while (!y.empty() && max_key != nullptr &&
                       !(*max_key < *current_pair.first) &&
                       !(*current_pair.first < *max_key)) {
                    current_intermediate.push_back(current_pair);
                    ++(*threadCtx->jc->shuffle_process);
                    y.pop_back();
                    if (!y.empty()) {
                        current_pair = y.back();
                    }
                }
            }
        }

        // Add grouped pairs to shuffle queue
        threadCtx->jc->max_indexes.clear();
        threadCtx->jc->intermidiate_queue.push_back(current_intermediate);
        ++(*threadCtx->jc->vector_after_shuffle_counter);

        // Update progress percentage
        threadCtx->jc->mutexState.lock();
        float num, num2;
        try {
            num = static_cast<float>(threadCtx->jc->shuffle_process->load());
            num2 = static_cast<float>(threadCtx->jc->numOfIntermidaryElements->load());
        } catch (const std::exception &e) {
            std::cout << "system error: " << e.what() << std::endl;
            exit(1);
        }
        JobState s3 = threadCtx->jc->state.load();
        s3.percentage = (num * 100.0f) / num2;
        threadCtx->jc->state.store(s3);
        threadCtx->jc->mutexState.unlock();
    }
}

/**
 * @brief Execute the reduce phase for assigned intermediate vectors
 *
 * This function processes a subset of the shuffled intermediate data by calling
 * the client's reduce function for each group of pairs sharing the same key.
 * Work is distributed evenly among threads.
 *
 * @param i Thread ID
 * @param threadCtx Thread context
 */
void reduceStage(size_t i, ThreadContext *threadCtx) {
    // Set job state to REDUCE_STAGE (only once)
    if (!threadCtx->jc->reduceStageSet.test_and_set()) {
        JobState s4 = threadCtx->jc->state.load();
        s4.stage = REDUCE_STAGE;
        s4.percentage = 0;
        threadCtx->jc->state.store(s4);
    }

    // Calculate this thread's work range
    size_t numBuckets = threadCtx->jc->intermidiate_queue.size();
    size_t numThreads = threadCtx->jc->threads.size();
    size_t base = numBuckets / numThreads;
    size_t rem = numBuckets % numThreads;

    // Distribute remainder evenly among first 'rem' threads
    size_t reduceStart = i * base + (i < rem ? i : rem);
    size_t reduceEnd = reduceStart + base + (i < rem ? 1 : 0);

    // Process assigned intermediate vectors
    for (size_t idx = reduceStart; idx < reduceEnd; ++idx) {
        IntermediateVec localPairs = std::move(threadCtx->jc->intermidiate_queue[idx]);
        // Call client's reduce function
        threadCtx->jc->client.reduce(&localPairs, threadCtx);

        // Update progress percentage
        threadCtx->jc->mutexState.lock();
        float done, total;
        try {
            done = static_cast<float>(threadCtx->jc->reduce_counter->load());
            total = static_cast<float>(threadCtx->jc->intermidiate_queue.size());
        } catch (const std::exception &e) {
            std::cout << "system error: " << e.what() << std::endl;
            exit(1);
        }
        JobState s5 = threadCtx->jc->state.load();
        s5.percentage = total > 0
                        ? ((done * 100.0f) / total)
                        : 100.0f;
        threadCtx->jc->state.store(s5);
        threadCtx->jc->mutexState.unlock();
    }
}

/**
 * @brief Main worker thread function
 *
 * This function orchestrates the three phases of MapReduce for each worker thread:
 * 1. Map phase: Process assigned input data
 * 2. Shuffle phase: Only thread 0 groups intermediate data by key
 * 3. Reduce phase: Process assigned intermediate data
 *
 * Barriers ensure all threads complete each phase before proceeding to the next.
 *
 * @param i Thread ID
 * @param start Start index for map phase
 * @param end End index for map phase
 * @param threadCtx Thread context
 */
void threadWorkerFunction(size_t i, size_t start, size_t end, ThreadContext *threadCtx) {
    // Execute Map Stage
    mapStage(i, start, end, threadCtx);

    // Wait for all threads to complete map phase
    threadCtx->jc->barrier.barrier();

    // Execute Shuffle Stage (only thread 0)
    if (i == 0) {
        shuffleStage(threadCtx);
    }

    // Wait for shuffle to complete
    threadCtx->jc->barrier.barrier();

    // Execute Reduce Stage
    reduceStage(i, threadCtx);
}

/**
 * @brief Start a new MapReduce job
 *
 * This function initializes job and thread contexts, distributes work among threads,
 * and launches worker threads to execute the MapReduce process.
 *
 * @param client MapReduce client with map and reduce implementations
 * @param inputVec Input data to process
 * @param outputVec Vector for collecting output
 * @param multiThreadLevel Number of worker threads
 * @return JobHandle for monitoring and controlling the job
 */
JobHandle startMapReduceJob(const MapReduceClient &client, const InputVec &inputVec, OutputVec &outputVec, int multiThreadLevel) {
    // Create job context
    auto *job_context = new JobContext(client, inputVec, outputVec, multiThreadLevel);
    auto *thread_context = new ThreadContext[multiThreadLevel];

    // Calculate work distribution for map phase
    size_t totalElements = inputVec.size();
    size_t base_chunk = totalElements / multiThreadLevel;
    size_t remainder = totalElements % multiThreadLevel;

    // Create and launch worker threads
    for (size_t i = 0; i < static_cast<size_t>(multiThreadLevel); i++) {
        thread_context[i] = {job_context, i};

        // Calculate this thread's input range (distribute remainder evenly)
        size_t start = i * base_chunk + (i < remainder ? i : remainder);
        size_t end = start + base_chunk + (i < remainder ? 1 : 0);

        ThreadContext *threadCtx = &thread_context[i];
        try {
            job_context->threads[i] = std::thread(threadWorkerFunction, i, start, end, threadCtx);
        } catch (const std::exception &e) {
            std::cout << "system error: " << e.what() << std::endl;
            exit(1);
        }
    }

    // Return thread context array as job handle
    return static_cast<JobHandle>(thread_context);
}

/**
 * @brief Wait for a MapReduce job to complete
 *
 * This function blocks until all worker threads have finished. It uses an atomic
 * flag to ensure threads are joined only once, even if called multiple times.
 *
 * @param job Job handle returned by startMapReduceJob
 */
void waitForJob(JobHandle job) {
    auto *tc = static_cast<ThreadContext *>(job);
    // Use test_and_set to ensure we only join threads once
    if (!tc->jc->waitForJobFlag.test_and_set()) {
        for (auto &thread: tc->jc->threads) {
            thread.join();
        }
    }
}

/**
 * @brief Query the current state of a job
 *
 * This function retrieves the current stage and completion percentage.
 * It can be called at any time to monitor job progress.
 *
 * @param job Job handle
 * @param state Pointer to JobState structure to fill
 */
void getJobState(JobHandle job, JobState *state) {
    auto *tc = static_cast<ThreadContext *>(job);
    *state = tc->jc->state.load();
}

/**
 * @brief Clean up resources associated with a job
 *
 * This function waits for the job to complete (if not already done) and then
 * frees all allocated memory including atomic counters, contexts, and threads.
 *
 * @param job Job handle to close
 */
void closeJobHandle(JobHandle job) {
    auto *tc = static_cast<ThreadContext *>(job);

    // Ensure job is complete
    waitForJob(job);

    // Free all dynamically allocated resources
    delete tc->jc->numOfIntermidaryElements;
    delete tc->jc->vector_after_shuffle_counter;
    delete tc->jc->map_counter;
    delete tc->jc->shuffle_process;
    delete tc->jc->reduce_counter;
    delete tc->jc;
    delete[] tc;
}