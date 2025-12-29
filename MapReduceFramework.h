/**
 * @file MapReduceFramework.h
 * @brief Public API for the MapReduce framework
 * 
 * This file defines the public interface for starting and managing MapReduce jobs.
 * The framework handles thread management, synchronization, and the three main
 * phases: Map, Shuffle, and Reduce.
 */

#ifndef MAPREDUCEFRAMEWORK_H
#define MAPREDUCEFRAMEWORK_H

#include "MapReduceClient.h"

/**
 * @typedef JobHandle
 * @brief Opaque handle to a running MapReduce job
 * 
 * This handle is used to query job state and wait for job completion.
 */
typedef void* JobHandle;

/**
 * @enum stage_t
 * @brief Enumeration of MapReduce job stages
 */
enum stage_t {
    UNDEFINED_STAGE = 0,  ///< Job not yet started
    MAP_STAGE = 1,        ///< Currently executing map phase
    SHUFFLE_STAGE = 2,    ///< Currently executing shuffle phase
    REDUCE_STAGE = 3      ///< Currently executing reduce phase
};

/**
 * @struct JobState
 * @brief Represents the current state of a MapReduce job
 */
typedef struct {
    stage_t stage;      ///< Current stage of execution
    float percentage;   ///< Completion percentage of current stage (0-100)
} JobState;

/**
 * @brief Emit an intermediate key-value pair from the map function
 * 
 * This function is called within the map implementation to produce
 * intermediate results. The framework will handle collecting and
 * sorting these pairs for the shuffle phase.
 * 
 * @param key Intermediate key (K2*)
 * @param value Intermediate value (V2*)
 * @param context Thread context provided to the map function
 */
void emit2(K2* key, V2* value, void* context);

/**
 * @brief Emit an output key-value pair from the reduce function
 * 
 * This function is called within the reduce implementation to produce
 * final output results. The framework will collect these pairs in the
 * output vector.
 * 
 * @param key Output key (K3*)
 * @param value Output value (V3*)
 * @param context Thread context provided to the reduce function
 */
void emit3(K3* key, V3* value, void* context);

/**
 * @brief Start a new MapReduce job
 * 
 * This function creates and starts a MapReduce job with the specified number
 * of threads. The job executes asynchronously and can be monitored using
 * getJobState and waitForJob.
 * 
 * The MapReduce process consists of three phases:
 * 1. Map: Input data is divided among threads, each calling client.map()
 * 2. Shuffle: Intermediate pairs are sorted and grouped by key
 * 3. Reduce: Grouped data is processed by client.reduce()
 * 
 * @param client MapReduce client implementation with map and reduce functions
 * @param inputVec Vector of input key-value pairs to process
 * @param outputVec Vector where output pairs will be stored
 * @param multiThreadLevel Number of worker threads to use
 * @return JobHandle for monitoring and controlling the job
 */
JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel);

/**
 * @brief Wait for a MapReduce job to complete
 * 
 * This function blocks until the specified job has finished all three phases.
 * It can be called multiple times safely - subsequent calls will return immediately.
 * 
 * @param job Handle to the job to wait for
 */
void waitForJob(JobHandle job);

/**
 * @brief Query the current state of a MapReduce job
 * 
 * This function retrieves the current stage and completion percentage of a job.
 * It can be called while the job is running to monitor progress.
 * 
 * @param job Handle to the job to query
 * @param state Pointer to JobState structure to fill with current state
 */
void getJobState(JobHandle job, JobState* state);

/**
 * @brief Release resources associated with a completed job
 * 
 * This function waits for the job to complete (if not already done) and then
 * frees all resources associated with the job. The JobHandle becomes invalid
 * after this call.
 * 
 * @param job Handle to the job to close
 */
void closeJobHandle(JobHandle job);


#endif // MAPREDUCEFRAMEWORK_H