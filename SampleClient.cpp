/**
 * @file SampleClient.cpp
 * @brief Example MapReduce client that counts character frequencies
 * 
 * This sample demonstrates how to implement a MapReduce client that processes
 * text strings and counts the frequency of each character across all inputs.
 * The map phase counts characters in each string, and the reduce phase aggregates
 * counts for each unique character.
 */

#include "MapReduceFramework.h"
#include <cstdio>
#include <string>
#include <array>
#include <unistd.h>

/**
 * @class VString
 * @brief Input value type containing a string
 * 
 * This class wraps a string as an input value (V1) for the MapReduce framework.
 */
class VString : public V1 {
public:
    VString(std::string content) : content(content) { }
    std::string content;  ///< The string content to process
};

/**
 * @class KChar
 * @brief Key type representing a single character
 * 
 * This class serves as both intermediate key (K2) and output key (K3).
 * It represents a single character and implements comparison for sorting.
 */
class KChar : public K2, public K3{
public:
    KChar(char c) : c(c) { }

    /**
     * @brief Comparison operator for K2 (intermediate key)
     */
    virtual bool operator<(const K2 &other) const {
        return c < static_cast<const KChar&>(other).c;
    }

    /**
     * @brief Comparison operator for K3 (output key)
     */
    virtual bool operator<(const K3 &other) const {
        return c < static_cast<const KChar&>(other).c;
    }

    char c;  ///< The character represented by this key
};

/**
 * @class VCount
 * @brief Value type representing a count
 * 
 * This class serves as both intermediate value (V2) and output value (V3).
 * It represents the count/frequency of a character.
 */
class VCount : public V2, public V3{
public:
    VCount(int count) : count(count) { }
    int count;  ///< The count value
};

/**
 * @class CounterClient
 * @brief MapReduce client for counting character frequencies
 * 
 * This client implements the MapReduce pattern to count how many times each
 * character appears across all input strings.
 */
class CounterClient : public MapReduceClient {
public:
    /**
     * @brief Map function that counts characters in a single string
     *
     * This function processes one input string and emits a (character, count)
     * pair for each unique character found in that string.
     *
     * Algorithm:
     * 1. Initialize count array for all 256 possible byte values
     * 2. Count occurrences of each character in the string
     * 3. For each character that appears at least once, emit (char, count)
     *
     * @param key Input key (unused in this example)
     * @param value Input value containing the string to process
     * @param context Thread context for emitting results
     */
    void map(const K1* key, const V1* value, void* context) const {
        // Array to count each possible byte value (0-255)
        std::array<int, 256> counts;
        counts.fill(0);

        // Count each character in the string
        for(const char& c : static_cast<const VString*>(value)->content) {
            counts[(unsigned char) c]++;
        }

        // Emit (character, count) pairs for characters that appear
        for (int i = 0; i < 256; ++i) {
            if (counts[i] == 0)
                continue;

            KChar* k2 = new KChar(i);
            VCount* v2 = new VCount(counts[i]);
            usleep(150000);  // Simulate processing time
            emit2(k2, v2, context);
        }
    }

    /**
     * @brief Reduce function that aggregates counts for a single character
     *
     * This function receives all (character, count) pairs for one unique character
     * and sums up the counts to produce the final frequency.
     *
     * Algorithm:
     * 1. Extract the character from the first pair
     * 2. Sum all count values for this character
     * 3. Clean up intermediate pairs (delete allocated memory)
     * 4. Emit final (character, total_count) pair
     *
     * @param pairs Vector of all intermediate pairs with the same character key
     * @param context Thread context for emitting results
     */
    virtual void reduce(const IntermediateVec* pairs,
                        void* context) const {
        // Get the character this reduce call is processing
        const char c = static_cast<const KChar*>(pairs->at(0).first)->c;
        int count = 0;

        // Sum all counts for this character
        for(const IntermediatePair& pair: *pairs) {
            count += static_cast<const VCount*>(pair.second)->count;
            // Clean up intermediate data
            delete pair.first;
            delete pair.second;
        }

        // Emit final result
        KChar* k3 = new KChar(c);
        VCount* v3 = new VCount(count);
        usleep(150000);  // Simulate processing time
        emit3(k3, v3, context);
    }
};

/**
 * @brief Main function demonstrating the MapReduce framework
 * 
 * This function creates sample input data, starts a MapReduce job to count
 * character frequencies, monitors the job progress, and displays the results.
 * 
 * @return 0 on success
 */
int main(int argc, char** argv)
{
    // Create client and data structures
    CounterClient client;
    InputVec inputVec;
    OutputVec outputVec;

    // Create sample input strings
    VString s1("This string is full of characters");
    VString s2("Multithreading is awesome");
    VString s3("race conditions are bad");

    // Add strings to input vector (key is null for this example)
    inputVec.push_back({nullptr, &s1});
    inputVec.push_back({nullptr, &s2});
    inputVec.push_back({nullptr, &s3});

    // Variables for tracking job state
    JobState state;
    JobState last_state={UNDEFINED_STAGE,1};

    // Start MapReduce job with 4 threads
    JobHandle job = startMapReduceJob(client, inputVec, outputVec, 4);
    getJobState(job, &state);

    // Monitor job progress until completion
    while (state.stage != REDUCE_STAGE || state.percentage != 100.0)
    {
        // Only print when state changes to avoid spam
        if (last_state.stage != state.stage || last_state.percentage != state.percentage){
            printf("stage %d, %f%% \n",
                   state.stage, state.percentage);
        }
        usleep(100000);  // Check every 0.1 seconds
        last_state = state;
        getJobState(job, &state);
    }

    // Print final state
    printf("stage %d, %f%% \n",
           state.stage, state.percentage);
    printf("Done!\n");

    // Wait for job completion and clean up
    closeJobHandle(job);

    // Display results
    for (OutputPair& pair: outputVec) {
        char c = ((const KChar*)pair.first)->c;
        int count = ((const VCount*)pair.second)->count;
        printf("The character %c appeared %d time%s\n",
               c, count, count > 1 ? "s" : "");
        // Clean up output data
        delete pair.first;
        delete pair.second;
    }

    return 0;
}