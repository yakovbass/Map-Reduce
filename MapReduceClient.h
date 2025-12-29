/**
 * @file MapReduceClient.h
 * @brief Interface definitions for MapReduce client implementations
 * 
 * This file defines the abstract base classes and type definitions needed
 * to implement a MapReduce client. The MapReduce paradigm processes large
 * datasets by mapping input data to intermediate key-value pairs, then
 * reducing those pairs by key to produce final output.
 */

#ifndef MAPREDUCECLIENT_H
#define MAPREDUCECLIENT_H

#include <vector>  // std::vector
#include <utility> // std::pair

/**
 * @class K1
 * @brief Abstract base class for input keys
 * 
 * Input keys are provided to the map function. They must be comparable
 * to support various operations in the framework.
 */
class K1 {
public:
    virtual ~K1(){}

    /**
     * @brief Comparison operator for ordering keys
     * @param other The key to compare with
     * @return true if this key is less than other, false otherwise
     */
    virtual bool operator<(const K1 &other) const = 0;
};

/**
 * @class V1
 * @brief Abstract base class for input values
 * 
 * Input values are paired with K1 keys and provided to the map function.
 */
class V1 {
public:
    virtual ~V1() {}
};

/**
 * @class K2
 * @brief Abstract base class for intermediate keys
 * 
 * Intermediate keys are emitted by the map function and used by the
 * shuffle phase to group values. They must be comparable for sorting
 * and grouping operations.
 */
class K2 {
public:
    virtual ~K2(){}

    /**
     * @brief Comparison operator for ordering keys
     * @param other The key to compare with
     * @return true if this key is less than other, false otherwise
     */
    virtual bool operator<(const K2 &other) const = 0;
};

/**
 * @class V2
 * @brief Abstract base class for intermediate values
 * 
 * Intermediate values are emitted by the map function and grouped
 * by their associated K2 keys for the reduce phase.
 */
class V2 {
public:
    virtual ~V2(){}
};

/**
 * @class K3
 * @brief Abstract base class for output keys
 * 
 * Output keys are emitted by the reduce function and represent the
 * final keys in the MapReduce result.
 */
class K3 {
public:
    virtual ~K3()  {}

    /**
     * @brief Comparison operator for ordering keys
     * @param other The key to compare with
     * @return true if this key is less than other, false otherwise
     */
    virtual bool operator<(const K3 &other) const = 0;
};

/**
 * @class V3
 * @brief Abstract base class for output values
 * 
 * Output values are emitted by the reduce function and represent the
 * final values in the MapReduce result.
 */
class V3 {
public:
    virtual ~V3() {}
};

// Type definitions for key-value pairs
typedef std::pair<K1*, V1*> InputPair;           // Input to map phase
typedef std::pair<K2*, V2*> IntermediatePair;    // Output of map, input to reduce
typedef std::pair<K3*, V3*> OutputPair;          // Output of reduce phase

// Type definitions for vectors of pairs
typedef std::vector<InputPair> InputVec;              // Collection of input data
typedef std::vector<IntermediatePair> IntermediateVec; // Intermediate data grouped by key
typedef std::vector<OutputPair> OutputVec;            // Final output results

/**
 * @class MapReduceClient
 * @brief Abstract interface for MapReduce operations
 * 
 * Clients must implement this interface to define their specific
 * map and reduce logic. The framework handles parallelization,
 * shuffling, and synchronization.
 */
class MapReduceClient {
public:
    /**
     * @brief Map function processes a single input pair
     *
     * This function is called for each input key-value pair. It should
     * process the input and emit zero or more intermediate key-value pairs
     * using the emit2 function.
     *
     * @param key Input key (K1)
     * @param value Input value (V1)
     * @param context Thread context for emitting intermediate pairs
     */
    virtual void map(const K1* key, const V1* value, void* context) const = 0;

    /**
     * @brief Reduce function processes all values for a single key
     *
     * This function is called for each unique intermediate key after the
     * shuffle phase. It receives all values associated with that key and
     * should emit zero or more output key-value pairs using the emit3 function.
     *
     * @param pairs Vector of all intermediate pairs with the same key
     * @param context Thread context for emitting output pairs
     */
    virtual void reduce(const IntermediateVec* pairs, void* context) const = 0;
};


#endif // MAPREDUCECLIENT_H