# MapReduce Framework (C++)

A lightweight **multi-threaded MapReduce framework** in C++ that runs a job in three stages: **Map → Shuffle → Reduce**.
It includes a reusable **Barrier** synchronization primitive and a **SampleClient** that demonstrates character-frequency counting.

## Project Structure

- `MapReduceClient.h`  
  Defines the MapReduce client interface (`map` / `reduce`) and the key/value base types (`K1,V1,K2,V2,K3,V3`).
- `MapReduceFramework.h` / `MapReduceFramework.cpp`  
  The framework implementation: thread management, map/shuffle/reduce flow, synchronization, and progress tracking.
- `Barrier.h` / `Barrier.cpp`  
  A reusable barrier implementation for synchronizing threads between phases.
- `SampleClient.cpp`  
  Example client that counts character occurrences across multiple strings.
- `CMakeLists.txt` (in this repo it may be saved as `.txt`)  
  Minimal CMake configuration to build the sample.

## Requirements

- C++17 compatible compiler (e.g., `g++`, `clang++`)
- CMake 3.22+ (recommended)

## Build & Run

### Using CMake (recommended)

```bash
mkdir -p build
cd build
cmake ..
cmake --build .
./MapReduce
