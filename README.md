Distributed-Log-Structured-Merge-LSM-Tree

A high-performance, persistent key-value storage engine built in C++. This project implements an LSM Tree, a data structure optimized for write-heavy workloads, featuring a multi-layered architecture to balance memory speed with disk durability.
🏗 System Architecture

The engine is divided into a volatile memory layer and a persistent disk layer to ensure both speed and data safety.
Components:

    Write-Ahead Log (WAL): Every operation is logged to a sequential file before being applied to memory, ensuring 100% data recovery after a crash.

    MemTable (Skip List): A probabilistic, sorted data structure that provides O(logN) search and insertion performance.

    SSTables (Sorted String Tables): Immutable disk files containing sorted key-value pairs.

    Bloom Filter: A probabilistic structure stored in each SSTable used to skip files that definitely do not contain a specific key, drastically reducing disk I/O.

    Sparse Index: A memory-resident index that maps a subset of keys to their byte offsets, allowing for rapid binary searching within large files.

🚀 Core Functionality (Completed)

    Efficient Writes: Sequential appends to the WAL and in-memory Skip List updates.

    Binary Serialization: Custom [KeyLen][Key][ValLen][Val] format for efficient variable-length storage.

    Point Lookups: Optimized search path: MemTable → Bloom Filter → Sparse Index → Disk Seek.

    Logical Deletes: "Tombstone" implementation to mark data for removal without immediate rewrites.

    Compaction Engine: Automatic merging of multiple SSTables to deduplicate data and physically purge deleted keys.

🔮 Future Roadmap 
The core storage engine is complete. The following features are planned for the next evolution of this project:

    Concurrency Control: Implementation of std::shared_mutex to support thread-safe parallel reads and single-writer access.

    Distributed Networking: Wrapping the engine in a TCP/IP server with a custom binary protocol to allow remote client connections.

    Benchmarking: Detailed analysis of "Write Amplification" and p99 latency percentiles.

    Advanced Compaction: Moving to a Leveled Compaction strategy (LevelDB/RocksDB style) for improved disk space management.

🛠 Usage & Testing
Compiling
Bash

g++ -std=c++17 main.cpp -o lsm_tree_db

Running
Bash

./lsm_tree_db
