#include <iostream>
#include <ctime>
#include "kvstore.h"

int main() {
    srand(time(0));

    std::cout << "--- LSM Tree Test (Day 15) ---\n";
    KVStore db;

    // 1. Initial Puts
    db.put("user:1", "Alice");
    db.put("user:2", "Bob");
    db.put("user:3", "Charlie");
    std::cout << "Flushing L0_001...\n";
    db.flush();

    // 2. Delete and Update
    db.del("user:2");
    db.put("user:3", "Charlie_Updated");
    std::cout << "Flushing L0_002 (with Tombstone)...\n";
    db.flush();

    // 3. Check logical state
    std::cout << "Read user:1 (Expected Alice): " << db.get("user:1") << "\n";
    std::cout << "Read user:2 (Expected Deleted/Empty): " << db.get("user:2") << "\n";
    std::cout << "Read user:3 (Expected Updated): " << db.get("user:3") << "\n";

    // 4. Compaction
    std::cout << "\nRunning Compaction (Physical Removal)...\n";
    db.compact();

    std::cout << "Post-Compaction Check:\n";
    std::cout << "Read user:1: " << db.get("user:1") << "\n";
    std::cout << "Read user:2: " << (db.get("user:2").empty() ? "Verified Gone" : "Error") << "\n";

    return 0;
}
