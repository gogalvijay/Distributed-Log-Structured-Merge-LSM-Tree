#include <iostream>
#include <ctime>
#include "kvstore.h"

int main() {
    srand(time(0));

    {
        std::cout << "--- Session 1 ---\n";
        KVStore db;
        
        db.put("apple", "red");
        db.put("banana", "yellow");
        db.put("grape", "purple");
        db.put("zebra", "stripe");

        std::cout << "Flushing to disk...\n";
        db.flush();

        db.put("mango", "green_ram_only");
    }

    std::cout << "\n--- Session 2 (Recovery) ---\n";
    {
        KVStore db;
        
        std::string v1 = db.get("banana");
        std::cout << "Get 'banana' (from SST): " << v1 << "\n";

        std::string v2 = db.get("mango");
        std::cout << "Get 'mango' (from WAL): " << v2 << "\n";

        std::string v3 = db.get("apple");
        std::cout << "Get 'apple' (from SST): " << v3 << "\n";
        
        std::string v4 = db.get("nothing");
        std::cout << "Get 'nothing': " << (v4.empty() ? "not found" : v4) << "\n";
    }

    return 0;
}
