#include <iostream>
#include <string>
#include <cstdlib>
#include "kvstore.h" 


const int TEST_COUNT = 100;

void runCrashTest() {
    std::cout << "--- [TEST] Phase 1: Writing data & crashing ---\n";
    KVStore db;
    
    
    for (int i = 0; i < TEST_COUNT; i++) {
        std::string key = "key:" + std::to_string(i);
        std::string val = "val:" + std::to_string(i);
        db.put(key, val);
    }
    
    std::cout << "Data inserted. Simulating HARD CRASH (aborting process)...\n";
    
  
    std::abort(); 
}

void runRecoveryTest() {
    std::cout << "--- [TEST] Phase 2: Recovering & Verifying ---\n";
    KVStore db;
    
    
    db.recover();

   
    int foundCount = 0;
    for (int i = 0; i < TEST_COUNT; i++) {
        std::string key = "key:" + std::to_string(i);
        std::string expectedVal = "val:" + std::to_string(i);
        
        std::string actualVal = db.get(key);
        
        if (actualVal == expectedVal) {
            foundCount++;
        } else {
            std::cerr << "Mismatch! Key: " << key << " | Expected: " << expectedVal << " | Got: " << actualVal << "\n";
        }
    }

    
    if (foundCount == TEST_COUNT) {
        std::cout << "\n✅ SUCCESS: All " << foundCount << " keys recovered successfully!\n";
        std::cout << "   (Your Write-Ahead Log is working correctly)\n";
    } else {
        std::cout << "\n❌ FAILURE: Only recovered " << foundCount << "/" << TEST_COUNT << " keys.\n";
    }
}

int main(int argc, char* argv[]) {
    
    if (argc < 2) {
        std::cerr << "Usage: ./test_day5 [write|read]\n";
        return 1;
    }

    std::string mode = argv[1];

    if (mode == "write") {
        runCrashTest();
    } 
    else if (mode == "read") {
        runRecoveryTest();
    }
    else {
        std::cerr << "Unknown mode. Use 'write' or 'read'.\n";
    }

    return 0;
}
