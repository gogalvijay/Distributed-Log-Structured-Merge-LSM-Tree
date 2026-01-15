#include <iostream>
#include <ctime>
#include "kvstore.h"

int main() {
    srand(time(0));

    KVStore db;

   
    db.recover(); 

  
    std::cout << "\nInserting new data...\n";
    db.put("user:101", "Alice");
    db.put("user:102", "Bob");
    db.put("config:mode", "production");
    db.put("session:x99", "{json:data}");


    std::string result = db.get("user:101");
    std::cout << "Found user:101 => " << result << std::endl;

    
    db.displayList();

    return 0;
}
