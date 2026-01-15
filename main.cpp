#include <iostream>
#include <ctime>
#include "kvstore.h"

int main() {
    srand(time(0));

    KVStore db;

   
    db.recover(); 

  
    std::cout << "\n new data...\n";
    db.put("user:101", "gogal");
    db.put("user:102", "vijay");
    db.put("hi", "hello");
    db.put("welcome", "{name:gv}");


    std::string result = db.get("user:101");
    std::cout << "found user:101" << result << std::endl;

    
    db.displayList();

    return 0;
}
