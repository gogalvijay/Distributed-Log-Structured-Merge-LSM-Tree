#include <iostream>
#include <vector>
#include <string>
#include <fstream>
#include <cstdlib>
#include <ctime>
#include <cstring>
#include <cstdint>


using Buffer = std::vector<uint8_t>;

// Helper: Encode 32-bit int to buffer
void encodeLength(Buffer& buffer, uint32_t length) {
    buffer.push_back(static_cast<uint8_t>((length >> 24) & 0xFF));
    buffer.push_back(static_cast<uint8_t>((length >> 16) & 0xFF));
    buffer.push_back(static_cast<uint8_t>((length >> 8) & 0xFF));
    buffer.push_back(static_cast<uint8_t>(length & 0xFF));
}

// Helper: Encode string bytes to buffer
void encodeBytes(Buffer& buffer, const std::string& str) {
    for (char c : str) {
        buffer.push_back(static_cast<uint8_t>(c));
    }
}

// Helper: Decode 32-bit int from buffer
uint32_t decodeLength(const Buffer& buffer, size_t& offset) {
    if (offset + 4 > buffer.size()) throw std::runtime_error("Buffer underflow");
    uint32_t length = 0;
    length |= (static_cast<uint32_t>(buffer[offset + 0]) << 24);
    length |= (static_cast<uint32_t>(buffer[offset + 1]) << 16);
    length |= (static_cast<uint32_t>(buffer[offset + 2]) << 8);
    length |= (static_cast<uint32_t>(buffer[offset + 3]));
    offset += 4;
    return length;
}


std::string decodeBytes(const Buffer& buffer, size_t& offset, uint32_t length) {
    if (offset + length > buffer.size()) throw std::runtime_error("Buffer underflow");
    std::string str;
    for (size_t i = 0; i < length; ++i) {
        str += static_cast<char>(buffer[offset + i]);
    }
    offset += length;
    return str;
}


const int MAX_LEVEL = 6;

struct Node {
    std::string key;   
    std::string value; 
    Node** forward;
    int nodeLevel;

    Node(std::string k, std::string v, int level) {
        key = k;
        value = v;
        nodeLevel = level;
        forward = new Node*[level + 1];
        memset(forward, 0, sizeof(Node*) * (level + 1));
    }

    ~Node() {
        delete[] forward;
    }
};

class KVStore {
private:
    Node* head;
    int currentLevel;
    std::ofstream walFile; 
    const std::string walFileName = "wal.log";

public:
    KVStore() {
        currentLevel = 0;
      
        head = new Node("", "", MAX_LEVEL); 

        
        walFile.open(walFileName, std::ios::out | std::ios::app | std::ios::binary);
        if (!walFile.is_open()) {
            std::cerr << "Failed to open WAL file!" << std::endl;
        }
    }

    ~KVStore() {
     
        if (walFile.is_open()) {
            walFile.close();
        }

       
        Node* curr = head->forward[0];
        while (curr != nullptr) {
            Node* next = curr->forward[0];
            delete curr;
            curr = next;
        }
        delete head;
    }

    int randomLevel() {
        int lvl = 0;
        while ((rand() % 2) == 1 && lvl < MAX_LEVEL) {
            lvl++;
        }
        return lvl;
    }

    
    void put(std::string key, std::string value) {
      
        Buffer logEntry;
        encodeLength(logEntry, key.size());
        encodeBytes(logEntry, key);
        encodeLength(logEntry, value.size());
        encodeBytes(logEntry, value);

        
        if (walFile.is_open()) {
            walFile.write(reinterpret_cast<const char*>(logEntry.data()), logEntry.size());
            walFile.flush();
        }

      
        insertInMemory(key, value);
    }

    
    void insertInMemory(std::string key, std::string value) {
        Node* current = head;
        Node* update[MAX_LEVEL + 1];
        memset(update, 0, sizeof(Node*) * (MAX_LEVEL + 1));

        for (int i = currentLevel; i >= 0; i--) {
            while (current->forward[i] != nullptr && current->forward[i]->key < key) {
                current = current->forward[i];
            }
            update[i] = current;
        }
        current = current->forward[0];

        if (current != nullptr && current->key == key) {
            current->value = value;
            return;
        }

        int rLevel = randomLevel();
        if (rLevel > currentLevel) {
            for (int i = currentLevel + 1; i <= rLevel; i++) {
                update[i] = head;
            }
            currentLevel = rLevel;
        }

        Node* n = new Node(key, value, rLevel);
        for (int i = 0; i <= rLevel; i++) {
            n->forward[i] = update[i]->forward[i];
            update[i]->forward[i] = n;
        }
    }

    std::string get(std::string key) {
        Node* current = head;
        for (int i = currentLevel; i >= 0; i--) {
            while (current->forward[i] != nullptr && current->forward[i]->key < key) {
                current = current->forward[i];
            }
        }
        current = current->forward[0];

        if (current != nullptr && current->key == key) {
            return current->value;
        } else {
            return ""; 
        }
    }

    
    void recover() {
        std::ifstream inFile(walFileName, std::ios::in | std::ios::binary);
        if (!inFile.is_open()) {
            std::cout << "No existing WAL found. Starting fresh.\n";
            return;
        }

        std::cout << "--- Recovering from WAL ---\n";
        
       
        std::vector<uint8_t> fileData((std::istreambuf_iterator<char>(inFile)), std::istreambuf_iterator<char>());
        inFile.close();

        size_t offset = 0;
        int recordsRecovered = 0;

        while (offset < fileData.size()) {
            try {
               
                uint32_t keyLen = decodeLength(fileData, offset);
                std::string key = decodeBytes(fileData, offset, keyLen);

               
                uint32_t valLen = decodeLength(fileData, offset);
                std::string val = decodeBytes(fileData, offset, valLen);

               
                insertInMemory(key, val);
                recordsRecovered++;
            } catch (const std::exception& e) {
                std::cerr << "Corruption detected in WAL, stopping recovery.\n";
                break;
            }
        }
        std::cout << "Recovered " << recordsRecovered << " records.\n";
    }

    void displayList() {
        std::cout << "\n--- Current Database State ---\n";
        for (int i = 0; i <= currentLevel; i++) {
            Node* node = head->forward[i];
            std::cout << "Level " << i << ": ";
            while (node != nullptr) {
                std::cout << "[" << node->key << ":" << node->value << "] -> ";
                node = node->forward[i];
            }
            std::cout << "NULL\n";
        }
        std::cout << "------------------------------\n";
    }
};



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
