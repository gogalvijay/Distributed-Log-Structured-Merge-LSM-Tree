#include <iostream>
#include <vector>
#include <string>
#include <fstream>
#include <cstdlib>
#include <ctime>
#include <cstring>
#include <cstdint>


using Buffer = std::vector<uint8_t>;

inline void encodeLength(Buffer& buffer, uint32_t length) {
    buffer.push_back(static_cast<uint8_t>((length >> 24) & 0xFF));
    buffer.push_back(static_cast<uint8_t>((length >> 16) & 0xFF));
    buffer.push_back(static_cast<uint8_t>((length >> 8) & 0xFF));
    buffer.push_back(static_cast<uint8_t>(length & 0xFF));
}

inline void encodeBytes(Buffer& buffer, const std::string& str) {
    for (char c : str) {
        buffer.push_back(static_cast<uint8_t>(c));
    }
}

inline uint32_t decodeLength(const Buffer& buffer, size_t& offset) {
    if (offset + 4 > buffer.size()) throw std::runtime_error("underflow");
    uint32_t length = 0;
    length |= (static_cast<uint32_t>(buffer[offset + 0]) << 24);
    length |= (static_cast<uint32_t>(buffer[offset + 1]) << 16);
    length |= (static_cast<uint32_t>(buffer[offset + 2]) << 8);
    length |= (static_cast<uint32_t>(buffer[offset + 3]));
    offset += 4;
    return length;
}

inline std::string decodeBytes(const Buffer& buffer, size_t& offset, uint32_t length) {
    if (offset + length > buffer.size()) throw std::runtime_error("underflow");
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
    int sstCounter = 1; 

public:
    KVStore() {
        currentLevel = 0;
        head = new Node("", "", MAX_LEVEL); 
        
        walFile.open(walFileName, std::ios::out | std::ios::app | std::ios::binary);
        if (!walFile.is_open()) {
            std::cerr << "wal not opened" << std::endl;
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
            return;
        }

        std::vector<uint8_t> fileData((std::istreambuf_iterator<char>(inFile)), std::istreambuf_iterator<char>());
        inFile.close();

        size_t offset = 0;
        while (offset < fileData.size()) {
            try {
                uint32_t keyLen = decodeLength(fileData, offset);
                std::string key = decodeBytes(fileData, offset, keyLen);
                uint32_t valLen = decodeLength(fileData, offset);
                std::string val = decodeBytes(fileData, offset, valLen);

                insertInMemory(key, val);
            } catch (const std::exception& e) {
                break;
            }
        }
    }

    
    void flush() {
        
        std::string sstFileName = "L0_00" + std::to_string(sstCounter) + ".sst";
        
        std::ofstream sstFile(sstFileName, std::ios::out | std::ios::binary);
        if (!sstFile.is_open()) {
            std::cerr << "Failed to open SSTable file for writing." << std::endl;
            return;
        }

        std::cout << "Flushing MemTable to " << sstFileName << "...\n";

        
        Node* current = head->forward[0];
        while (current != nullptr) {
            Buffer entry;
            encodeLength(entry, current->key.size());
            encodeBytes(entry, current->key);
            encodeLength(entry, current->value.size());
            encodeBytes(entry, current->value);

            sstFile.write(reinterpret_cast<const char*>(entry.data()), entry.size());
            
            current = current->forward[0];
        }

        sstFile.close();
        sstCounter++;

        
    }

    void displayList() {
        std::cout << "current db:\n";
        Node* node = head->forward[0]; 
        while (node != nullptr) {
            std::cout << node->key << " : " << node->value << '\n';
            node = node->forward[0];
        }
    }
};
