#include <iostream>
#include <vector>
#include <string>
#include <fstream>
#include <cstdlib>
#include <ctime>
#include <cstring>
#include <cstdint>
#include <filesystem>
#include <algorithm>

using Buffer = std::vector<uint8_t>;

struct IndexEntry {
    std::string key;
    uint32_t offset;
};

struct SSTableMetadata {
    std::string filename;
    std::vector<IndexEntry> sparseIndex;
};

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
    const std::string manifestFileName = "MANIFEST";
    int sstCounter = 1; 
    std::vector<SSTableMetadata> sstables;

public:
    KVStore() {
        currentLevel = 0;
        head = new Node("", "", MAX_LEVEL); 
        
        recover();
        loadManifest();

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

    void loadManifest() {
        std::ifstream manifest(manifestFileName);
        if (!manifest.is_open()) return;

        std::string filename;
        int maxNum = 0;

        while (std::getline(manifest, filename)) {
            if (filename.empty()) continue;
            loadSSTableIndex(filename);

            try {
                int num = std::stoi(filename.substr(5, 3));
                if (num > maxNum) maxNum = num;
            } catch(...) {}
        }
        sstCounter = maxNum + 1;
        manifest.close();
    }

    void appendToManifest(const std::string& filename) {
        std::ofstream manifest(manifestFileName, std::ios::app);
        if (manifest.is_open()) {
            manifest << filename << "\n";
            manifest.close();
        }
    }

    void loadSSTableIndex(const std::string& filename) {
        std::ifstream file(filename, std::ios::binary | std::ios::ate);
        if (!file.is_open()) return;

        std::streampos fileSize = file.tellg();
        if (fileSize < 4) return; 

        file.seekg(-4, std::ios::end);
        char buffer[4];
        file.read(buffer, 4);
        
        Buffer lenBuf(4);
        memcpy(lenBuf.data(), buffer, 4);
        size_t dummyOffset = 0;
        uint32_t indexOffset = decodeLength(lenBuf, dummyOffset);

        file.seekg(indexOffset);
        std::vector<uint8_t> indexData;
        size_t indexSize = (size_t)fileSize - 4 - indexOffset;
        indexData.resize(indexSize);
        file.read(reinterpret_cast<char*>(indexData.data()), indexSize);

        SSTableMetadata meta;
        meta.filename = filename;
        size_t parseOffset = 0;
        while (parseOffset < indexData.size()) {
            try {
                uint32_t keyLen = decodeLength(indexData, parseOffset);
                std::string key = decodeBytes(indexData, parseOffset, keyLen);
                uint32_t offsetVal = decodeLength(indexData, parseOffset);
                meta.sparseIndex.push_back({key, offsetVal});
            } catch (...) {
                break;
            }
        }
        
        sstables.push_back(meta);
        file.close();
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
        } 

        for (int i = sstables.size() - 1; i >= 0; i--) {
            std::string res = searchInSSTable(sstables[i], key);
            if (!res.empty()) return res;
        }

        return ""; 
    }

    std::string searchInSSTable(const SSTableMetadata& meta, const std::string& key) {
        if (meta.sparseIndex.empty()) return "";

        auto it = std::lower_bound(meta.sparseIndex.begin(), meta.sparseIndex.end(), key,
            [](const IndexEntry& entry, const std::string& val) {
                return entry.key < val;
            });

        uint32_t searchOffset = 0;

        if (it != meta.sparseIndex.end() && it->key == key) {
            searchOffset = it->offset;
        } 
        else if (it != meta.sparseIndex.begin()) {
            it--;
            searchOffset = it->offset;
        } 
        else {
            return ""; 
        }

        std::ifstream file(meta.filename, std::ios::binary);
        if (!file.is_open()) return "";

        file.seekg(searchOffset);

        Buffer buffer((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
        size_t offset = 0;

        while (offset < buffer.size()) {
            try {
                uint32_t kLen = decodeLength(buffer, offset);
                std::string k = decodeBytes(buffer, offset, kLen);
                uint32_t vLen = decodeLength(buffer, offset);
                std::string v = decodeBytes(buffer, offset, vLen);

                if (k == key) return v;
                if (k > key) break; 

            } catch (...) {
                break; 
            }
        }

        return "";
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
        
        if (!sstFile.is_open()) return;

        std::vector<IndexEntry> currentSSTIndex;
        uint64_t currentOffset = 0;
        int entryCount = 0;         
        int SPARSE_FACTOR = 3; 
        
        Node* current = head->forward[0];
        while (current != nullptr) {
            Buffer entry;
            encodeLength(entry, current->key.size());
            encodeBytes(entry, current->key);
            encodeLength(entry, current->value.size());
            encodeBytes(entry, current->value);

            if (entryCount % SPARSE_FACTOR == 0) {
                currentSSTIndex.push_back({current->key, (uint32_t)currentOffset});
            }

            sstFile.write(reinterpret_cast<const char*>(entry.data()), entry.size());
            currentOffset += entry.size();
            entryCount++;
            current = current->forward[0];
        }

        uint64_t indexStartOffset = currentOffset; 
        
        for (const auto& idx : currentSSTIndex) {
            Buffer idxEntry;
            encodeLength(idxEntry, idx.key.size());
            encodeBytes(idxEntry, idx.key);
            encodeLength(idxEntry, idx.offset); 
            sstFile.write(reinterpret_cast<const char*>(idxEntry.data()), idxEntry.size());
        }

        Buffer footer;
        encodeLength(footer, (uint32_t)indexStartOffset);
        sstFile.write(reinterpret_cast<const char*>(footer.data()), footer.size());

        sstFile.close();

        appendToManifest(sstFileName);
        sstables.push_back({sstFileName, currentSSTIndex});

        Node* wipe = head->forward[0];
        while (wipe != nullptr) {
            Node* next = wipe->forward[0];
            delete wipe;
            wipe = next;
        }
        for(int i=0; i<=MAX_LEVEL; i++) head->forward[i] = nullptr;
        currentLevel = 0;
        
        walFile.close();
        walFile.open(walFileName, std::ios::out | std::ios::trunc | std::ios::binary); 

        sstCounter++;
    }

    void displayList() {
        Node* node = head->forward[0]; 
        while (node != nullptr) {
            std::cout << node->key << " : " << node->value << '\n';
            node = node->forward[0];
        }
    }
};
