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
#include <cmath>
#include <functional>
#include <map> 

using Buffer = std::vector<uint8_t>;

// --- Serialization Helpers ---

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

// --- Bloom Filter ---

class BloomFilter {
public:
    std::vector<bool> bitArray;
    int numHashes;
    int sizeInBits;

    BloomFilter() : numHashes(0), sizeInBits(0) {}

    BloomFilter(int n, double p = 0.01) {
        double m = -(n * log(p)) / (pow(log(2), 2));
        double k = (m / n) * log(2);

        sizeInBits = static_cast<int>(ceil(m));
        numHashes = static_cast<int>(ceil(k));
        bitArray.resize(sizeInBits, false);
    }

    void add(const std::string& key) {
        std::hash<std::string> hash1;
        std::hash<std::string> hash2;
        
        size_t h1 = hash1(key);
        size_t h2 = hash2(key + "_salt"); 

        for (int i = 0; i < numHashes; i++) {
            size_t bitIndex = (h1 + i * h2) % sizeInBits;
            bitArray[bitIndex] = true;
        }
    }

    bool possiblyContains(const std::string& key) const {
        if (sizeInBits == 0) return false;
        
        std::hash<std::string> hash1;
        std::hash<std::string> hash2;

        size_t h1 = hash1(key);
        size_t h2 = hash2(key + "_salt");

        for (int i = 0; i < numHashes; i++) {
            size_t bitIndex = (h1 + i * h2) % sizeInBits;
            if (!bitArray[bitIndex]) {
                return false; 
            }
        }
        return true; 
    }

    void serialize(Buffer& buffer) const {
        encodeLength(buffer, (uint32_t)numHashes);
        encodeLength(buffer, (uint32_t)sizeInBits);
        
        int numBytes = (sizeInBits + 7) / 8;
        for (int i = 0; i < numBytes; i++) {
            uint8_t byte = 0;
            for (int bit = 0; bit < 8; bit++) {
                int index = i * 8 + bit;
                if (index < sizeInBits && bitArray[index]) {
                    byte |= (1 << bit);
                }
            }
            buffer.push_back(byte);
        }
    }

    static BloomFilter deserialize(const Buffer& buffer, size_t& offset) {
        BloomFilter bf;
        bf.numHashes = decodeLength(buffer, offset);
        bf.sizeInBits = decodeLength(buffer, offset);
        
        bf.bitArray.resize(bf.sizeInBits);
        int numBytes = (bf.sizeInBits + 7) / 8;

        for (int i = 0; i < numBytes; i++) {
            if (offset >= buffer.size()) break;
            uint8_t byte = buffer[offset++];
            for (int bit = 0; bit < 8; bit++) {
                int index = i * 8 + bit;
                if (index < bf.sizeInBits) {
                    bf.bitArray[index] = (byte & (1 << bit)) != 0;
                }
            }
        }
        return bf;
    }
};

// --- Structures ---

struct IndexEntry {
    std::string key;
    uint32_t offset;
};

struct SSTableMetadata {
    std::string filename;
    std::vector<IndexEntry> sparseIndex;
    BloomFilter bloomFilter; 
};

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

// --- Main KVStore Class ---

class KVStore {
private:
    Node* head;
    int currentLevel;
    std::ofstream walFile; 
    const std::string walFileName = "wal.log";
    const std::string manifestFileName = "MANIFEST";
    
    // Tombstone Constant
    const std::string TOMBSTONE_VALUE = "~~DELETED~";

    int sstCounter = 1; 
    std::vector<SSTableMetadata> sstables;

    // Helper for Compaction: Reads all Key-Values from a single SSTable
    std::vector<std::pair<std::string, std::string>> readAllFromSSTable(const SSTableMetadata& meta) {
        std::vector<std::pair<std::string, std::string>> data;
        std::ifstream file(meta.filename, std::ios::binary | std::ios::ate);
        if (!file.is_open()) return data;

        size_t fileSize = file.tellg();
        if (fileSize < 8) return data;

        // 1. Read Footer to find where Data Block ends (which is Index Offset)
        file.seekg(-8, std::ios::end);
        char footerBuf[8];
        file.read(footerBuf, 8);
        
        Buffer fBuf(8);
        memcpy(fBuf.data(), footerBuf, 8);
        size_t fOff = 0;
        uint32_t indexOffset = decodeLength(fBuf, fOff); // Data ends here

        // 2. Read Data Block
        file.seekg(0, std::ios::beg);
        Buffer buffer(indexOffset);
        file.read(reinterpret_cast<char*>(buffer.data()), indexOffset);
        
        size_t offset = 0;
        while (offset < buffer.size()) {
            try {
                uint32_t kLen = decodeLength(buffer, offset);
                std::string k = decodeBytes(buffer, offset, kLen);
                uint32_t vLen = decodeLength(buffer, offset);
                std::string v = decodeBytes(buffer, offset, vLen);
                data.push_back({k, v});
            } catch (...) {
                break;
            }
        }
        return data;
    }

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
            loadSSTableMeta(filename);

            try {
              
                size_t start = filename.find('_');
                size_t end = filename.find('.');
                if (start != std::string::npos && end != std::string::npos) {
                    std::string numPart = filename.substr(start + 1, end - start - 1);
                    if (isdigit(numPart[0])) {
                        int num = std::stoi(numPart);
                        if (num > maxNum) maxNum = num;
                    }
                }
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

    void loadSSTableMeta(const std::string& filename) {
        std::ifstream file(filename, std::ios::binary | std::ios::ate);
        if (!file.is_open()) return;

        std::streampos fileSize = file.tellg();
        if (fileSize < 8) return; 

        file.seekg(-8, std::ios::end);
        char buffer[8];
        file.read(buffer, 8);
        
        Buffer footerBuf(8);
        memcpy(footerBuf.data(), buffer, 8);
        size_t footerOffset = 0;
        uint32_t indexOffset = decodeLength(footerBuf, footerOffset);
        uint32_t bloomOffset = decodeLength(footerBuf, footerOffset);

        SSTableMetadata meta;
        meta.filename = filename;

       
        file.seekg(indexOffset);
        size_t indexSize = bloomOffset - indexOffset; 
        std::vector<uint8_t> indexData(indexSize);
        file.read(reinterpret_cast<char*>(indexData.data()), indexSize);

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

       
        file.seekg(bloomOffset);
        size_t bloomSize = (size_t)fileSize - 8 - bloomOffset;
        Buffer bloomData(bloomSize);
        file.read(reinterpret_cast<char*>(bloomData.data()), bloomSize);
        
        size_t bloomParseOffset = 0;
        meta.bloomFilter = BloomFilter::deserialize(bloomData, bloomParseOffset);
        
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

   
    void del(std::string key) {
        put(key, TOMBSTONE_VALUE);
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
            if (current->value == TOMBSTONE_VALUE) return ""; 
            return current->value; 
        } 

       
        for (int i = sstables.size() - 1; i >= 0; i--) {
            if (!sstables[i].bloomFilter.possiblyContains(key)) {
                continue;
            }
            
            std::string res = searchInSSTable(sstables[i], key);
            if (!res.empty()) {
                if (res == TOMBSTONE_VALUE) return ""; 
                return res;
            }
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
        if (!inFile.is_open()) return;

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
        
        // Count elements for Bloom Filter
        int numElements = 0;
        Node* countNode = head->forward[0];
        while(countNode) { numElements++; countNode = countNode->forward[0]; }
        
        BloomFilter bf(numElements > 0 ? numElements : 10);

        uint64_t currentOffset = 0;
        int entryCount = 0;         
        int SPARSE_FACTOR = 3; 
        
        Node* current = head->forward[0];
        while (current != nullptr) {
            bf.add(current->key); 

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
            currentOffset += idxEntry.size();
        }

        uint64_t bloomStartOffset = currentOffset;
        Buffer bloomBuf;
        bf.serialize(bloomBuf);
        sstFile.write(reinterpret_cast<const char*>(bloomBuf.data()), bloomBuf.size());

        Buffer footer;
        encodeLength(footer, (uint32_t)indexStartOffset);
        encodeLength(footer, (uint32_t)bloomStartOffset);
        sstFile.write(reinterpret_cast<const char*>(footer.data()), footer.size());

        sstFile.close();

        appendToManifest(sstFileName);
        sstables.push_back({sstFileName, currentSSTIndex, bf});

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

    
    void compact() {
        if (sstables.empty()) return;
        std::cout << "Compacting " << sstables.size() << " SSTables..." << std::endl;
        
       
        std::map<std::string, std::string> mergedData;
        for (const auto& meta : sstables) {
            auto fileData = readAllFromSSTable(meta);
            for (const auto& kv : fileData) {
                mergedData[kv.first] = kv.second;
            }
        }

     
        std::string newSSTName = "L1_merged.sst";
        std::ofstream sstFile(newSSTName, std::ios::out | std::ios::binary);
        
        std::vector<IndexEntry> newIndex;
        
        
        int validElements = 0;
        for (const auto& kv : mergedData) {
            if (kv.second != TOMBSTONE_VALUE) validElements++;
        }
        BloomFilter bf(validElements > 0 ? validElements : 10);

        uint64_t currentOffset = 0;
        int entryCount = 0;
        int SPARSE_FACTOR = 3;

        for (const auto& kv : mergedData) {
            
            if (kv.second == TOMBSTONE_VALUE) {
                continue; 
            }

            bf.add(kv.first);

            Buffer entry;
            encodeLength(entry, kv.first.size());
            encodeBytes(entry, kv.first);
            encodeLength(entry, kv.second.size());
            encodeBytes(entry, kv.second);

            if (entryCount % SPARSE_FACTOR == 0) {
                newIndex.push_back({kv.first, (uint32_t)currentOffset});
            }

            sstFile.write(reinterpret_cast<const char*>(entry.data()), entry.size());
            currentOffset += entry.size();
            entryCount++;
        }

        // Write Index
        uint64_t indexStart = currentOffset;
        for (const auto& idx : newIndex) {
            Buffer idxEntry;
            encodeLength(idxEntry, idx.key.size());
            encodeBytes(idxEntry, idx.key);
            encodeLength(idxEntry, idx.offset);
            sstFile.write(reinterpret_cast<const char*>(idxEntry.data()), idxEntry.size());
            currentOffset += idxEntry.size();
        }

        // Write Bloom
        uint64_t bloomStart = currentOffset;
        Buffer bloomBuf;
        bf.serialize(bloomBuf);
        sstFile.write(reinterpret_cast<const char*>(bloomBuf.data()), bloomBuf.size());

        // Write Footer
        Buffer footer;
        encodeLength(footer, (uint32_t)indexStart);
        encodeLength(footer, (uint32_t)bloomStart);
        sstFile.write(reinterpret_cast<const char*>(footer.data()), footer.size());
        sstFile.close();

        // 3. Cleanup Old Files
        for (const auto& meta : sstables) {
            std::filesystem::remove(meta.filename);
        }
        
        // 4. Update Metadata
        sstables.clear();
        std::ofstream manifest(manifestFileName, std::ios::trunc);
        manifest << newSSTName << "\n";
        manifest.close();

        loadSSTableMeta(newSSTName);
    }

    void displayList() {
        Node* node = head->forward[0]; 
        while (node != nullptr) {
            std::cout << node->key << " : " << node->value << '\n';
            node = node->forward[0];
        }
    }
};
