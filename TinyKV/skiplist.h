#include <iostream> 
#include <cstdlib>
#include <cmath>
#include <cstring>
#include <mutex>
#include <fstream>
#include"mem.h"



std::mutex mtx;
std::string delimiter = ":";

template<typename K, typename V>
class Node {
public:
    Node() {}
    Node(K k, V v, int);
    K* get_key();
    V* get_value();
    void set_value(V*);
    void* operator new(size_t size, int level, BlockArena* arena){     
        size_t point_size = sizeof(Node<K, V>*) * (level + 1);
        size += point_size;
        size_t align_size = std::alignment_of<Node<K, V>>::value;
        void* ptr = arena->allocate(size, align_size);
        
        return ptr;
    }
    void operator delete(void* phead) {
        
    }    
    Node<K, V>* next() {
        if (this->node_level < 1) return nullptr; 
        else return this->forward[1];
    }

private:
    K key;
    V value;
public:
    int node_level;
    Node<K, V>* forward[];
};

template<typename K, typename V>
Node<K, V>::Node(const K k, const V v, int level) {
    this->key = k;
    this->value = v;
    this->node_level = level;
    memset(this->forward, 0, sizeof(Node<K, V>*) * (level + 1));
};

template<typename K, typename V>
K* Node<K, V>::get_key(){
    return  &key;
};

template<typename K, typename V>
V* Node<K, V>::get_value() {
    return &value;
};
template<typename K, typename V>
void Node<K, V>::set_value(V* value) {
    this->value = *value;
};


template <typename K, typename V>
class SkipList {
public:
    SkipList(int);
    ~SkipList();
    int get_random_level();
    Node<K, V>* create_node(K, V, int);
    int insert_element(K, V);
    V* search_element(K&);
    void delete_element(K); 
    int size();
    class Iterator;

    Iterator begin() {
        return Iterator(_header);
    }

    Iterator end() {
        return Iterator(nullptr);
    }

    class Iterator {
    public:
        Iterator() noexcept :
            m_pCurrentNode(SkipList<K, V>::_header) { }

        Iterator(Node<K, V>* pNode) noexcept :
            m_pCurrentNode(pNode) { }

        Iterator& operator=(Node<K, V>* pNode)
        {
            this->m_pCurrentNode = pNode;
            return *this;
        }


        Iterator& operator++()
        {
            if (m_pCurrentNode)
                m_pCurrentNode = m_pCurrentNode->next();
            return *this;
        }

        Iterator operator++(int)
        {
            if (m_pCurrentNode)
                m_pCurrentNode = m_pCurrentNode->next();
            return *this;
        }

        bool operator!=(const Iterator& iterator)
        {
            return m_pCurrentNode != iterator.m_pCurrentNode;
        }

        Node<K, V>* operator*()
        {
            return m_pCurrentNode;
        }

    private:
        Node<K, V>* m_pCurrentNode;
    };

private:
    void get_key_value_from_string(const std::string& str, std::string* key, std::string* value);
    bool is_valid_string(const std::string& str);

private:

    int _max_level;

    int _skip_list_level;

    Node<K, V>* _header;

    int _element_count;

    BlockArena* arena;
};


template<typename K, typename V>
Node<K, V>* SkipList<K, V>::create_node(const K k, const V v, int level) {
    Node<K, V>* n = new(level, arena) Node<K, V>(k, v, level);
    return n;
}

template<typename K, typename V>
int SkipList<K, V>::insert_element(K key, V value) {

    mtx.lock();
    Node<K, V>* current = this->_header;
    Node<K, V>** update = new Node<K,V>*[_max_level + 1]();
    memset(update, 0, sizeof(Node<K, V>*) * (_max_level + 1));

    for (int i = _skip_list_level; i >= 0; i--) {
        while (current->forward[i] != NULL && *(current->forward[i]->get_key()) < key) {
            current = current->forward[i];
        }
        update[i] = current;
    }

    current = current->forward[0];

    if (current != NULL && *(current->get_key()) == key) {
        current->set_value(&value);
    }

    if (current == NULL || *(current->get_key()) != key) {

        int random_level = get_random_level();

        if (random_level > _skip_list_level) {
            for (int i = _skip_list_level + 1; i < random_level + 1; i++) {
                update[i] = _header;
            }
            _skip_list_level = random_level;
        }

        Node<K, V>* inserted_node = create_node(key, value, random_level);

        for (int i = 0; i <= random_level; i++) {
            inserted_node->forward[i] = update[i]->forward[i];
            update[i]->forward[i] = inserted_node;
        }     
    }
    _element_count++;
    mtx.unlock();
    return 0;
}

template<typename K, typename V>
int SkipList<K, V>::size() {
    return _element_count;
}

template<typename K, typename V>
void SkipList<K, V>::get_key_value_from_string(const std::string& str, std::string* key, std::string* value) {

    if (!is_valid_string(str)) {
        return;
    }
    *key = str.substr(0, str.find(delimiter));
    *value = str.substr(str.find(delimiter) + 1, str.length());
}

template<typename K, typename V>
bool SkipList<K, V>::is_valid_string(const std::string& str) {

    if (str.empty()) {
        return false;
    }
    if (str.find(delimiter) == std::string::npos) {
        return false;
    }
    return true;
}

template<typename K, typename V>
void SkipList<K, V>::delete_element(K key) {

    mtx.lock();
    Node<K, V>* current = this->_header;
    Node<K, V>** update = new Node<K, V>*[_max_level + 1]();
    memset(update, 0, sizeof(Node<K, V>*) * (_max_level + 1));

    for (int i = _skip_list_level; i >= 0; i--) {
        while (current->forward[i] != NULL && *(current->forward[i]->get_key()) < key) {
            current = current->forward[i];
        }
        update[i] = current;
    }

    current = current->forward[0];
    if (current != NULL && *(current->get_key()) == key) {

        for (int i = 0; i <= _skip_list_level; i++) {

            if (update[i]->forward[i] != current)
                break;

            update[i]->forward[i] = current->forward[i];
        }

        while (_skip_list_level > 0 && _header->forward[_skip_list_level] == 0) {
            _skip_list_level--;
        }

        _element_count--;
    }
 
    mtx.unlock();
    return;
}

template<typename K, typename V>
V* SkipList<K, V>::search_element(K& key) {

    Node<K, V>* current = _header;

    for (int i = _skip_list_level; i >= 0; i--) {
        while (current->forward[i] && *(current->forward[i]->get_key()) < key) {
            current = current->forward[i];
        }
    }

    current = current->forward[0];

    if (current and *(current->get_key()) == key) {
        return current->get_value();
    }

    return nullptr;
}

template<typename K, typename V>
SkipList<K, V>::SkipList(int max_level) {

    this->_max_level = max_level;
    this->_skip_list_level = 0;
    this->_element_count = 0;
    this->arena = new BlockArena;

    K k{};
    V v{};
    this->_header = new(_max_level, arena) Node<K, V>(k, v, _max_level);
};

template<typename K, typename V>
SkipList<K, V>::~SkipList() {
    delete arena;
}


template<typename K, typename V>
int SkipList<K, V>::get_random_level() {

    int k = 1;
    while (rand() % 2) {
        k++;
    }
    k = (k < _max_level) ? k : _max_level;
    return k;
};




