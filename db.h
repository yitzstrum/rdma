#ifndef EX3_HASHTABLE_H
#define EX3_HASHTABLE_H
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define TABLE_SIZE 16
#define MAX_OVERLOAD_FACTOR 0.75
#define MIN_OVERLOAD_FACTOR 0.25
#define GROWTH_FACTOR 2

typedef struct KeyValuePair {
    char* key;
    char* value;
    bool is_dirty;
    struct KeyValuePair* next;
} KeyValuePair;


typedef struct {
    KeyValuePair** entries;
    size_t capacity;
    size_t size;
} HashTable;

HashTable* initializeHashTable();
int hashTable_set(const char* key, char* value, HashTable* table);
void hashTable_get(const char* key, char** value, HashTable* table);
int hashTable_set_dirty(const char* key, HashTable* table);
int hashTable_unset_dirty(const char* key, HashTable* table);
int hashTable_delete(const char* key, HashTable* table);
void hashTable_delete_cleanup(HashTable* table);
#endif //EX3_HASHTABLE_H
