//
// Created by danieltal on 8/16/23.
//

#ifndef NETWORK3_DB_H
#define NETWORK3_DB_H

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
    struct KeyValuePair* next;
} KeyValuePair;


typedef struct {
    KeyValuePair** entries;
    size_t capacity;
    size_t size;
} DB;

DB* initializeHashTable();
int hashTable_set(const char* key, const char* value, DB* table);
void hashTable_get(const char* key, char** value, DB* table);
int hashTable_delete(const char* key, DB* table);
void hashTable_delete_cleanup(DB* table);

#endif //NETWORK3_DB_H
