#include <stdbool.h>
#include "db.h"

unsigned int hash(const char* key) {
    unsigned int hash = 5381;
    int c;

    while ((c = *key++))
        hash = ((hash << 5) + hash) + c; // djb2 hash function

    return hash % TABLE_SIZE;
}

HashTable* initializeHashTable() {
    HashTable* table = (HashTable*)malloc(sizeof(HashTable));
    table->capacity = TABLE_SIZE;
    table->size = 0;
    table->entries = (KeyValuePair**)calloc(table->capacity, sizeof(KeyValuePair*));
    return table;
}

void resizeHashTable(HashTable* table, size_t newCapacity) {
    KeyValuePair** newEntries = (KeyValuePair**)calloc(newCapacity, sizeof(KeyValuePair*));

    // Rehash existing entries
    for (size_t i = 0; i < table->capacity; i++) {
        KeyValuePair* entry = table->entries[i];
        while (entry != NULL) {
            KeyValuePair* next = entry->next;
            unsigned int newIndex = hash(entry->key) % newCapacity;

            entry->next = newEntries[newIndex];
            newEntries[newIndex] = entry;

            entry = next;
        }
    }

    // Update the table with new capacity and entries
    free(table->entries);
    table->entries = newEntries;
    table->capacity = newCapacity;
}

bool checkResize(HashTable* table) {
    float loadFactor = (float)table->size / table->capacity;
    return loadFactor >= MAX_OVERLOAD_FACTOR || loadFactor <= MIN_OVERLOAD_FACTOR;
}

int hashTable_set(const char* key, char* value, HashTable* table) {
    unsigned int index = hash(key);

    // Check if the key already exists
    KeyValuePair* entry = table->entries[index];
    while (entry != NULL) {
        if (strcmp(entry->key, key) == 0) {
            // Key already exists, update the value
            if (!entry->is_dirty)
                free(entry->value);
            entry->value = value;
            return 0;
        }
        entry = entry->next;
    }

    // Key does not exist, create a new entry
    KeyValuePair* newEntry = (KeyValuePair*)malloc(sizeof(KeyValuePair));
    if (newEntry == NULL)
    {
        return 1;
    }
    newEntry->key = strdup(key);
    newEntry->value = value;
    newEntry->is_dirty = false;
    newEntry->next = NULL;

    // Insert the new entry at the beginning of the linked list
    if (table->entries[index] == NULL) {
        table->entries[index] = newEntry;
    } else {
        newEntry->next = table->entries[index];
        table->entries[index] = newEntry;
    }

    ++table->size;

    if (checkResize(table))
    {
        resizeHashTable(table, table->capacity * GROWTH_FACTOR);
    }

    return 0;
}

KeyValuePair* hashTable_get_entry(const char* key, HashTable* table)
{
    unsigned int index = hash(key);

    KeyValuePair* entry = table->entries[index];
    while (entry != NULL) {
        if (strcmp(entry->key, key) == 0) {
            return entry;
        }
        entry = entry->next;
    }

    return NULL;
}

void hashTable_get(const char* key, char** value, HashTable* table) {
    KeyValuePair* entry = hashTable_get_entry(key, table);
    *value = entry != NULL ? entry->value : "";
}

int hashTable_set_is_dirty(const char* key, HashTable* table, bool is_dirty)
{
    KeyValuePair* entry = hashTable_get_entry(key, table);
    if (entry == NULL)
    {
        return 1;
    }

    entry->is_dirty = is_dirty;
    return 0;
}

int hashTable_set_dirty(const char* key, HashTable* table)
{
    return hashTable_set_is_dirty(key, table, true);
}

int hashTable_unset_dirty(const char* key, HashTable* table)
{
    return hashTable_set_is_dirty(key, table, false);
}

int hashTable_delete(const char* key, HashTable* table) {
    unsigned int index = hash(key);

    KeyValuePair* entry = table->entries[index];
    KeyValuePair* prevEntry = NULL;

    while (entry != NULL) {
        if (strcmp(entry->key, key) == 0) {
            if (prevEntry == NULL) {
                // Key is at the head of the linked list
                table->entries[index] = entry->next;
            } else {
                // Key is in the middle or at the tail of the linked list
                prevEntry->next = entry->next;
            }

            free(entry->key);
            free(entry->value);
            free(entry);
            table->size--;
            if (checkResize(table))
            {
                resizeHashTable(table, table->capacity * GROWTH_FACTOR);
            }
            return 0;
        }

        prevEntry = entry;
        entry = entry->next;
    }

    return 1;
}


void hashTable_delete_cleanup(HashTable* table) {
    for (size_t i = 0; i < table->capacity; i++) {
        KeyValuePair* entry = table->entries[i];
        while (entry != NULL) {
            KeyValuePair* next = entry->next;
            free(entry->key);
            free(entry->value);
            free(entry);
            entry = next;
        }
    }
    free(table->entries);
    free(table);
}