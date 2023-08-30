#include <stdio.h>
#include <infiniband/verbs.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include <time.h>
#include "Client.h"
#include "Server.h"
#include "bw_template.h"

#define STRING_LENGTH (10)
#define NUM_WARMUP_REQUESTS (100)
#define TOTAL_REQUESTS (1000)
#define KEY_LEN (2)
#define MAX_RENDEZVOUS_MSG_SIZE (1048832)
#define MB (1048576)

void generate_random_string(char** str, int key_length) {
    srand(time(NULL));
    *str = (char*)malloc((key_length) * sizeof(char));
    if (*str == NULL)
    {
        printf("Failed malloc\n");
        return;
    }
    for (int i = 0; i < key_length - 1; i++) {
        int random_number = rand() % 26;
        (*str)[i] = 'A' + random_number;
    }
    (*str)[key_length - 1] = '\0';
}

int warmup(KvHandle * pHandler)
{
    const int value_size = MAX_EAGER_SIZE - KEY_LEN - 1;

    char *value = malloc(value_size);
    memset(value, 'A', value_size - 1);
    value[value_size - 1] = '\0';

    char* key;
    generate_random_string(&key, KEY_LEN);

    if (kv_set((void *)pHandler, key, value) != 0) {
        free(key);
        return 1;
    }

    char* receivedValue;
    for (int i = 0; i < NUM_WARMUP_REQUESTS - 1; ++i) {
        if (kv_get((void *)pHandler, key, &receivedValue) != 0) {
            return 1;
        }
    }

    kv_release(receivedValue);
    free(key);
    return 0;
}

void calculate_throughput(clock_t start_time, clock_t end_time, size_t bytes_size) {
    double elapsed_time = (double) (end_time - start_time) / CLOCKS_PER_SEC;
    double throughput = (TOTAL_REQUESTS / elapsed_time) * bytes_size;
    double throughput_in_MBS = throughput / MB;
    printf("%zu\t%.2f\tMBs\n", bytes_size, throughput_in_MBS);
}

void measure_get_throughput_by_message_size(KvHandle * kv_handle, int message_size)
{
    if (warmup(kv_handle) != 0) {
        return;
    }

    int value_size = message_size - KEY_LEN;
    char *value_data = malloc(value_size);
    memset(value_data, 'A', value_size - 1);
    value_data[message_size - 1] = '\0';

    char *key_data;
    generate_random_string(&key_data, KEY_LEN);
    if (kv_set(kv_handle, key_data, value_data) != 0) {
        return;
    }
    kv_release(value_data);

    char* received_value;
    clock_t start_time = clock();
    for (int i = 0; i < TOTAL_REQUESTS; i++) {
        kv_get(kv_handle, key_data, &received_value);
    }
    clock_t end_time = clock();
    calculate_throughput(start_time, end_time, message_size);

    kv_release(received_value);
}

void measure_set_throughput_by_message_size(KvHandle* kv_handle, int message_size)
{
    if (warmup(kv_handle) != 0) {
        return;
    }

    int value_size = message_size - KEY_LEN;
    char *value_data = malloc(value_size);
    memset(value_data, 'A', value_size - 1);
    value_data[message_size - 1] = '\0';

    char *key_data;
    generate_random_string(&key_data, KEY_LEN);

    clock_t start_time = clock();
    for (int i = 0; i < TOTAL_REQUESTS; i++) {
        if (kv_set(kv_handle, key_data, value_data) != 0)
            return;
    }
    clock_t end_time = clock();
    calculate_throughput(start_time, end_time, message_size);
}

int main(int argc, char *argv[])
{
        char* servername = NULL;
        srand48(getpid() * time(NULL));

        if (optind == argc - 1)
            servername = strdup(argv[optind]);
        else if (optind < argc) {
            return 1;
        }

        KvHandle handler;
        memset(&handler, 0, sizeof(KvHandle));
        KvHandle* kv_handle = &handler;

        if (init_network_context(kv_handle, servername) != 0)
        {
            fprintf(stderr, "failed to initialize KvHandle");
            return 1;
        }

        // Client
        if (servername)
        {
            if(init_client_post_recv(kv_handle)){
                return 1;
            }
            // Initialize Work Request sent.
            kv_handle->ctx->count_send=0;

            if (kv_open(servername, (void **) &kv_handle) != 0)
            {
                fprintf(stderr, "Client couldn't connect\n");
                return 1;
            }

//            for (int message_size = 4; message_size <= MAX_RENDEZVOUS_MSG_SIZE; message_size *= 2) {
//                measure_set_throughput_by_message_size(kv_handle, message_size);
//            }

            const char *key = "key";
            const char *value = "value";

            if (kv_set((void *)kv_handle, key, value) != 0)
            {
                return 1;
            }

            char *received_value = NULL;

            sleep(5);
            if (kv_get((void *)kv_handle, key, &received_value) != 0)
            {
                fprintf(stderr, "Client failed to preform get\n");
                return 1;
            }
            printf("%s: %s\n", key, received_value);


            kv_release(received_value);
        }

        // Server
        else
        {
            start_server((void *) kv_handle);
            for (int i = 0; i < NUM_OF_CLIENTS; ++i) {
                pp_close_ctx(kv_handle->clients_ctx[i]);
            }
            release_db(kv_handle->hashTable);
        }

        release_kv_handler((KvHandle **) &kv_handle);
        return 0;
    }
