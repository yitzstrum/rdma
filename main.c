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

#define NUM_WARMUP_REQUESTS (99)
#define STRING_LENGTH (10)
char* generate_random_string() {
    // Seed the random number generator with the current time
    srand(time(NULL));
    char* random_string = (char*)malloc((STRING_LENGTH + 1) * sizeof(char));

    for (int i = 0; i < STRING_LENGTH; i++) {
        int random_number = rand() % 26;
        random_string[i] = 'A' + random_number;
    }
    random_string[STRING_LENGTH] = '\0';

    return random_string;
}

int warmup(KvHandle* pHandler)
{
    char* key = generate_random_string();
    printf("key %s\n", key);
    const int value_size = MAX_EAGER_SIZE - STRING_LENGTH - 1;
    char *value = malloc(value_size);
    memset(value, 'A', value_size - 1);
    value[value_size - 1] = '\0';

    if (kv_set((void *)pHandler, key, value) != 0) {
        free(key);
        return 1;
    }

    for (int i = 0; i < NUM_WARMUP_REQUESTS; ++i) {
        if (kv_get((void *)pHandler, key, &value) != 0) {
            return 1;
        }
    }
    free(key);

    return 0;
}

void measure_eager_throughput(KvHandle * pHandler)
{
    const int total_requests = 100;
    int value_sizes[] = {1022, 2046, 4095};
    printf("Message Size, Throughput (KB/s)\n");
    for (int j = 0; j < 3; j++) {
        int message_size = value_sizes[j];
        if (warmup(pHandler) != 0) {
            return;
        }

        const char *key_data = "A";
        char *value_data = malloc(message_size);
        char* received_value = malloc(message_size);

        memset(value_data, 'A', message_size - 1);
        value_data[message_size - 1] = '\0';
        if (kv_set((void *)pHandler, key_data, value_data) != 0) {
            return;
        }

        uint64_t start_time = clock();
        for (int i = 0; i < total_requests; i++) {
            kv_get(pHandler, key_data, &received_value);
        }
        uint64_t end_time = clock();
        uint64_t elapsed_time = end_time - start_time;
        double throughput = (double)(message_size * 2) / 1024 / (double)elapsed_time * CLOCKS_PER_SEC; // KB/s
        printf("#%d, %.2f KB/s\n", message_size + 2, throughput);

        free(value_data);
        free(received_value);
    }
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
        KvHandle* networkContext = &handler;

        if (init_network_context(networkContext, servername) != 0)
        {
            fprintf(stderr, "Couldn't init Kv_handle");
            return 1;
        }

        if (servername)
        {
            if(init_client_post_recv(networkContext)){
                return 1;
            }
            networkContext->ctx->count_send=0;

            if (kv_open(servername, (void **) &networkContext) != 0)
            {
                fprintf(stderr, "Client couldn't connect\n");
                return 1;
            }

//            measure_eager_throughput(networkContext);
            const char *key = "key";
//            const char *key1 = "key1";
//            const char *key2 = "key2";
            const char *value = "value";
//            const char *value1 = "value1";
//            const char *value2 = "value2";

            if (kv_set((void *)networkContext, key, value) != 0)
            {
                return 1;
            }
//            if (kv_set((void *)networkContext, key1, value1) != 0)
//            {
//                return 1;
//            }
//            if (kv_set((void *)networkContext, key2, value2) != 0)
//            {
//                return 1;
//            }

//            char *received_value = NULL;
//            if (kv_get((void *)networkContext, key, &received_value) != 0)
//            {
//                fprintf(stderr, "Client failed to preform get\n");
//                return 1;
//            }
//            printf("%s: %s\n", key, received_value);

//            if (kv_get((void *)networkContext, key1, &received_value) != 0)
//            {
//                fprintf(stderr, "Client failed to preform get\n");
//                return 1;
//            }
//            printf("%s: %s\n", key1, received_value);
//            if (kv_get((void *)networkContext, key2, &received_value) != 0)
//            {
//                fprintf(stderr, "Client failed to preform get\n");
//                return 1;
//            }
//            printf("%s: %s\n", key2, received_value);

//            kv_release(received_value);
        }

        else
        {
            start_server((void *) networkContext);
            for (int i = 0; i < NUM_OF_CLIENTS; ++i) {
//                pp_close_ctx((networkContext)->clients_ctx[i]);
            }
        }

//          free_kv_handler((Kv_handle **) &pHandler);

            return 0;
    }
