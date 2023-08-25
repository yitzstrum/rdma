//
// Created by danieltal on 8/16/23.
//

#include <infiniband/verbs.h>
#include <stdio.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include "bw_template.h"
#include "Server.h"
#include "DB.h"

int init_clients_ctx(My_kv* my_kv,int i){
    my_kv->clients_ctx[i] = pp_init_ctx(my_kv, my_kv->ctx->cq);
    if (my_kv->clients_ctx[i] == NULL)
    {
        fprintf(stderr, "Server failed to connect clients, ctx is null\n");
        return 1;
    }
    return 0;
}

int init_clients_routs(My_kv* my_kv, int i){
    my_kv->clients_ctx[i]->routs = pp_post_recv_server(my_kv->clients_ctx[i], my_kv->clients_ctx[i]->rx_depth);
    if (my_kv->clients_ctx[i]->routs < my_kv->clients_ctx[i]->rx_depth) {
        fprintf(stderr, "Couldn't post receive (%d)\n", my_kv->clients_ctx[i]->routs);
        ibv_destroy_qp(my_kv->clients_ctx[i]->qp);
        return 1;
    }
    return 0;
}

int init_clients_dest(My_kv* my_kv,int i){
//    todo
    my_kv->my_dest.qpn = my_kv->clients_ctx[i]->qp->qp_num;
    my_kv->rem_dest = pp_server_exch_dest(my_kv->clients_ctx[i]->qp, &my_kv->my_dest, my_kv->gidx);
    if (!my_kv->rem_dest) {
        return 1;
    }
    return 0;
}

int connect_to_clients(My_kv* my_kv)
{
    for (int i = 0; i < NUM_OF_CLIENTS; ++i) {
        if(init_clients_ctx(my_kv,i)){return 1;}
        if(init_clients_routs(my_kv,i)){return 1;}
        if(init_clients_dest(my_kv,i)){return 1;}
        inet_ntop(AF_INET6, &my_kv->rem_dest->gid, my_kv->gid, sizeof(my_kv->gid));
        printf("  remote address: LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
               my_kv->rem_dest->lid, my_kv->rem_dest->qpn, my_kv->rem_dest->psn, my_kv->gid);
    }
    return 0;
}

void start_server(void* obj){
    My_kv *pHandler = ((My_kv *) obj);
    DB *hashTable = initializeHashTable();

    enum Protocol protocol;
    enum Operation_t operation;
    size_t key_size;
    size_t val_size;
    int client_idx;
    bool error = false;
    struct ibv_wc wc;


    if (connect_to_clients(pHandler) != 0) {
        fprintf(stderr, "Couldn't connect clients\n");
        return;
    }
    printf("server start\n");


    while (1) {
        if (pull_cq(pHandler, &wc, 1) != 0)
        {
            perror("Server failed pull cq:");
            return;
        }


        if ((client_idx = get_client_identifier(pHandler, wc.qp_num)) == -1)
        {
            fprintf(stderr, "failed to find client\n");
        }


        if (wc.opcode == IBV_WC_RDMA_READ)
        {
            end_rendezvous_read(pHandler, client_idx, hashTable);
            continue;
        }


        if (wc.opcode == IBV_WC_RDMA_WRITE)
        {
            end_rendezvous_write(pHandler, client_idx);
            continue;
        }


        int resource_idx = (int)wc.wr_id;


        if (resource_idx >= MAX_RESOURCES || resource_idx < 0)
        {
//            continue;
            fprintf(stderr, "Invalid wr_id %d (client idx %d)\n", resource_idx, client_idx);
            parse_header(pHandler->clients_ctx[client_idx]->buf, &protocol, &operation, &key_size, &val_size);
            char* protocol_name = protocol == RANDEVOUS ? "rendezvous" : "eager";
            char* operation_name = operation == GET ? "get" : "set";
            printf("client %d %s %s resource %d key_s %zu, val_s %zu\n", client_idx, protocol_name, operation_name, resource_idx, key_size, val_size);
            printf("is_server %d\n", ((bool *)pHandler->clients_ctx[client_idx]->buf)[MAX_BUF_SIZE - 1]);
            break;
        }



        void* buf = pHandler->clients_ctx[client_idx]->resources[resource_idx].buf;
        size_t header_size = parse_header(buf, &protocol, &operation, &key_size, &val_size);
        char *data = (char *) ((uint8_t *) buf + header_size);

        char* protocol_name = protocol == RANDEVOUS ? "rendezvous" : "eager";
        char* operation_name = operation == GET ? "get" : "set";
        printf("client %d %s %s resource %d key_s %zu, val_s %zu\n", client_idx, protocol_name, operation_name, resource_idx, key_size, val_size);



        switch (operation) {
            case SET:
                if (handle_set(pHandler, data, hashTable, client_idx, protocol, key_size, val_size) != 0) {
                    fprintf(stderr, "Server failed to handle set\n");
                    error = true;
                    break;
                }
                pp_post_recv(pHandler->clients_ctx[client_idx], resource_idx);
                break;

            case GET:
                if (handle_get(pHandler, data, hashTable, client_idx, resource_idx, key_size) != 0) {
                    fprintf(stderr, "Server failed to handle get\n");
                    error = true;
                    break;
                }
                break;
            default:
                fprintf(stderr, "Server does not support the requested operation\n");
                error = true;
                break;
        }

        if (error) {
            hashTable_delete_cleanup(hashTable);
            return;
        }
    }
}