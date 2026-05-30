#ifndef MTHELPERS_H
#define MTHELPERS_H

#include "linkedlist.h"
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <signal.h>
#include <inttypes.h>
// INSERT FUNCTION DECLARATIONS HERE
extern volatile sig_atomic_t sigint_flag;
extern volatile sig_atomic_t sigusr1_flag;

extern pthread_mutex_t maxDonations_lock;
extern pthread_mutex_t donation_log_lock;
extern pthread_mutex_t charity_locks[5];

extern int clientCnt;
extern uint64_t maxDonations[3];
extern charity_t charities[5];
extern int log_fd;

// === Signal Handlers ===

void sigusr1_handler(int signal);
void sigint_handler(int signal);


// === Initializing Server ===
int Open(const char* filename, int flag, mode_t mode);

void Sigaction(int signal, const struct sigaction *act, void*);

void clientid_deleter(void* data);

void init_locks();

void init_server_stats();

// === Client Work ===
void update_maxDonations(uint64_t total_donations);

int client_donate(message_t*msg, int client_fd);

void client_info(message_t*msg, int client_fd);

void client_top(message_t*msg, int client_fd);

void client_logout(int client_fd, uint64_t total_donations);

void client_error(message_t*msg, int client_fd);

// === Client Thread ID Linked List ===
void add_client_thread(list_t* thread_id_list, pthread_t tid);

void reap_current_client_threads(list_t* thread_id_list);

// === Closing Server ===

void destroy_locks();

void close_server(list_t* thread_id_list, int clientCnt, uint64_t* maxDonations, charity_t* charities);

void kill_client_threads(list_t* thread_id_list);

void reap_client_threads(list_t* thread_id_list);

void print_data();
#endif