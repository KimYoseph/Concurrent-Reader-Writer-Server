#ifndef RWHELPERS_H
#define RWHELPERS_H

#include "linkedlist.h"
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <signal.h>
#include <semaphore.h>
#include <inttypes.h>

// INSERT FUNCTION DECLARATIONS HERE


// Server Global Variables (Statistics)

extern volatile sig_atomic_t sigint_flag;
extern volatile sig_atomic_t sigusr1_flag;

extern sem_t charities_lock;
extern sem_t charities_w;
extern sem_t maxDonations_lock;
extern sem_t maxDonations_w;
extern pthread_mutex_t donation_log_lock;
extern pthread_mutex_t clientCnt_lock;



extern int clientCnt;
extern uint64_t maxDonations[3];
extern charity_t charities[5];


extern int log_fd;
extern int charities_readcnt;
extern int maxDonations_readcnt;
// === Signal Handlers ===

void sigusr1_handler(int signal);

void sigint_handler(int signal);


// === Semaphores ===
void P(sem_t *s);

void V(sem_t *s);

// === Initialize Server ===
int Open(const char* filename, int flag, mode_t mode);

void Sigaction(int signal, const struct sigaction *act, void*);

void init_server_stats();

void init_locks();


// === Thread Work ===

void writer_logout(int client_fd, uint64_t total_donations);

void client_error(message_t*msg, int client_fd);

// === Reader Thread Work ===

void reader_info(message_t*msg, int client_fd);

void reader_top(message_t*msg, int client_fd);

void reader_stats(message_t*msg, int client_fd);

void reader_logout(int client_fd);

// === Writer Thread Work ===

int writer_donate(message_t*msg, int client_fd);

int check_update_maxDonations(uint64_t total_donations);

void update_maxDonations(uint64_t total_donations);

// === Client Thread ID Linked List ===
void threadid_deleter(void* data);

void add_client_thread(list_t* thread_id_list, pthread_t tid);

void reap_current_client_threads(list_t* thread_id_list);

// === Closing Server ===

void destroy_locks();

void close_server(list_t* thread_id_list, int clientCnt, uint64_t* maxDonations, charity_t* charities);

void kill_client_threads(list_t* thread_id_list);

void reap_client_threads(list_t* thread_id_list);

void print_data(int clientCnt, uint64_t* maxDonations, charity_t* charities);
#endif