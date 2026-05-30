#include "server.h"
#include "protocol.h"
#include "MThelpers.h"
#include "linkedlist.h"
#include <pthread.h>
#include <signal.h>
#include <errno.h>


/**********************DECLARE ALL LOCKS HERE BETWEEN THES LINES FOR MANUAL GRADING*************/
pthread_mutex_t maxDonations_lock;  // lock for maxDonations array.
pthread_mutex_t donation_log_lock;  // lock for the donation file
pthread_mutex_t charity_locks[5];  // array of locks for each position in the charity array


/***********************************************************************************************/


// Global variables, statistics collected since server start-up
int clientCnt;  // # of client connections made, Updated by the main thread
uint64_t maxDonations[3];  // 3 highest total donations amounts (sum of all donations to all
                           // charities in one connection), updated by client threads
                           // index 0 is the highest total donation
charity_t charities[5]; // Global variable, one charity per index

volatile sig_atomic_t sigint_flag = 0;
volatile sig_atomic_t sigusr1_flag = 0;

int log_fd;

void* client_work(void* clientfd_ptr);


int main(int argc, char *argv[]) {

    // Arg parsing
    int opt;
    while ((opt = getopt(argc, argv, "h")) != -1) {
        switch (opt) {
            case 'h':
                fprintf(stderr, USAGE_MSG_MT);
                exit(EXIT_FAILURE);
        }
    }

    // 3 positional arguments necessary
    if (argc != 3) {
        fprintf(stderr, USAGE_MSG_MT);
        exit(EXIT_FAILURE);
    }
    unsigned int port_number = atoi(argv[1]);
    char *log_filename = argv[2];


    // INSERT SERVER INITIALIZATION CODE HERE

    log_fd = Open(log_filename, O_WRONLY | O_CREAT | O_TRUNC, 0644); // open file log
    list_t* thread_id_list = CreateList(clientid_deleter); // NO SYNCH NEEDED PER HW SPEC
    struct sigaction myaction = {{0}}; 
    myaction.sa_handler = sigint_handler; 
    Sigaction(SIGINT, &myaction, NULL); // install sigchild_handler.
    init_locks();// initialize all locks.
    init_server_stats();

    
    // Initiate server socket for listening
    int listen_fd = socket_listen_init(port_number);
    printf("Currently listening on port: %d.\n", port_number);
    int client_fd;
    struct sockaddr_in client_addr;
    unsigned int client_addr_len = sizeof(client_addr);

    pthread_t tid; // thread ids.

    sigset_t sigint_mask;
    sigemptyset(&sigint_mask);
    sigaddset(&sigint_mask ,SIGINT);

    while(1) {
        // Wait and Accept the connection from client
        if (sigint_flag == 1) {
            break;
        }
        do {
            client_fd = accept(listen_fd, (SA*)&client_addr, &client_addr_len);
        }
        while ((client_fd < 0 && errno == EINTR && sigint_flag == 0)); // perfect
        if (sigint_flag == 1) // 
            break;
        if (client_fd < 0) { // failed, but not because of a signal.
            printf("server accept failed\n");
            exit(EXIT_FAILURE);
        }
        // INSERT SERVER ACTIONS FOR CONNECTED CLIENT CODE HERE
        int* thread_fd = malloc(sizeof(int));
        *thread_fd = client_fd;
        
        reap_current_client_threads(thread_id_list);

        pthread_sigmask(SIG_BLOCK, &sigint_mask, NULL);
        if (pthread_create(&tid, NULL, client_work, (void*) thread_fd) == -1){
            free(thread_fd);
            close(client_fd);
            continue;
        }
        pthread_sigmask(SIG_UNBLOCK, &sigint_mask, NULL);
        add_client_thread(thread_id_list, tid);
        clientCnt++; // total client connections made.
    }

    close(listen_fd);
    close_server(thread_id_list, clientCnt, maxDonations, charities);
    
    close(log_fd);
    DeleteList(thread_id_list);
    free(thread_id_list);
    destroy_locks();
    return 0;
}

void* client_work(void* clientfd_ptr) {
    int client_fd  = *((int*) clientfd_ptr);
    free(clientfd_ptr);

    // install sigusr1 handler
    struct sigaction sa = {{0}};
    sa.sa_handler = sigusr1_handler;
    Sigaction(SIGUSR1, &sa, NULL); 

    uint64_t total_donations = 0; // thread tracking donations to charities.
    message_t msg;
    // zero out
    while(1){
        if (sigusr1_flag == 1) {
            break;
        }
        memset(&msg, 0, sizeof(message_t));
        int size = read(client_fd, &msg, sizeof(message_t));
        if (size == -1) {
            if (errno == EINTR) {
                if (sigusr1_flag == 1)
                    break;
                continue;
            }
            break;
        }
        if (size == 0) {
            break; // client disconnected
        }
        if (size < sizeof(message_t)) {
            client_error(&msg, client_fd);
            continue;
        }
        switch(msg.msgtype) {
            case DONATE:
                if (client_donate(&msg, client_fd) == 0)
                    total_donations += msg.msgdata.donation.amount;
                break;
            case CINFO:
                client_info(&msg, client_fd);
                break;
            case TOP:
                client_top(&msg, client_fd);
                break;
            case LOGOUT:
                client_logout(client_fd, total_donations);
                return NULL;
            default:
                client_error(&msg, client_fd);
                break;
        }
    }
    close(client_fd);
    update_maxDonations(total_donations); //WIP
    return NULL;
    
}

int socket_listen_init(int server_port){
    int sockfd;
    struct sockaddr_in servaddr;

    // socket create and verification
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd == -1) {
        printf("socket creation failed...\n");
        exit(EXIT_FAILURE);
    }
    else
        printf("Socket successfully created\n");

    bzero(&servaddr, sizeof(servaddr));

    // assign IP, PORT
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr.sin_port = htons(server_port);

    int opt = 1;

    if(setsockopt(sockfd, SOL_SOCKET, SO_REUSEPORT, (char *)&opt, sizeof(opt))<0)
    {
    	perror("setsockopt");exit(EXIT_FAILURE);
    }

    // Binding newly created socket to given IP and verification
    if ((bind(sockfd, (SA*)&servaddr, sizeof(servaddr))) != 0) {
        printf("socket bind failed\n");
        exit(EXIT_FAILURE);
    }
    else
        printf("Socket successfully binded\n");

    // Now server is ready to listen and verification
    if ((listen(sockfd, 1)) != 0) {
        printf("Listen failed\n");
        exit(EXIT_FAILURE);
    }
    return sockfd;
}


