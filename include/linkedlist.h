// DO NOT MODIFY THIS FILE
// Any additions should be placed in hw2_helpers.h

#ifndef LINKEDLIST_H
#define LINKEDLIST_H

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

typedef struct node {
    pthread_t* data;          // pointer to the data to be stored
    struct node* next;   // pointer to the next node in the linked list
    struct node* prev;
} node_t;

typedef struct list {
    node_t* head;        // pointer to the node_t at the head of the list
    int length;          // number of items in the list
    void (*deleter)(void*);              // function pointer for deleting any dynamically 
                                         // allocated items within the data stored
} list_t;

// Functions implemented/provided in linkedList.c
list_t* CreateList(void (*delete)(void*));

void InsertAtHead(list_t* list, pthread_t tid);

void RemoveFromList(list_t* list, node_t* node);

void DeleteList(list_t* list);


#endif
