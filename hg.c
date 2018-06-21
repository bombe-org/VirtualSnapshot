#include <pthread.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <fcntl.h>
#include <math.h>


#include "util.h"
#define REST 0
#define PREPARE 1
#define LINE_SIZE 4096

typedef struct {
    long long int size;
    char* D1;
    char* D2;
    int* bit1;
    int* bit2;
    int* bitr;
    pthread_mutex_t* D1_lock;
    pthread_mutex_t* D2_lock;
    int STATE;
} database;

database global_db;
int is_finished = 0;     //程序是否结束
long long int throughput = 0;   //  系统最大并行度
long long int active0=0;
long long int active1=0;
int peroid=0;

int* msec_throughput;
long long int timestamp = 0;
int ckp_fd;  // 文件描述符



void load_db(long long int size) {
    global_db.size = size;
    global_db.STATE = REST;
    global_db.D1 =      (char *) malloc(global_db.size * LINE_SIZE);
    global_db.D2 =      (char *) malloc(global_db.size * LINE_SIZE);
    global_db.bit1 =    (int *)  malloc(global_db.size * sizeof(int));
    global_db.bit2 =    (int *)  malloc(global_db.size * sizeof(int));
    global_db.bitr =    (int *)  malloc(global_db.size * sizeof(int));

    global_db.D1_lock =   (pthread_mutex_t *) malloc(global_db.size * sizeof(pthread_mutex_t));
    global_db.D2_lock =   (pthread_mutex_t *) malloc(global_db.size * sizeof(pthread_mutex_t));
    long long int i = 0;
    while(i < global_db.size) {
        pthread_mutex_init(&(global_db.D1_lock[i]),NULL);
        pthread_mutex_init(&(global_db.D2_lock[i]),NULL);
        i++;
    }
}

void unit_write0(long long int index1)
{
    global_db.bit1[index1] = 1;
    int k =0;
	int rnd;
	rnd = rand();
    while(k++ < 1024)
    {		
        memcpy( global_db.D1 + LINE_SIZE * index1 + 4*k , &rnd, 4);
    }
    global_db.bitr[index1] = 0;
}

void unit_write1(long long int index1)
{
    global_db.bit2[index1] = 1;
    int k =0;
	int filed;
	filed = rand();
    while(k++ < 1024)
    {		
        memcpy( global_db.D2 + LINE_SIZE * index1 + 4*k , &filed, 4);
    }
    global_db.bitr[index1] = 1;
}

// 采用两阶段锁操作并发事务
void work0()
{
    //long long int start_time = get_ntime();
    long long int index1 = rand() % (global_db.size);   //int value1 = rand();
    long long int index2 = rand() % (global_db.size);   //int value2 = rand();
    long long int index3 = rand() % (global_db.size);   //int value3 = rand();
    //pthread_mutex_lock(&(global_db.D1_lock[index1]));
    unit_write0(index1);
    //pthread_mutex_lock(&(global_db.D1_lock[index2]));
    unit_write0(index2);
    //pthread_mutex_lock(&(global_db.D1_lock[index3]));
    unit_write0(index3);
    //pthread_mutex_unlock(&(global_db.D1_lock[index1]));
    //pthread_mutex_unlock(&(global_db.D1_lock[index2]));
    //pthread_mutex_unlock(&(global_db.D1_lock[index3]));
    ++msec_throughput[timestamp];
}

void work1()
{
    //long long int start_time = get_ntime();
    long long int index1 = rand() % (global_db.size);   //int value1 = rand();
    long long int index2 = rand() % (global_db.size);   //int value2 = rand();
    long long int index3 = rand() % (global_db.size);   //int value3 = rand();
    //pthread_mutex_lock(&(global_db.D1_lock[index1]));
    unit_write1(index1);
    //pthread_mutex_lock(&(global_db.D1_lock[index2]));
    unit_write1(index2);
    //pthread_mutex_lock(&(global_db.D1_lock[index3]));
    unit_write1(index3);
    //pthread_mutex_unlock(&(global_db.D1_lock[index1]));
    //pthread_mutex_unlock(&(global_db.D1_lock[index2]));
    //pthread_mutex_unlock(&(global_db.D1_lock[index3]));
    ++msec_throughput[timestamp];
}

void* transaction(void* info) {
    while(is_finished==0)
    {
        int p = peroid;
        if(p%2==0)
        {
            __sync_fetch_and_add(&active0,1);
            work0();
            __sync_fetch_and_sub(&active0,1);
        }else if(p%2==1){
            __sync_fetch_and_add(&active1,1);
            work1();
            __sync_fetch_and_sub(&active1,1);
        }
    }
}


void* timer(void *info){
    while(is_finished==0){
        sleep(1);
        printf("%d\n",msec_throughput[timestamp]);
        ++timestamp;
    }
}

void checkpointer(int num) {
    while(num--) {
        int p = peroid;   // peroid的作用相当于指针交换
        long long int i;
        if(p%2==1){
            while(active0>0);
            i = 0;
            ckp_fd = open("./dump.dat", O_WRONLY | O_TRUNC | O_SYNC | O_CREAT, 666);
            while(i < global_db.size) {
                if(global_db.bit1[i] == 1) {
                    write(ckp_fd, global_db.D1 + i * LINE_SIZE,LINE_SIZE);
                    lseek(ckp_fd, 0, SEEK_END);
                    global_db.bit1[i] == 0;
                } else{
                    write(ckp_fd, global_db.D1 + i * LINE_SIZE,LINE_SIZE);
                    lseek(ckp_fd, 0, SEEK_END);
                }
                i++;
            }
        }
        else if(p%2==0){
            while(active1>0);
            i = 0;
            ckp_fd = open("./dump.dat", O_WRONLY | O_TRUNC | O_SYNC | O_CREAT, 666);
            while(i < global_db.size) {
                if(global_db.bit2[i] == 1) {
                    write(ckp_fd, global_db.D2 + i * LINE_SIZE,LINE_SIZE);
                    lseek(ckp_fd, 0, SEEK_END);
                    global_db.bit2[i] == 0;
                } else{
                    write(ckp_fd, global_db.D2 + i * LINE_SIZE,LINE_SIZE);
                    lseek(ckp_fd, 0, SEEK_END);
                }
                i++;
            }
        }
        peroid++;
        sleep(30);
    }
    is_finished = 1;
}

int main(int argc, char const *argv[]) {
    srand((unsigned)time(NULL));
    load_db(atoi(argv[1]));

    msec_throughput = (int*) malloc(3600 * sizeof(int));  // assume running 1000s
    for (int j = 0; j < 1000; ++j) {
        msec_throughput[j] = 0;
    }
    throughput = atoi(argv[2]);

    for (int i = 0; i < throughput; ++i)
    {
        pthread_t pid_t;
        pthread_create(&pid_t,NULL,transaction,NULL);
    }
    pthread_t time_thread;
    pthread_create(&time_thread, NULL, timer, NULL);
    checkpointer(5);
    return 0;
}
