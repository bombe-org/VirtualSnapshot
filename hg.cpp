#include <pthread.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <fcntl.h>
#include <math.h>

#define lock(lkp) do{  \
    while(!__sync_bool_compare_and_swap(lkp, 0, 1)){    \
        sched_yield();  \
    }   \
}while(0)

#define unlock(lkp) do{    \
    *(lkp) = 0;  \
}while(0)
 
#define try_lock(lkp) ({   \
    (__sync_bool_compare_and_swap(lkp, 0, 1) ? 0 : -1);   \
})

#include "util.h"
#include <fstream>

using namespace std;

#define LINE_SIZE 4096

typedef struct {
    long long int size;
    char *D1;
    char *D2;
    int *bit1;
    int *bit2;
    int *bitr;
    pthread_mutex_t *D1_lock;
    pthread_mutex_t *D2_lock;
} database;

database global_db;
int is_finished = 0;     //程序是否结束
long long int throughput = 0;   //  系统最大并行度
long long int active0 = 0;
long long int active1 = 0;
int peroid = 0;

int *sec_throughput;
long long int timestamp = 0;


int no_dead_lock1 = 0;
int no_dead_lock2 = 0;
int no_dead_lock3 = 0;
int no_dead_lock4 = 0;



void load_db(long long int size) {
    global_db.size = size;
    global_db.D1 = (char *) malloc(global_db.size * LINE_SIZE);
    global_db.D2 = (char *) malloc(global_db.size * LINE_SIZE);
    global_db.bit1 = (int *) malloc(global_db.size * sizeof(int));
    global_db.bit2 = (int *) malloc(global_db.size * sizeof(int));
    global_db.bitr = (int *) malloc(global_db.size * sizeof(int));

    global_db.D1_lock = (pthread_mutex_t *) malloc(global_db.size * sizeof(pthread_mutex_t));
    global_db.D2_lock = (pthread_mutex_t *) malloc(global_db.size * sizeof(pthread_mutex_t));
    long long int i = 0;
    while (i < global_db.size) {
        pthread_mutex_init(&(global_db.D1_lock[i]), NULL);
        pthread_mutex_init(&(global_db.D2_lock[i]), NULL);
        i++;
    }
}

void unit_write0(long long int index1) {
    int k = 0;
    int rnd;
    rnd = rand();
    while (k++ < 1024) {
        memcpy(global_db.D1 + LINE_SIZE * index1 + 4 * k, &rnd, 4);
    }
    global_db.bit1[index1] = 1;
    global_db.bitr[index1] = 0;
}

void unit_write1(long long int index1) {
    int k = 0;
    int filed;
    filed = rand();
    while (k++ < 1024) {
        memcpy(global_db.D2 + LINE_SIZE * index1 + 4 * k, &filed, 4);
    }
    global_db.bit2[index1] = 1;
    global_db.bitr[index1] = 1;
}

// 采用两阶段锁操作并发事务
void work0() {
    int i = 0;
	while(i++<8){
		long long int index1 = rand() % (global_db.size);   //int value1 = rand();
    unit_write0(index1);
    }
	++sec_throughput[timestamp];
}

void work1() {
	int i = 0;
	while(i++<8){
    long long int index1 = rand() % (global_db.size);   //int value1 = rand();
    unit_write1(index1);
	}
    ++sec_throughput[timestamp];
}

void *transaction(void *info) {
    while (is_finished == 0) {
        int p = peroid;
        if (p % 2 == 0) {
            __sync_fetch_and_add(&active0, 1);
            work0();
            __sync_fetch_and_sub(&active0, 1);
        } else if (p % 2 == 1) {
            __sync_fetch_and_add(&active1, 1);
            work1();
            __sync_fetch_and_sub(&active1, 1);
        }
    }
}


void *timer(void *info) {
    while (is_finished == 0) {
        sleep(1);
        //printf("%d\n",sec_throughput[timestamp]);
        ++timestamp;
    }
}

void checkpointer(int num) {
    while (num--) {
        int p = peroid;   // peroid的作用相当于指针交换
        long long int i;
        if (p % 2 == 1) {
            while (active0 > 0);
            i = 0;
            ofstream ckp_fd("dump.dat", ios::binary);
            while (i < global_db.size) {
                if (global_db.bit1[i] == 1) {
                    ckp_fd.write(global_db.D1 + i * LINE_SIZE, LINE_SIZE);
                    global_db.bit1[i] == 0;
                } else {
                    ckp_fd.write(global_db.D1 + i * LINE_SIZE, LINE_SIZE);
                }
                i++;
            }
        } else if (p % 2 == 0) {
            while (active1 > 0);
            i = 0;
            ofstream ckp_fd("dump.dat", ios::binary);
            while (i < global_db.size) {
                if (global_db.bit2[i] == 1) {
                    ckp_fd.write(global_db.D2 + i * LINE_SIZE, LINE_SIZE);
                    global_db.bit2[i] == 0;
                } else {
                    ckp_fd.write( global_db.D2 + i * LINE_SIZE, LINE_SIZE);
                }
                i++;
            }
        }
        peroid++;
        sleep(1);
    }
    is_finished = 1;
}

int main(int argc, char const *argv[]) {
    srand((unsigned) time(NULL));
    load_db(atoi(argv[1]));

    sec_throughput = (int *) malloc(3600 * sizeof(int));  // assume running 1000s
    for (int j = 0; j < 1000; ++j) {
        sec_throughput[j] = 0;
    }
    throughput = atoi(argv[2]);

    for (int i = 0; i < throughput; ++i) {
        pthread_t pid_t;
        pthread_create(&pid_t, NULL, transaction, NULL);
    }
    pthread_t time_thread;
    pthread_create(&time_thread, NULL, timer, NULL);
    checkpointer(5);
    long long int sum = 0;
    for (int i = timestamp/4; i < timestamp/4*3; ++i) {
        sum += sec_throughput[i];
    }
    printf("HG,%d,%lld,%lld,%f\n", atoi(argv[1]), throughput, timestamp, (float) sum / timestamp * 2);
    return 0;
}
