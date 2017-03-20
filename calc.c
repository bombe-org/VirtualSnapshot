#include "threadpool.h"   //线程池
#include <stdio.h>  
#include <stdlib.h>  
#include <time.h>
#include <set>
#define REST 0
#define PREPARE 1
#define RESOLVE 2
#define CAPTURE 3
#define COMPLETE 4

#define PAGE_SIZE 4096

typedef struct
{
	long long int size;
	char* live;
	char* stable;
	int* bit;
	pthread_mutex_t* live_lock;
	pthread_mutex_t* stable_lock;
	int STATE;
} database;

database global_db;
int is_finished = 0;     //程序是否结束
long long int count = 0;    // 事务编号
std::set<long long int> active_set;
std::set<long long int> prepare_set;
std::set<long long int> complete_set;

int workload_thread;
int workload_buffer;


void load_db(long long int size)
{
	global_db.size = size;
	global_db.live = (char *) malloc(PAGE_SIZE * global_db.size);
	global_db.stable = (char *) malloc(PAGE_SIZE * global_db.size);
	global_db.bit = (int *) malloc(PAGE_SIZE * sizeof(int));
	global_db.live_lock = (pthread_mutex_t *) malloc(PAGE_SIZE * sizeof(pthread_mutex_t));
	global_db.stable_lock = (pthread_mutex_t *) malloc(PAGE_SIZE * sizeof(pthread_mutex_t));
	global_db.STATE = REST;

	int i = 0;
	while(i<size)
	{
		pthread_mutex_init(&(global_db.live_lock[i]),NULL);
		pthread_mutex_init(&(global_db.stable_lock[i]),NULL);
		i++;
	}
}

//   采用两阶段锁操作并发事务
void* trans(void* info)
{
    long long int transID = ++count;
    int start_state = global_db.STATE;
    //  在REST或者COMPLETE阶段提交的事务，放到活动集合
    if(start_state = REST || start_state == COMPLETE)     
    {
    	active_set.insert(transID);
    }
    if(start_state = PREPARE)
    {
    	prepare_set.insert(transID);
    }
    if(start_state = RESOLVE || start_state == CAPTURE)
    {
    	complete_set.insert(transID);
    }

    long long int index1 = rand() % (global_db.size); 
    int value1 = rand();
    long long int index2 = rand() % (global_db.size); 
    int value2 = rand();
    long long int index3 = rand() % (global_db.size); 
    int value3 = rand();
    long long int index4 = rand() % (global_db.size); 
    int value4 = rand();
    long long int index5 = rand() % (global_db.size); 
    int value5 = rand();

    pthread_mutex_lock(&(global_db.live_lock[index1]));
    pthread_mutex_lock(&(global_db.stable_lock[index1]));
    pthread_mutex_lock(&(global_db.live_lock[index2]));
    pthread_mutex_lock(&(global_db.stable_lock[index2]));
    pthread_mutex_lock(&(global_db.live_lock[index3]));
    pthread_mutex_lock(&(global_db.stable_lock[index3]));
    pthread_mutex_lock(&(global_db.live_lock[index4]));
    pthread_mutex_lock(&(global_db.stable_lock[index4]));
    pthread_mutex_lock(&(global_db.live_lock[index5]));
    pthread_mutex_lock(&(global_db.stable_lock[index5]));

    if(start_state == PREPARE)
    	if(global_db.bit[index1] == 0)
    		global_db.stable[index1] == global_db.live[index1];
    else if(start_state == RESOLVE || start_state == CAPTURE)
    	if(global_db.bit[index1] == 0)
    	{
    		global_db.stable[index1] == global_db.live[index1];
    		global_db.bit[index1] = 1;
    	}
    else if(start_state == COMPLETE || start_state == REST)
    	if(global_db.stable[index1]);

    global_db.live[index1] = value1;

	if(start_state == PREPARE)
    	if(global_db.bit[index2] == 0)
    		global_db.stable[index2] == global_db.live[index2];
    else if(start_state == RESOLVE || start_state == CAPTURE)
    	if(global_db.bit[index2] == 0)
    	{
    		global_db.stable[index2] == global_db.live[index2];
    		global_db.bit[index2] = 1;
    	}
    else if(start_state == COMPLETE || start_state == REST)
    	if(global_db.stable[index2]);

    global_db.live[index2] = value2;

    if(start_state == PREPARE)
    	if(global_db.bit[index3] == 0)
    		global_db.stable[index3] == global_db.live[index3];
    else if(start_state == RESOLVE || start_state == CAPTURE)
    	if(global_db.bit[index3] == 0)
    	{
    		global_db.stable[index3] == global_db.live[index3];
    		global_db.bit[index3] = 1;
    	}
    else if(start_state == COMPLETE || start_state == REST)
    	if(global_db.stable[index3]);

    global_db.live[index3] = value3;

    if(start_state == PREPARE)
    	if(global_db.bit[index4] == 0)
    		global_db.stable[index4] == global_db.live[index4];
    else if(start_state == RESOLVE || start_state == CAPTURE)
    	if(global_db.bit[index4] == 0)
    	{
    		global_db.stable[index4] == global_db.live[index4];
    		global_db.bit[index4] = 1;
    	}
    else if(start_state == COMPLETE || start_state == REST)
    	if(global_db.stable[index4]);

    global_db.live[index4] = value4;

    if(start_state == PREPARE)
    	if(global_db.bit[index5] == 0)
    		global_db.stable[index5] == global_db.live[index5];
    else if(start_state == RESOLVE || start_state == CAPTURE)
    	if(global_db.bit[index5] == 0)
    	{
    		global_db.stable[index5] == global_db.live[index5];
    		global_db.bit[index5] = 1;
    	}
    else if(start_state == COMPLETE || start_state == REST)
    	if(global_db.stable[index5]);
    	

    global_db.live[index5] = value5;
	printf("commit trans: %lld\n",transID);
//开始提交
    int commit_state = global_db.STATE;
	if(start_state == PREPARE)
		if(commit_state == RESOLVE)
		{
			global_db.bit[index1]=1;
			global_db.bit[index2]=1;
			global_db.bit[index3]=1;
			global_db.bit[index4]=1;
			global_db.bit[index5]=1;
		}

    pthread_mutex_unlock(&(global_db.live_lock[index1]));pthread_mutex_unlock(&(global_db.stable_lock[index1]));
    pthread_mutex_unlock(&(global_db.live_lock[index2]));pthread_mutex_unlock(&(global_db.stable_lock[index2]));
    pthread_mutex_unlock(&(global_db.live_lock[index3]));pthread_mutex_unlock(&(global_db.stable_lock[index3]));
    pthread_mutex_unlock(&(global_db.live_lock[index4]));pthread_mutex_unlock(&(global_db.stable_lock[index4]));
    pthread_mutex_unlock(&(global_db.live_lock[index5]));pthread_mutex_unlock(&(global_db.stable_lock[index5]));

    if(start_state = REST)     //  在REST阶段提交的事务，放到活动集合
    {
    	active_set.erase(transID);
    }
    if(start_state = PREPARE)
    {
    	prepare_set.erase(transID);
    }
    if(start_state = RESOLVE || global_db.STATE == CAPTURE)
    {
    	complete_set.erase(transID);
    }
}

void* workload(void* argv)
{
	struct threadpool *pool = threadpool_init(10,200);
	while(1)
	{
		threadpool_add_job(pool,trans,NULL);
		usleep(10);
		if(1 == is_finished)  break;
	}
	threadpool_destroy(pool);
}


void runcheckpointer(int num)
{
	while(num--)
	{
		printf("REST\n");
		global_db.STATE = REST;
		sleep(10);
		global_db.STATE = PREPARE;
		printf("prepare\n" );
		while(!active_set.empty());
		global_db.STATE = RESOLVE;
		printf("RESOLVE\n");
		while(!prepare_set.empty());
		printf("CAPTURE\n");
		global_db.STATE = CAPTURE;

		int i = 0;
		while(i < global_db.size)
		{
			if(global_db.bit[i] == 1)
			{
				//global_db.stable[i];  // dump
			}			
			else if(global_db.bit[i] == 0)
			{
				global_db.bit[i] == 1;
				//global_db.live[i];   //dump
			}
			i++;
		}
		printf("COMPLETE\n");
		global_db.STATE = COMPLETE;
		while(!complete_set.empty());
	}
	is_finished = 1;
}

int main(int argc, char const *argv[])
{
	srand((unsigned)time(NULL));
	load_db(atoi(argv[1]));
	pthread_t workload_pid;
	workload_thread = 1;
	workload_buffer = 1;
	pthread_create(&workload_pid,NULL,workload,NULL);
	runcheckpointer(10);    
	return 0;
}
