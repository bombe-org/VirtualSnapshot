#include <pthread.h>
#include <stdio.h>  
#include <unistd.h>
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
long long int throughput = 0;
std::set<long long int> active_set;
std::set<long long int> prepare_set;
std::set<long long int> complete_set;

int workload_thread;
int workload_buffer;
int work = 0;

void load_db(long long int size)
{
	global_db.size = size;
	global_db.live = (char *) malloc(PAGE_SIZE * global_db.size);
	global_db.stable = (char *) malloc(PAGE_SIZE * global_db.size);
	global_db.bit = (int *) malloc(PAGE_SIZE * sizeof(int));
	global_db.live_lock = (pthread_mutex_t *) malloc(PAGE_SIZE * sizeof(pthread_mutex_t));
	global_db.stable_lock = (pthread_mutex_t *) malloc(PAGE_SIZE * sizeof(pthread_mutex_t));
	global_db.STATE = REST;

	long long int i = 0;
	while(i<size)
	{
		pthread_mutex_init(&(global_db.live_lock[i]),NULL);
		pthread_mutex_init(&(global_db.stable_lock[i]),NULL);
		i++;
	}
}

// long long int index1 = 12; int value1 = 333;
// long long int index2 = 34; int value2 = 333;
// long long int index3 = 56; int value3 = 333;
// long long int index4 = 78; int value4 = 333;
// long long int index5 = 123; int value5 = 333;
pthread_mutex_t mutex_count;

//   采用两阶段锁操作并发事务
void* transaction(void* info)
{
    pthread_mutex_lock(&mutex_count);
    long long int transactionID = ++count;
    pthread_mutex_unlock(&mutex_count);

    int start_state = global_db.STATE;
    //  在REST或者COMPLETE阶段提交的事务，放到活动集合

    work--;
    if(start_state == REST)
    {
        active_set.insert(transactionID);
    }
    else if(start_state == COMPLETE)
    {
    	active_set.insert(transactionID);
	}

    else if(start_state == PREPARE)
    {
        prepare_set.insert(transactionID);
    }
	else if(start_state == RESOLVE)
	{
    	complete_set.insert(transactionID);
	}
	else if(start_state == CAPTURE)
    {
        complete_set.insert(transactionID);
    }

    long long int index1 = rand() % (global_db.size);    int value1 = rand();
    pthread_mutex_lock(&(global_db.live_lock[index1]));    
    pthread_mutex_lock(&(global_db.stable_lock[index1]));
    
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
    long long int index2 = rand() % (global_db.size);    int value2 = rand();
    pthread_mutex_lock(&(global_db.live_lock[index2]));    
    pthread_mutex_lock(&(global_db.stable_lock[index2]));
    
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
    long long int index3 = rand() % (global_db.size);    int value3 = rand();
    pthread_mutex_lock(&(global_db.live_lock[index3]));    
    pthread_mutex_lock(&(global_db.stable_lock[index3]));
    
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
    long long int index4 = rand() % (global_db.size);    int value4 = rand();
    pthread_mutex_lock(&(global_db.live_lock[index4]));    
    pthread_mutex_lock(&(global_db.stable_lock[index4]));
    
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
    long long int index5 = rand() % (global_db.size);    int value5 = rand();
    pthread_mutex_lock(&(global_db.live_lock[index5]));    
    pthread_mutex_lock(&(global_db.stable_lock[index5]));
    
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
    usleep(1000);
	printf("#");
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

    if(start_state == REST)
    {
        active_set.erase(transactionID);
    }
    else if(start_state == COMPLETE)
    {
    	active_set.insert(transactionID);
	}
	else if(start_state == RESOLVE)
	{
    	complete_set.erase(transactionID);
	}
    else if(start_state == PREPARE)
    {
        prepare_set.insert(transactionID);
    }
    else if(start_state == CAPTURE)
    {
        complete_set.insert(transactionID);
    }

    pthread_mutex_unlock(&(global_db.live_lock[index1]));    pthread_mutex_unlock(&(global_db.stable_lock[index1]));
    pthread_mutex_unlock(&(global_db.live_lock[index2]));    pthread_mutex_unlock(&(global_db.stable_lock[index2]));
    pthread_mutex_unlock(&(global_db.live_lock[index3]));    pthread_mutex_unlock(&(global_db.stable_lock[index3]));
    pthread_mutex_unlock(&(global_db.live_lock[index4]));    pthread_mutex_unlock(&(global_db.stable_lock[index4]));
    pthread_mutex_unlock(&(global_db.live_lock[index5]));    pthread_mutex_unlock(&(global_db.stable_lock[index5]));
	
    pthread_exit(NULL);
}

void* workload(void* argv)
{	
	while(1)
	{

		if(work > 0)
        {
            
            pthread_t pid_t;
            pthread_create(&pid_t,NULL,transaction,NULL);    
            usleep(1000);		
        }	
        else
            work = 100;	
	}	
}


void checkpointer(int num)
{
	while(num--)
	{
		printf("\nREST\n");
		global_db.STATE = REST;
		sleep(1);
		global_db.STATE = PREPARE;
		printf("\nprepare\n" );
		while(!active_set.empty());
		global_db.STATE = RESOLVE;
		printf("\nRESOLVE\n");
		while(!prepare_set.empty());
		printf("\nCAPTURE\n");
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
            //printf("%d ", i);	
            //sleep(8);
			i++;		
		}

		printf("\nCOMPLETE\n");
		global_db.STATE = COMPLETE;
		while(!complete_set.empty());
	}
	is_finished = 1;
}

int main(int argc, char const *argv[])
{
	srand((unsigned)time(NULL));
    pthread_mutex_init(&mutex_count,NULL);
    //pthread_mutex_init(&mutex_throuthput,NULL);
	//load_db(atoi(argv[1]));
	load_db(1000);
	pthread_t workload_pid;
	//workload_thread = atoi(argv[2]);	
	pthread_create(&workload_pid,NULL,workload,NULL);
	checkpointer(10);
	return 0;
}
