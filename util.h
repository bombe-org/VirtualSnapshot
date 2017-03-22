#pragma once

long long get_ntime(void)
{
    struct timespec timeNow;
    clock_gettime(CLOCK_MONOTONIC, &timeNow);
    return timeNow.tv_sec * 1000000000 + timeNow.tv_nsec;
}

long long get_utime(void)
{
    return get_ntime() / 1000;
}

long long get_mtime(void)
{
	return get_ntime() / 1000000;
}

long long get_time(void)
{
     struct timespec timeNow;
    clock_gettime(CLOCK_MONOTONIC, &timeNow);
    return timeNow.tv_sec;
}


//max_min函数定义太乱了，应该是这样的：

void max_min(int a[],long long int n,int *maxp,int *minp)
{
    long long int i;
    *maxp=a[0];
    *minp=a[0];
    for(i=1;i<=n-1;i++)
    {
		if(a[i]>=*maxp)
		*maxp=a[i];
		else if (a[i]<*minp) 
		*minp=a[i];
    }
}