#ifndef TIME_H
#define TIME_H
#include <cstddef>
#include <sys/time.h>
inline timeval ___StatTime_Start()
{
	timeval t1;
	gettimeofday(&t1, NULL);
	return t1;
}
inline double ___StatTime_End(timeval t1)
{
	timeval t2;
	gettimeofday(&t2, NULL);
	double  time1 = 1000000*(t2.tv_sec - t1.tv_sec) + t2.tv_usec-t1.tv_usec;
	time1 = time1/1000000;
	return time1;
}

#endif
