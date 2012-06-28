#ifndef __CPU_H
#define __CPU_H

extern double get_cpu_frequency(void);
extern void   bind_thread_to_cpu(int);

static inline uint64_t cycle_timer(void) {
    uint32_t __a,__d;
    uint64_t val;

    //cpuid();
    asm volatile("rdtsc" : "=a" (__a), "=d" (__d));
    (val) = ((uint64_t)__a) | (((uint64_t)__d)<<32);
    return val;
}

static inline double get_microseconds(double cpu_freq) {
    return cycle_timer() / cpu_freq;
}


static inline uint64_t get_microsecond_from_tsc(uint64_t count,
                                                double cpu_frequency) {
    return count / cpu_frequency;
}


#include <stdio.h>
#include <sys/param.h>
#include <sys/types.h>
#include <sys/time.h>
#ifdef __FreeBSD__
#include <sys/resource.h>
#include <sys/cpuset.h>
#else
//#define __USE_GNU
#include <sched.h>
#endif
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <err.h>

#define MEGAHERTZ 1000000

static inline void cpuid(void) {
    uint32_t regs[4];
    uint32_t *p = regs;
    uint32_t ax = 0x0000001;

    __asm __volatile("cpuid"
                     : "=a" (p[0]), "=b" (p[1]), "=c" (p[2]), "=d" (p[3])
                     :  "0" (ax));
}

static long get_usec_interval(struct timeval *start, struct timeval *end) {
    return (((end->tv_sec - start->tv_sec) * MEGAHERTZ)
            + (end->tv_usec - start->tv_usec));
}


double get_cpu_frequency(void) {
    struct timeval start;
    struct timeval end;
    uint64_t tsc_start;
    uint64_t tsc_end;
    long usec;

    if (gettimeofday(&start, 0)) {
        err(1, "gettimeofday");
    }

    tsc_start = cycle_timer();
    usleep(10000);

    if (gettimeofday(&end, 0)) {
        err(1, "gettimeofday");
    }
    tsc_end = cycle_timer();
    usec = get_usec_interval(&start, &end);
    return (tsc_end - tsc_start) * 1.0 / usec;
}


#if defined(__FreeBSD__)
void bind_thread_to_cpu(int cpuid) {
    cpuset_t mask;

    memset(&mask, 0, sizeof(mask));

    // bind this thread to a single cpu
    CPU_SET(cpuid, &mask);
    if (cpuset_setaffinity(CPU_LEVEL_WHICH, CPU_WHICH_TID, -1,
                           sizeof(mask), &mask) < 0) {
      errx(1, "cpuset_setaffinity");
      return;
    }
}
#elif defined(__APPLE_CC__)
void bind_thread_to_cpu(int cpuid) {
    static int warned = 0;

    if (!warned) {
        warned = 1;
        fprintf(stderr, "WARNING: processor affinity not supported\n");
    }
}
#else
void bind_thread_to_cpu(int cpuid) {
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(cpuid, &mask);
    if (sched_setaffinity(0, sizeof(cpu_set_t), &mask)) {
	err(1, "sched_setaffinity");
    }
}
#endif


#endif
