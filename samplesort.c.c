/*
# Filename: psort.c
# Student name and No.: Martin K. Mwai 3035716804
# Development platform:  Ubuntu
# Remark: Completed
*/

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <math.h>
#include <errno.h>
#include <sys/time.h>

int checking(unsigned int *, int );
int compare(const void *, const void *);

long size;            // size of the array
unsigned int *intarr; // main array
unsigned int *samples;       // sample array (sampled elements)
int  samples_transverser = 0;
unsigned int *pivots; // pivot array
unsigned int *partition_handler; // partition array
unsigned int *thread_size;      // element size array
unsigned int *final;        // final output array
int  final_transverser = 0;         // transverses the final array
int id_checker = 0;
int nil =  0;
int one = 1;
int all_threads;   // total number of worker threads
int thread_var = 0;

pthread_mutex_t main_lock = PTHREAD_MUTEX_INITIALIZER; // mutex lock to switch between threads
pthread_cond_t gatekeeper_cond = PTHREAD_COND_INITIALIZER;    // gatekeeping condition

void *sort_function(void *id)
{
    unsigned int *manager_thread; // to store and manage thread subsequence

    int *identifier= (int *)id;
    float range_manager_thread = (float)size / (float)all_threads;

    int  front = range_manager_thread * (*identifier);
    int  back = range_manager_thread * (*identifier + one) - one;
    int  size_manager_thread = back - front + one;

    thread_size[*identifier] = size_manager_thread;

    manager_thread = (unsigned int *)malloc(size_manager_thread * sizeof(unsigned int));

    int  transverser = front;

    pthread_mutex_lock(&main_lock);

    int  intarr_transverser = 0; //main array transverser
    while (intarr_transverser < size_manager_thread)
    {
        if (transverser <= back)
        {
            manager_thread[intarr_transverser++] = intarr[transverser++];
        }
    }

    transverser = front;

    qsort(manager_thread, size_manager_thread, sizeof(unsigned int), compare);
    int  thread_transverser = 0;
    while (thread_transverser < size_manager_thread)
    {
        if (transverser <= back)
        {
            intarr[transverser++] = manager_thread[thread_transverser++];
        }
    }

    // conditional to establish synchrony
    while (id_checker != *identifier)
    {
        pthread_cond_wait(&gatekeeper_cond, &main_lock);
    }

    //sampling and extracting from array
    int p = 0;
    while (p < all_threads)
    {
        samples[samples_transverser++] = manager_thread[p * size / (all_threads * all_threads)];
        p++;
    }
    // broadcast and handing over of locks
    pthread_cond_broadcast(&gatekeeper_cond);
    id_checker++;
    pthread_mutex_unlock(&main_lock);
    pthread_mutex_lock(&main_lock);

    for (; id_checker != all_threads + one;)
    {
        pthread_cond_wait(&gatekeeper_cond, &main_lock);
    }

    for (; thread_var != *identifier;)
    {
        pthread_cond_wait(&gatekeeper_cond, &main_lock);
    }

    int partition = 0;       // to store the number of partition
    int  partition_size = 0; // to store the size of partition
    int  partition_transverser = 0;
    int  level = 0;

    partition = *identifier;

    int i = 0;
    while (i < all_threads)
    {
        int  x;
        int  y;

        switch (i)
        {
            case 0:
                x = 0;
                y = thread_size[i];
                break;
            default:
                x = level;
                y = level + thread_size[i];
        }
        int counter = x;
        while (counter < y)
        {
            if (partition == 0)
            {
                if (intarr[counter] <= pivots[partition])
                {
                    partition_size++;
                }
            }
            else if (partition == all_threads - one)
            {
                if (pivots[partition - one] < intarr[counter])
                {
                    partition_size++;
                }
            }
            else
            {
                if (intarr[counter] > pivots[partition - one] && intarr[counter] <= pivots[partition])
                {
                    partition_size++;
                }
            }
            counter++;
        }
        level += thread_size[i];
        i++;
    }

    // Creating an array to store and handle partitions
    partition_handler = (unsigned int *)malloc((partition_size) * sizeof(unsigned int));

    level = 0;

    // Adding Elements to the array created above
    i = 0;
    while (i < all_threads)
    {
        int  x;
        int  y;

        switch (i)
        {
            case 0:
                x = 0;
                y = thread_size[i];
                break;
            default:
                x = level;
                y = level + thread_size[i];
        }

        int  counter = x;
        int place_holder = all_threads - one;
        while (counter < y)
        {
            if  (partition == place_holder)
            {
                if (pivots[partition - one] < intarr[counter] )
                {
                    partition_handler[partition_transverser++] = intarr[counter];
                }
            }
            switch (partition)
            {
                case 0:
                    if (intarr[counter] <= pivots[partition])
                    {
                        partition_handler[partition_transverser++] = intarr[counter];
                    }
                    break;
                default:
                    if (intarr[counter] > pivots[partition - one] && intarr[counter] <= pivots[partition])
                    {
                        partition_handler[partition_transverser++] = intarr[counter];
                    }
            }
            counter++;
        }
        level += thread_size[i];
        i++;
    }

    // Sorting the partition
    qsort(partition_handler, partition_size, sizeof(unsigned int), compare);

    //array allocations for partitions
    for (i = 0; i < partition_size; i++)
    {
        final[final_transverser++] = partition_handler[i];
    }
    pthread_cond_broadcast(&gatekeeper_cond);
    thread_var++;
    pthread_mutex_unlock(&main_lock);
    pthread_exit(NULL);
}

int main(int argc, char **argv)
{
    long  i, j;
    struct timeval start, end;

    switch (argc)
    {
        case 1:
            printf("Usage: p_sort <number>\n");
            exit(0);
        break;
        case 2:
            printf("Usage: p_sort <number>\n");
            exit(0);
        break;
        case 3:
            if (atol(argv[2]) <= 1)
            {
                printf("Number of threads can be lesser or equal to 1!\n");
                exit(0);
            }
            all_threads = atol(argv[2]);
            break;
        default:
            all_threads = 4;
    }

    int thread_ids[all_threads];
    pthread_t threads[all_threads];

    int id = 0;
    while (all_threads > id)
    {
        thread_ids[id] = id;
        id++;
    }

    size = atol(argv[1]);
    intarr = (unsigned int *)malloc(size * sizeof(unsigned int));
    if (intarr == NULL)
    {
        perror("malloc");
        exit(0);
    }

    // set the random seed for generating a fixed random
    // sequence across different runs
    char *env = getenv("RANNUM"); // get the env variable
    if (!env)                     // if not exists
        srand(3230);
    else
        srand(atol(env));

    for (i = 0; i < size; i++)
    {
        intarr[i] = rand();
    }

    // measure the start time
    gettimeofday(&start, NULL);

    // Allocating the memory for sample array, pivots, and others
    samples = (unsigned int *)malloc((all_threads * all_threads) * sizeof(unsigned int));
    final = (unsigned int *)malloc(size * sizeof(unsigned int));
    thread_size = (unsigned int *)malloc(all_threads * sizeof(unsigned int));

    // Creating worker threads
    int thread_number = 0;
    for  (; thread_number < all_threads;)
    {
        pthread_create(&threads[thread_number], NULL, sort_function, (void *)&thread_ids[thread_number]);
        thread_number++;
    }

    pthread_mutex_lock(&main_lock);

    for  (; id_checker != all_threads;)
    {
        pthread_cond_wait(&gatekeeper_cond, &main_lock);
    }

    // Start of Phase 2
    qsort(samples, all_threads * thread_number, sizeof(unsigned int), compare);

    // Identifying Pivot values and storing them in pivots array
    pivots = (unsigned int *) malloc((all_threads - one) * sizeof(unsigned int));

    int pivot_val = 0;
    while (pivot_val < all_threads - one)
    {
        pivots[pivot_val] = samples[(pivot_val + one) * all_threads + (all_threads / 2) - one];
        pivot_val++;
    }

    id_checker++;

    pthread_cond_broadcast(&gatekeeper_cond);

    pthread_mutex_unlock(&main_lock);
    for (i = 0; i < all_threads; i++)
    {
        pthread_join(threads[i], NULL);
    }

    // measure the end time
    gettimeofday(&end, NULL);

    if (!checking(final, size))
    {
        printf("The array is not in sorted order!!\n");
    }

    printf("Total elapsed time: %.4f s\n", (end.tv_sec - start.tv_sec) * 1.0 + (end.tv_usec - start.tv_usec) / 1000000.0);
    return 0;
}

int compare(const void *a, const void *b)
{
    return (*(unsigned int *)a > *(unsigned int *)b) ? 1 : ((*(unsigned int *)a == *(unsigned int *)b) ? 0 : -1);
}

int checking(unsigned int *list, int  size)
{
    int  i;
    printf("First : %d\n", list[0]);
    printf("At 25%%: %d\n", list[size / 4]);
    printf("At 50%%: %d\n", list[size / 2]);
    printf("At 75%%: %d\n", list[3 * size / 4]);
    printf("Last  : %d\n", list[size - 1]);
    for (i = 0; i < size - 1; i++)
    {
        if (list[i] > list[i + 1])
        {
            return 0;
        }
    }
    return 1;
}
