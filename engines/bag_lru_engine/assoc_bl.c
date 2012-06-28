/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Hash table
 *
 */
#include "config.h"
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>

#include "default_engine_bl.h"

#define hashsize(n) ((uint32_t)1<<(n))
#define hashmask(n) (hashsize(n)-1)


#define PHT 1
#if defined(PHT)

#define NUM_ASSOC_LOCKS 64

void assoc_get_lock(int bucket);
void assoc_release_lock(int bucket);

static pthread_mutex_t striped_cache_locks[NUM_ASSOC_LOCKS]; //ALIGN(CACHE_LINE_SIZE);

void assoc_get_lock(int bucket){
    int lock_number = bucket % NUM_ASSOC_LOCKS;
    pthread_mutex_lock(&striped_cache_locks[lock_number]);
}

void assoc_release_lock(int bucket){
    int lock_number = bucket % NUM_ASSOC_LOCKS;
    pthread_mutex_unlock(&striped_cache_locks[lock_number]);
}


#endif

ENGINE_ERROR_CODE assoc_init(struct default_engine *engine) {

    
    engine->assoc.hashpower = 22;

    engine->assoc.primary_hashtable = calloc(hashsize(engine->assoc.hashpower), sizeof(void *));
	
#if defined(PHT)
    int i;
    for (i = 0; i < NUM_ASSOC_LOCKS; i++){
        pthread_mutex_init(&striped_cache_locks[i], NULL);
    }
    engine->assoc.insert_primary = 0;
#endif
    return (engine->assoc.primary_hashtable != NULL) ? ENGINE_SUCCESS : ENGINE_ENOMEM;
}

#if defined(PHT)

/* finds an item in the hash table                                            *
 * could return a false negative in a single case:                            *
 * while traversing the correct hash chain, this thread gets context swicthed *
 * and the item we are on gets deleted from the hash chain - delete locks and *
 * maintaining the next pointer until item_free() should prevent this         */
hash_item *assoc_find_unlocked(struct default_engine *engine, uint32_t hash, const char *key, const size_t nkey) {
    int bucket = hash & hashmask(engine->assoc.hashpower);
    hash_item *it;
    hash_item ** volatile hashchain_bucket;

    if(!engine->assoc.expanding){
        hashchain_bucket = &(engine->assoc.primary_hashtable)[bucket];
        it = *hashchain_bucket;
        
	while (it) {
            if ((nkey == it->nkey) && (memcmp(key, item_get_key(it), nkey) == 0) ){
                return it;
            }
            it = it->h_next;
        }

        /* we didnt find it, now we check to see  *
         * if we got passed over by an expand     */
        if(engine->assoc.expanding && bucket < engine->assoc.expand_bucket){
            int new_bucket = hash & hashmask(engine->assoc.hashpower);
            hash_item ** new_hashchain_bucket = &(engine->assoc.new_hashtable)[new_bucket];

            /* wait for the old bucket to be moved  */
            while(*hashchain_bucket != NULL) continue;
            
            it = *new_hashchain_bucket;
	    while (it) {
                if ((nkey == it->nkey) && (memcmp(key, item_get_key(it), nkey) == 0)) {
                    return it;
                }
                it = it->h_next;
            }
        }
    }else{
		/* expansion in progress - watch our back later */
        if(engine->assoc.new_hashpower != 0){
            bucket = hash & hashmask(engine->assoc.new_hashpower-1);
        }

        hashchain_bucket = &(engine->assoc.primary_hashtable)[bucket];

        if(bucket >= engine->assoc.expand_bucket){
            it = *hashchain_bucket;
            /* hasnt made it to our bucket yet */
            while (it) {
                if ((nkey == it->nkey) && (memcmp(key, item_get_key(it), nkey) == 0)) {
                    return it;
                }
                it = it->h_next;
            }
        }
        /* check if the expansion passed us - if so recheck the new bucket*/
        if(bucket < engine->assoc.expand_bucket){
            int new_bucket = hash & hashmask(engine->assoc.hashpower);
            hash_item ** new_hashchain_bucket = &(engine->assoc.new_hashtable)[new_bucket];

            /* wait for the old bucket to be moved */
            while(*hashchain_bucket != NULL) continue;

	    it = *new_hashchain_bucket;
	    while (it) {
                if ((nkey == it->nkey) && (memcmp(key, item_get_key(it), nkey) == 0)) {
                    return it;
                }
                it = it->h_next;
            }
        }
    }
    return NULL;
}


/* this is a find that will ensure we do not get a false negative  *
 * used in deletes and sets for memcached integrity                */
hash_item *assoc_find(struct default_engine *engine, uint32_t hash, const char *key, const size_t nkey) {

    int bucket = hash & hashmask(engine->assoc.hashpower);
    assoc_get_lock(bucket); 
    hash_item *it;
    hash_item ** volatile hashchain_bucket;

    if(!engine->assoc.expanding){
        hashchain_bucket = &(engine->assoc.primary_hashtable)[bucket];
        it = *hashchain_bucket;
        while (it) {
            if ((nkey == it->nkey) && (memcmp(key, item_get_key(it), nkey) == 0)) {
                assoc_release_lock(bucket);
                return it;
            }
            it = it->h_next;
        }
        assoc_release_lock(bucket);
    }else{
        if(engine->assoc.new_hashpower != 0){
            assoc_release_lock(bucket);
            bucket = hash & hashmask(engine->assoc.new_hashpower-1);
            assoc_get_lock(bucket);
        }
        if(bucket < engine->assoc.expand_bucket){
            assoc_release_lock(bucket);
            int new_bucket = hash & hashmask(engine->assoc.hashpower);
            assoc_get_lock(new_bucket);
            hash_item **new_hashchain_bucket = &(engine->assoc.new_hashtable)[new_bucket];
            it = *new_hashchain_bucket;

            while (it) {
                if ((nkey == it->nkey) && (memcmp(key, item_get_key(it), nkey) == 0)) {
                    assoc_release_lock(new_bucket);
					return it;
                }
                it = it->h_next;
            }
            assoc_release_lock(new_bucket);
        }else{
            hashchain_bucket = &(engine->assoc.primary_hashtable)[bucket];
            it = *hashchain_bucket;
            while (it) {
                if ((nkey == it->nkey) && (memcmp(key, item_get_key(it), nkey) == 0)) {
                    assoc_release_lock(bucket);
                    return it;
                }
                it = it->h_next;
            }
            assoc_release_lock(bucket);
	}
    }
    return NULL;
}


static void *assoc_maintenance_thread(void *arg);

/* grows the hashtable to the next power of 2. */
static void assoc_expand(struct default_engine *engine) {
    engine->assoc.new_hashtable = calloc(hashsize(engine->assoc.hashpower + 1), sizeof(void *));
    if (engine->assoc.new_hashtable) {
                pthread_t tid;
        pthread_attr_t attr;

        int ret = 0;
        if (pthread_attr_init(&attr) != 0 ||
            pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED) != 0 ||
            (ret = pthread_create(&tid, &attr,
                                  assoc_maintenance_thread, engine)) != 0)
        {
            EXTENSION_LOGGER_DESCRIPTOR *logger;
            logger = (void*)engine->server.extension->get_extension(EXTENSION_LOGGER);
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "Can't create thread: %s\n", strerror(ret));
            free(engine->assoc.new_hashtable);
                        engine->assoc.new_hashtable = NULL;
            engine->assoc.expanding = false;
        }
    } else {
         engine->assoc.expanding = false;
    }
}




/* IF the item exists, we don't do anything (this could be caused by a race condition  */
int assoc_insert(struct default_engine *engine, uint32_t hash, hash_item *it) {
	
    int bucket = hash & hashmask(engine->assoc.hashpower);
    hash_item *iptr;
    hash_item ** volatile hashchain_bucket;
    const char *key = item_get_key(it);
    assoc_get_lock(bucket);

    if(engine->assoc.expanding){
        if(engine->assoc.new_hashpower != 0){
            assoc_release_lock(bucket);
            bucket = hash & hashmask(engine->assoc.new_hashpower-1);
            assoc_get_lock(bucket);
        }
        if(bucket < engine->assoc.expand_bucket ){

            assoc_release_lock(bucket);
            int new_bucket = hash & hashmask(engine->assoc.hashpower);
            assoc_get_lock(new_bucket);

            hashchain_bucket = &(engine->assoc.new_hashtable)[new_bucket];
            iptr = *hashchain_bucket;
            while (iptr) {
                if ((iptr->nkey == it->nkey) && (memcmp(key, item_get_key(iptr), it->nkey) == 0)) {
                    break;
                }
                iptr = iptr->h_next;
            }

            if(!iptr){
                it->h_next = *hashchain_bucket;
	        *hashchain_bucket = it;
                (void)__sync_add_and_fetch(&(engine->assoc.hash_items),1);
            }
            assoc_release_lock(new_bucket);
        }else{
		 if(engine->assoc.insert_primary == 1){
            	 assoc_release_lock(bucket);
                 bucket = hash & hashmask(engine->assoc.hashpower);
                 assoc_get_lock(bucket);
		 hashchain_bucket = &(engine->assoc.primary_hashtable)[bucket];
            }else{
		 hashchain_bucket = &(engine->assoc.primary_hashtable)[bucket];
	    }
	 	 iptr = *hashchain_bucket;
            while (iptr) {
                if ((iptr->nkey == it->nkey) && (memcmp(key, item_get_key(iptr), it->nkey) == 0) ) {
                   break;
                }
                iptr = iptr->h_next;
            }
            if(!iptr){
                it->h_next = *hashchain_bucket;
                *hashchain_bucket = it;
                (void)__sync_add_and_fetch(&(engine->assoc.hash_items),1);
            }
            assoc_release_lock(bucket);
        }
    }else{
        hashchain_bucket = &(engine->assoc.primary_hashtable)[bucket];
        iptr = *hashchain_bucket;
	while (iptr) {
            if ((iptr->nkey == it->nkey) && (memcmp(key, item_get_key(iptr), it->nkey) == 0)) {
                return 1;
            }
            iptr = iptr->h_next;
        }

        if(!iptr){
            it->h_next = *hashchain_bucket;
            *hashchain_bucket = it;
            (void)__sync_add_and_fetch(&(engine->assoc.hash_items),1);
        }
        assoc_release_lock(bucket);
    }
    
    if (!engine->assoc.expanding && engine->assoc.hash_items > (hashsize(engine->assoc.hashpower) * 3) / 2) {
        /* ensure we are the only thread calling the expand */
        if(__sync_bool_compare_and_swap(&engine->assoc.expanding, false, true )){
            assoc_expand(engine);
        }
   } 
    return 1;
}

/* deletes a given key from the hash table */ 
void assoc_delete(struct default_engine *engine, uint32_t hash, const char *key, const size_t nkey) {

    int bucket = hash & hashmask(engine->assoc.hashpower);
    assoc_get_lock(bucket); 
    if(engine->assoc.expanding){
        if(engine->assoc.new_hashpower != 0){
            assoc_release_lock(bucket);
            bucket = hash & hashmask(engine->assoc.new_hashpower-1);
            assoc_get_lock(bucket);
        }
        if(bucket < engine->assoc.expand_bucket){
            assoc_release_lock(bucket);
            int new_bucket = hash & hashmask(engine->assoc.hashpower);
            assoc_get_lock(new_bucket);

            hash_item  *before =  (engine->assoc.new_hashtable)[new_bucket];
            hash_item  *cur_it = before;
            while (cur_it && !((nkey == cur_it->nkey) && (memcmp(key, item_get_key(cur_it), cur_it->nkey) == 0))) {
                before  = cur_it;
                cur_it = before->h_next;
            }

            if (cur_it) {
                hash_item *next = cur_it->h_next;
	        if(before == cur_it){
                 	(engine->assoc.primary_hashtable)[bucket] = next;
           	}else{
                	(before)->h_next = next;
            	}
		assoc_release_lock(new_bucket);
                (void)__sync_fetch_and_sub(&(engine->assoc.hash_items),1);
            }else{
                assoc_release_lock(new_bucket);
            }
        }else{
            hash_item  *before =  (engine->assoc.primary_hashtable)[bucket];
            hash_item  *cur_it = before;
            while (cur_it && !((nkey == cur_it->nkey) && (memcmp(key, item_get_key(cur_it), cur_it->nkey) == 0))) {
                before  = cur_it;
                cur_it = before->h_next;
            }

            if (cur_it) {
                hash_item *next = cur_it->h_next;
            	if(before == cur_it){
                    (engine->assoc.primary_hashtable)[bucket] = next;
            	}else{
                    (before)->h_next = next;
            	}
		assoc_release_lock(bucket);
                (void)__sync_fetch_and_sub(&(engine->assoc.hash_items),1);
            }else{
                assoc_release_lock(bucket);
            }
        }
    }else{
        hash_item  *before =   (engine->assoc.primary_hashtable)[bucket];
        hash_item   *cur_it = before;
        while (cur_it && !((nkey == cur_it->nkey) && (memcmp(key, item_get_key(cur_it), cur_it->nkey) == 0))) {
            before  = cur_it;
            cur_it = before->h_next;
        }

        if (cur_it) {
            hash_item *next = cur_it->h_next;
	    if(before == cur_it){
	   	 (engine->assoc.primary_hashtable)[bucket] = next;	
	    }else{
            	(before)->h_next = next;
            } 

           assoc_release_lock(bucket);
           (void)__sync_fetch_and_sub(&(engine->assoc.hash_items),1);

        }else{
            assoc_release_lock(bucket);
        }
    }
    /* Note:  we shouldn't actually get here.  the callers don't delete things they can't find. */
}


static void *assoc_maintenance_thread(void *arg) {
    	
    hash_item *iptr, *next, *cas_check;
    int bucket, expand_bucket;

    struct default_engine *engine = arg;
    engine->assoc.new_hashpower = engine->assoc.hashpower + 1;
    engine->assoc.hashpower = engine->assoc.new_hashpower;
    engine->assoc.expand_bucket = 0;

    bool done = false;
    do {
		
	assoc_get_lock(engine->assoc.expand_bucket);
        expand_bucket = engine->assoc.expand_bucket++;

	hash_item ** volatile hashchain_bucket;

        for (iptr = (engine->assoc.primary_hashtable)[expand_bucket];
            !(iptr == NULL);
            iptr = next) {
                next = iptr->h_next;
                bucket = engine->server.core->hash(item_get_key(iptr), iptr->nkey, 0)
                    & hashmask(engine->assoc.hashpower);

                hashchain_bucket = &(engine->assoc.new_hashtable)[bucket];
                cas_check = *hashchain_bucket;
                iptr->h_next = cas_check;
                while(!__sync_bool_compare_and_swap( &(engine->assoc.new_hashtable)[bucket], cas_check, iptr)){
                    cas_check = *hashchain_bucket;
                    iptr->h_next = cas_check;
                }
        }

	(engine->assoc.primary_hashtable)[expand_bucket] = NULL;
        assoc_release_lock(expand_bucket);

        if (expand_bucket == (hashsize(engine->assoc.hashpower - 1)-1)) {
            
			
            /* this method with block one thread while it waits     *
			 * for all threads to clear out of the Hash table       *
             * we can hide this blocking using....... MORE THREADS! */
            //expand_thread_barrier();
            //sleep(1);
	    engine->assoc.old_hashtable = engine->assoc.primary_hashtable;
            engine->assoc.primary_hashtable = engine->assoc.new_hashtable;
            
			/*flag for catching a race condition on inserts */
	    engine->assoc.insert_primary = 1;
            engine->assoc.new_hashtable = NULL;
	    engine->assoc.expand_bucket = 0;
            free(engine->assoc.old_hashtable);
            engine->assoc.expanding = false;

            engine->assoc.insert_primary = 0;
            if (engine->config.verbose > 1) {
				EXTENSION_LOGGER_DESCRIPTOR *logger;
				logger = (void*)engine->server.extension->get_extension(EXTENSION_LOGGER);
				logger->log(EXTENSION_LOG_INFO, NULL,
							"Hash table expansion done\n");
	    }
        }
        if (!engine->assoc.expanding) {
            done = true;
        }
    } while (!done);
    return NULL;
}

#else

hash_item *assoc_find(struct default_engine *engine, uint32_t hash, const char *key, const size_t nkey) {

    hash_item *it;
    unsigned int oldbucket;

    if (engine->assoc.expanding &&
        (oldbucket = (hash & hashmask(engine->assoc.hashpower - 1))) >= engine->assoc.expand_bucket)
    {
        it = engine->assoc.old_hashtable[oldbucket];
    } else {
        it = engine->assoc.primary_hashtable[hash & hashmask(engine->assoc.hashpower)];
    }

    hash_item *ret = NULL;
    int depth = 0;
    while (it) {
        if ((nkey == it->nkey) && (memcmp(key, item_get_key(it), nkey) == 0)) {
            ret = it;
            break;
        }
        it = it->h_next;
        ++depth;
    }
    MEMCACHED_ASSOC_FIND(key, nkey, depth);
    return ret;
}

/* returns the address of the item pointer before the key.  if *item == 0,
   the item wasn't found */

static hash_item** _hashitem_before(struct default_engine *engine,
                                    uint32_t hash,
                                    const char *key,
                                    const size_t nkey) {
    hash_item **pos;
    unsigned int oldbucket;

    if (engine->assoc.expanding &&
        (oldbucket = (hash & hashmask(engine->assoc.hashpower - 1))) >= engine->assoc.expand_bucket)
    {
        pos = &engine->assoc.old_hashtable[oldbucket];
    } else {
        pos = &engine->assoc.primary_hashtable[hash & hashmask(engine->assoc.hashpower)];
    }

    while (*pos && ((nkey != (*pos)->nkey) || memcmp(key, item_get_key(*pos), nkey))) {
        pos = &(*pos)->h_next;
    }
    return pos;
}

static void *assoc_maintenance_thread(void *arg);

static void assoc_expand(struct default_engine *engine) {
    engine->assoc.old_hashtable = engine->assoc.primary_hashtable;

    engine->assoc.primary_hashtable = calloc(hashsize(engine->assoc.hashpower + 1), sizeof(void *));
    if (engine->assoc.primary_hashtable) {
        engine->assoc.hashpower++;
        engine->assoc.expanding = true;
        engine->assoc.expand_bucket = 0;

        /* start a thread to do the expansion */
        int ret = 0;
        pthread_t tid;
        pthread_attr_t attr;

        if (pthread_attr_init(&attr) != 0 ||
            pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED) != 0 ||
            (ret = pthread_create(&tid, &attr,
                                  assoc_maintenance_thread, engine)) != 0)
        {
            EXTENSION_LOGGER_DESCRIPTOR *logger;
            logger = (void*)engine->server.extension->get_extension(EXTENSION_LOGGER);
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "Can't create thread: %s\n", strerror(ret));
            engine->assoc.hashpower--;
            engine->assoc.expanding = false;
            free(engine->assoc.primary_hashtable);
            engine->assoc.primary_hashtable =engine->assoc.old_hashtable;
        }
    } else {
        engine->assoc.primary_hashtable = engine->assoc.old_hashtable;
        /* Bad news, but we can keep running. */
    }
}



/* Note: this isn't an assoc_update.  The key must not already exist to call this */
int assoc_insert(struct default_engine *engine, uint32_t hash, hash_item *it) {
    unsigned int oldbucket;

    assert(assoc_find(engine, hash, item_get_key(it), it->nkey) == 0);  /* shouldn't have duplicately named things defined */

    if (engine->assoc.expanding &&
        (oldbucket = (hash & hashmask(engine->assoc.hashpower - 1))) >= engine->assoc.expand_bucket)
    {
        it->h_next = engine->assoc.old_hashtable[oldbucket];
        engine->assoc.old_hashtable[oldbucket] = it;
    } else {
        it->h_next = engine->assoc.primary_hashtable[hash & hashmask(engine->assoc.hashpower)];
        engine->assoc.primary_hashtable[hash & hashmask(engine->assoc.hashpower)] = it;
    }

    engine->assoc.hash_items++;
    if (! engine->assoc.expanding && engine->assoc.hash_items > (hashsize(engine->assoc.hashpower) * 3) / 2) {
        assoc_expand(engine);
    }

    MEMCACHED_ASSOC_INSERT(item_get_key(it), it->nkey, engine->assoc.hash_items);
    return 1;
}

void assoc_delete(struct default_engine *engine, uint32_t hash, const char *key, const size_t nkey) {
    hash_item **before = _hashitem_before(engine, hash, key, nkey);

    if (*before) {
        hash_item *nxt;
        engine->assoc.hash_items--;
        /* The DTrace probe cannot be triggered as the last instruction
         * due to possible tail-optimization by the compiler
         */
        MEMCACHED_ASSOC_DELETE(key, nkey, engine->assoc.hash_items);
        nxt = (*before)->h_next;
        (*before)->h_next = 0;   /* probably pointless, but whatever. */
        *before = nxt;
        return;
    }
    /* Note:  we never actually get here.  the callers don't delete things
       they can't find. */
    assert(*before != 0);
}


#define DEFAULT_HASH_BULK_MOVE 1
int hash_bulk_move = DEFAULT_HASH_BULK_MOVE;

static void *assoc_maintenance_thread(void *arg) {
    
    struct default_engine *engine = arg;
    bool done = false;
    do {
        int ii;
        pthread_mutex_lock(&engine->cache_lock);

        for (ii = 0; ii < hash_bulk_move && engine->assoc.expanding; ++ii) {
            hash_item *it, *next;
            int bucket;

            for (it = engine->assoc.old_hashtable[engine->assoc.expand_bucket];
                 NULL != it; it = next) {
                next = it->h_next;

                bucket = engine->server.core->hash(item_get_key(it), it->nkey, 0)
                    & hashmask(engine->assoc.hashpower);
                it->h_next = engine->assoc.primary_hashtable[bucket];
                engine->assoc.primary_hashtable[bucket] = it;
            }

            engine->assoc.old_hashtable[engine->assoc.expand_bucket] = NULL;
            engine->assoc.expand_bucket++;
            if (engine->assoc.expand_bucket == hashsize(engine->assoc.hashpower - 1)) {
                engine->assoc.expanding = false;
                free(engine->assoc.old_hashtable);
                if (engine->config.verbose > 1) {
                    EXTENSION_LOGGER_DESCRIPTOR *logger;
                    logger = (void*)engine->server.extension->get_extension(EXTENSION_LOGGER);
                    logger->log(EXTENSION_LOG_INFO, NULL,
                                "Hash table expansion done\n");
                }
            }
        }
        if (!engine->assoc.expanding) {
            done = true;
        }
        pthread_mutex_unlock(&engine->cache_lock);
    } while (!done);

    return NULL;
}

#endif
