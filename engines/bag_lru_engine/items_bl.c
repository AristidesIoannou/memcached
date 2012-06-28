/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <assert.h>
#include <inttypes.h>

#include "default_engine_bl.h"
#define BAG_LRU
#define PHT
/* Forward Declarations */
#if defined(BAG_LRU)
#include <unistd.h>
#include <sched.h>
#include <linux/unistd.h>
#include <sys/syscall.h>

#define LRU_SEARCH_DEPTH 50
#define MAX_BAGS_PER_LRU 500000 //threshold until we start merging bags... this keeps us from having wated empty bags in the end
#define MAX_BAG_ITEMS 1000000
#define MIN_BAG_ITEMS 100000 //ensure that the bags are at least filling a little bit :)
#define LRU_MAINT_INTERVAL 5.0 //how long until we clean again
#define MERGE_COUNT 100000
#define BAG_DEBUG 0

#define hashsize(n) ((uint32_t)1<<(n))
#define hashmask(n) (hashsize(n)-1)

//lock for evictions
static pthread_mutex_t eviction_lock[POWER_LARGEST];
static pthread_mutex_t cleaner_lock;

struct lru_bag{
        struct lru_bag *next_bag; //single linked list always placed at the end when created
        hash_item *newest_item;
        hash_item *oldest_item;
        time_t bag_created;
        time_t bag_closed;
        pthread_mutex_t bag_lock;// WHen the bag was created so we know how long this bag has been open
        int item_count; //we only really care about this
};


struct lru_bag_array_head{
        struct lru_bag *oldest_bag;
        struct lru_bag *newest_alternate; //this is used for moving when it points to the newest bag
        struct lru_bag *newest_bag;
        int num_bags;
};

static struct lru_bag_array_head **lru_bag_lists;


void baglru_flush_expired(struct default_engine *engine, int slabid);
bool try_evict_item(const void *cookie, struct default_engine *engine, int slabid);
void insert_new_item(hash_item *it);
void touch_existing_item(struct default_engine *engine, hash_item *it);
static void create_cleaner(struct default_engine *engine);
void add_new_bag(int lrunumber);
void lru_init(struct default_engine *engine);
void move_item(hash_item *it, struct lru_bag_array_head *lbah);
void clean_bag(struct default_engine *engine, struct lru_bag *lb,struct lru_bag_array_head *lbah, bool scrubber);
void clean_lru_lists(struct default_engine *engine, bool scrubber);
static void *lru_maintenance_loop(void *arg);
static void get_eviction_lock(int slabid);
static void release_eviction_lock(int slabid);
static void init_eviction_locks();
static void init_cleaner_lock();
static void get_cleaner_lock();
static void release_cleaner_lock();
static void lock_bag(struct lru_bag *lb);
static void unlock_bag(struct lru_bag *lb);

static inline bool item_check_and_clear_linked(hash_item* it){
	uint16_t flags = it->iflag;
	while(flags & ITEM_LINKED){
		if(__sync_bool_compare_and_swap(&it->iflag, flags, (flags & ~ITEM_LINKED))){
			return true;
		}
		flags = it->iflag;
	}
	return false;
}

static void get_eviction_lock(int slabid){
        pthread_mutex_lock(&eviction_lock[slabid]);
}

static void release_eviction_lock(int slabid){
        pthread_mutex_unlock(&eviction_lock[slabid]);
}

static void init_eviction_locks(){
        int i;
        for (i = 0; i < POWER_LARGEST; i++){
                pthread_mutex_init(&eviction_lock[i], NULL);
        }
}

static void init_cleaner_lock(){
        pthread_mutex_init(&cleaner_lock, NULL);
}


static void get_cleaner_lock(){
        pthread_mutex_lock(&cleaner_lock);
}

static void release_cleaner_lock(){
        pthread_mutex_unlock(&cleaner_lock);
}

static void lock_bag(struct lru_bag *lb){
        pthread_mutex_lock(&lb->bag_lock);
}

static void unlock_bag(struct lru_bag *lb){
        pthread_mutex_unlock(&lb->bag_lock);
}



#else /* !BAG_LRU */
	static void item_link_q(struct default_engine *engine, hash_item *it);
	static void item_unlink_q(struct default_engine *engine, hash_item *it);

#define hashsize(n) ((uint32_t)1<<(n))
#define hashmask(n) (hashsize(n)-1)

#endif /* end BAG_LRU */

static hash_item *do_item_alloc(struct default_engine *engine,
								const void *key, const size_t nkey,
								const int flags, const rel_time_t exptime,
								const int nbytes,
								const void *cookie);
static hash_item *do_item_get(struct default_engine *engine,
							  const char *key, const size_t nkey);
static hash_item *do_item_get_unlocked(struct default_engine *engine,
							  const char *key, const size_t nkey);
static int do_item_link(struct default_engine *engine, hash_item *it);
static void do_item_unlink(struct default_engine *engine, hash_item *it);
static void do_item_release(struct default_engine *engine, hash_item *it);
static void do_item_update(struct default_engine *engine, hash_item *it);
static int do_item_replace(struct default_engine *engine,
							hash_item *it, hash_item *new_it);
static void item_free(struct default_engine *engine, hash_item *it);

/*
 * We only reposition items in the LRU queue if they haven't been repositioned
 * in this many seconds. That saves us from churning on frequently-accessed
 * items.
 */
#define ITEM_UPDATE_INTERVAL 60
/*
 * To avoid scanning through the complete cache in some circumstances we'll
 * just give up and return an error after inspecting a fixed number of objects.
 */
static const int search_items = 50;

void item_stats_reset(struct default_engine *engine) {
	pthread_mutex_lock(&engine->cache_lock);
	memset(engine->items.itemstats, 0, sizeof(engine->items.itemstats));
	pthread_mutex_unlock(&engine->cache_lock);
}


/* warning: don't use these macros with a function, as it evals its arg twice */
static inline size_t ITEM_ntotal(struct default_engine *engine,
								 const hash_item *item) {
	size_t ret = sizeof(*item) + item->nkey + item->nbytes;
	if (engine->config.use_cas) {
		ret += sizeof(uint64_t);
	}

	return ret;
}

/* Get the next CAS id for a new item. */
static uint64_t get_cas_id(void) {
	static uint64_t cas_id = 0;
	return ++cas_id;
}

/* Enable this for reference-count debugging. */
#if 0
# define DEBUG_REFCNT(it,op) \
				fprintf(stderr, "item %x refcnt(%c) %d %c%c%c\n", \
						it, op, it->refcount, \
						(it->it_flags & ITEM_LINKED) ? 'L' : ' ', \
						(it->it_flags & ITEM_SLABBED) ? 'S' : ' ')
#else
# define DEBUG_REFCNT(it,op) while(0)
#endif


/*@null@*/
hash_item *do_item_alloc(struct default_engine *engine,
						 const void *key,
						 const size_t nkey,
						 const int flags,
						 const rel_time_t exptime,
						 const int nbytes,
						 const void *cookie) {
	hash_item *it = NULL;
	size_t ntotal = sizeof(hash_item) + nkey + nbytes;
	if (engine->config.use_cas) {
		ntotal += sizeof(uint64_t);
	}

	unsigned int id = slabs_clsid(engine, ntotal);
	if (id == 0)
		return 0;

	
#if !defined(BAG_LRU)
	/* do a quick check if we have any expired items in the tail.. */
	int tries = search_items;
	hash_item *search;

	rel_time_t current_time = engine->server.core->get_current_time();

	for (search = engine->items.tails[id];
		 tries > 0 && search != NULL;
		 tries--, search=search->prev) {
		if (search->refcount == 0 &&
			(search->exptime != 0 && search->exptime < current_time)) {
			it = search;
			/* I don't want to actually free the object, just steal
			 * the item to avoid to grab the slab mutex twice ;-)
			 */
			pthread_mutex_lock(&engine->stats.lock);
			engine->stats.reclaimed++;
			pthread_mutex_unlock(&engine->stats.lock);
			engine->items.itemstats[id].reclaimed++;
			it->refcount = 1;
			slabs_adjust_mem_requested(engine, it->slabs_clsid, ITEM_ntotal(engine, it), ntotal);
			do_item_unlink(engine, it);
			/* Initialize the item block: */
			it->slabs_clsid = 0;
			it->refcount = 0;
			break;
		}
	}
#endif /* end !defined(BAG_LRU) */
	if (it == NULL && (it = slabs_alloc(engine, ntotal, id)) == NULL) {

#if defined(BAG_LRU)
		if (engine->config.evict_to_free == 0) {
			engine->items.itemstats[id].outofmemory++;
			return NULL;
		}

		get_eviction_lock(id);
	
		while(try_evict_item(cookie,engine,id) && it==NULL){
			if(it==0) it=slabs_alloc(engine,ntotal,id);
		}
		release_eviction_lock(id);
		
		if (it == 0) {
			return NULL;
		}

#else
		/*
		** Could not find an expired item at the tail, and memory allocation
		** failed. Try to evict some items!
		*/
		tries = search_items;

		/* If requested to not push old items out of cache when memory runs out,
		 * we're out of luck at this point...
		 */

		if (engine->config.evict_to_free == 0) {
			engine->items.itemstats[id].outofmemory++;
			return NULL;
		}

		/*
		 * try to get one off the right LRU
		 * don't necessariuly unlink the tail because it may be locked: refcount>0
		 * search up from tail an item with refcount==0 and unlink it; give up after search_items
		 * tries
		 */

		if (engine->items.tails[id] == 0) {
			engine->items.itemstats[id].outofmemory++;
			return NULL;
		}

		for (search = engine->items.tails[id]; tries > 0 && search != NULL; tries--, search=search->prev) {
			if (search->refcount == 0) {
				if (search->exptime == 0 || search->exptime > current_time) {
					engine->items.itemstats[id].evicted++;
					engine->items.itemstats[id].evicted_time = current_time - search->time;
					if (search->exptime != 0) {
						engine->items.itemstats[id].evicted_nonzero++;
					}
					pthread_mutex_lock(&engine->stats.lock);
					engine->stats.evictions++;
					pthread_mutex_unlock(&engine->stats.lock);
					engine->server.stat->evicting(cookie,
												  item_get_key(search),
												  search->nkey);
				} else {
					engine->items.itemstats[id].reclaimed++;
					pthread_mutex_lock(&engine->stats.lock);
					engine->stats.reclaimed++;
					pthread_mutex_unlock(&engine->stats.lock);
				}
				do_item_unlink(engine, search);
				break;
			}
		}
		it = slabs_alloc(engine, ntotal, id);
		if (it == 0) {
			engine->items.itemstats[id].outofmemory++;
			/* Last ditch effort. There is a very rare bug which causes
			 * refcount leaks. We've fixed most of them, but it still happens,
			 * and it may happen in the future.
			 * We can reasonably assume no item can stay locked for more than
			 * three hours, so if we find one in the tail which is that old,
			 * free it anyway.
			 */
			tries = search_items;
			for (search = engine->items.tails[id]; tries > 0 && search != NULL; tries--, search=search->prev) {
				if (search->refcount != 0 && search->time + TAIL_REPAIR_TIME < current_time) {
					engine->items.itemstats[id].tailrepairs++;
					search->refcount = 0;
					do_item_unlink(engine, search);
					break;
				}
			}
			it = slabs_alloc(engine, ntotal, id);
			if (it == 0) {
				return NULL;
			}
		}
#endif
	}

	assert(it->slabs_clsid == 0);

	it->slabs_clsid = id;

	assert(it != engine->items.heads[it->slabs_clsid]);

	it->next = it->prev = it->h_next = 0;
	it->refcount = 1;     /* the caller will have a reference */
	DEBUG_REFCNT(it, '*');
	it->iflag = engine->config.use_cas ? ITEM_WITH_CAS : 0;
	it->nkey = nkey;
	it->nbytes = nbytes;
	it->flags = flags;
	memcpy((void*)item_get_key(it), key, nkey);
	it->exptime = exptime;
	return it;
}

static void item_free(struct default_engine *engine, hash_item *it) {
	size_t ntotal = ITEM_ntotal(engine, it);
	unsigned int clsid;
	assert((it->iflag & ITEM_LINKED) == 0);
	assert(it != engine->items.heads[it->slabs_clsid]);
	assert(it != engine->items.tails[it->slabs_clsid]);
	//assert(it->refcount == 0);

	/* so slab size changer can tell later if item is already free or not */
	clsid = it->slabs_clsid;
	it->slabs_clsid = 0;
	it->iflag |= ITEM_SLABBED;
	DEBUG_REFCNT(it, 'F');
	slabs_free(engine, it, ntotal, clsid);
}

#if !defined(BAG_LRU)
static void item_link_q(struct default_engine *engine, hash_item *it) { /* item is the new head */
	hash_item **head, **tail;
	assert(it->slabs_clsid < POWER_LARGEST);
	assert((it->iflag & ITEM_SLABBED) == 0);

	head = &engine->items.heads[it->slabs_clsid];
	tail = &engine->items.tails[it->slabs_clsid];
	assert(it != *head);
	assert((*head && *tail) || (*head == 0 && *tail == 0));
	it->prev = 0;
	it->next = *head;
	if (it->next) it->next->prev = it;
	*head = it;
	if (*tail == 0) *tail = it;
	engine->items.sizes[it->slabs_clsid]++;
	return;
}

static void item_unlink_q(struct default_engine *engine, hash_item *it) {
	hash_item **head, **tail;
	assert(it->slabs_clsid < POWER_LARGEST);
	head = &engine->items.heads[it->slabs_clsid];
	tail = &engine->items.tails[it->slabs_clsid];

	if (*head == it) {
		assert(it->prev == 0);
		*head = it->next;
	}
	if (*tail == it) {
		assert(it->next == 0);
		*tail = it->prev;
	}
	assert(it->next != it);
	assert(it->prev != it);

	if (it->next) it->next->prev = it->prev;
	if (it->prev) it->prev->next = it->next;
	engine->items.sizes[it->slabs_clsid]--;
	return;
}
#endif

int do_item_link(struct default_engine *engine, hash_item *it) {
	MEMCACHED_ITEM_LINK(item_get_key(it), it->nkey, it->nbytes);
	assert((it->iflag & (ITEM_LINKED|ITEM_SLABBED)) == 0);
	assert(it->nbytes < (1024 * 1024));  /* 1MB max size */
	it->iflag |= ITEM_LINKED;
	it->time = engine->server.core->get_current_time();
	assoc_insert(engine, engine->server.core->hash(item_get_key(it),
														it->nkey, 0),
				 it);
#if !defined(PHT)
	pthread_mutex_lock(&engine->stats.lock);
#endif
	engine->stats.curr_bytes += ITEM_ntotal(engine, it);
	engine->stats.curr_items += 1;
	engine->stats.total_items += 1;
#if !defined(PHT)
	pthread_mutex_unlock(&engine->stats.lock);
#endif

	/* Allocate a new CAS ID on link. */
	item_set_cas(NULL, NULL, it, get_cas_id());

#if !defined(BAG_LRU)
	item_link_q(engine, it);
#else
	insert_new_item(it);
	assert(it->prev != NULL);
#endif

	return 1;
}

void do_item_unlink(struct default_engine *engine, hash_item *it) {
	MEMCACHED_ITEM_UNLINK(item_get_key(it), it->nkey, it->nbytes);
#if defined(BAG_LRU)
	
	if (item_check_and_clear_linked(it)) {

#else
	if (it->iflag & ITEM_LINKED) {
		it->iflag &= ~ITEM_LINKED;
	
#endif
		pthread_mutex_lock(&engine->stats.lock);
		engine->stats.curr_bytes -= ITEM_ntotal(engine, it);
		engine->stats.curr_items -= 1;
		pthread_mutex_unlock(&engine->stats.lock);
		assoc_delete(engine, engine->server.core->hash(item_get_key(it), it->nkey, 0), item_get_key(it), it->nkey);

/* With bags, let the refcount fall to 0			   * 
 *we will take care of item_free in the cleaner thread */
#if !defined(BAG_LRU)
		item_unlink_q(engine, it);
		if (it->refcount == 0) {
			item_free(engine, it);
		}
#endif
	}
}

void do_item_release(struct default_engine *engine, hash_item *it) {
	MEMCACHED_ITEM_REMOVE(item_get_key(it), it->nkey, it->nbytes);
	unsigned short current_refcount = it->refcount;
	if (current_refcount != 0) {
#if defined(BAG_LRU)
		while (!__sync_bool_compare_and_swap(&it->refcount, current_refcount, current_refcount - 1)){
			current_refcount = it->refcount;
			if(current_refcount == 0){break;}
		}
#else
		it->refcount--;
#endif

		DEBUG_REFCNT(it, '-');
	}
/* we take care of item_free in the cleaner thread */
#if !defined(BAG_LRU)
	if (it->refcount == 0 && (it->iflag & ITEM_LINKED) == 0) {
		item_free(engine, it);
	}
#endif
}

void do_item_update(struct default_engine *engine, hash_item *it) {
	rel_time_t current_time = engine->server.core->get_current_time();
	MEMCACHED_ITEM_UPDATE(item_get_key(it), it->nkey, it->nbytes);
	if (it->time < current_time - ITEM_UPDATE_INTERVAL) {
		assert((it->iflag & ITEM_SLABBED) == 0);

		if ((it->iflag & ITEM_LINKED) != 0) {

#if defined(BAG_LRU)
			touch_existing_item(engine, it);
#else
			item_unlink_q(engine, it);
			it->time = current_time;
			item_link_q(engine, it);
#endif
		}
	}
}

int do_item_replace(struct default_engine *engine,
					hash_item *it, hash_item *new_it) {
	MEMCACHED_ITEM_REPLACE(item_get_key(it), it->nkey, it->nbytes,
						   item_get_key(new_it), new_it->nkey, new_it->nbytes);
	assert((it->iflag & ITEM_SLABBED) == 0);
	do_item_unlink(engine, it);
	return do_item_link(engine, new_it);
}

/*@null@*/
static char *do_item_cachedump(const unsigned int slabs_clsid,
							   const unsigned int limit,
							   unsigned int *bytes) {
#ifdef FUTURE
	unsigned int memlimit = 2 * 1024 * 1024;   /* 2MB max response size */
	char *buffer;
	unsigned int bufcurr;
	hash_item *it;
	unsigned int len;
	unsigned int shown = 0;
	char key_temp[KEY_MAX_LENGTH + 1];
	char temp[512];

	it = engine->items.heads[slabs_clsid];

	buffer = malloc((size_t)memlimit);
	if (buffer == 0) return NULL;
	bufcurr = 0;


	while (it != NULL && (limit == 0 || shown < limit)) {
		assert(it->nkey <= KEY_MAX_LENGTH);
		/* Copy the key since it may not be null-terminated in the struct */
		strncpy(key_temp, item_get_key(it), it->nkey);
		key_temp[it->nkey] = 0x00; /* terminate */
		len = snprintf(temp, sizeof(temp), "ITEM %s [%d b; %lu s]\r\n",
					   key_temp, it->nbytes,
					   (unsigned long)it->exptime + process_started);
		if (bufcurr + len + 6 > memlimit)  /* 6 is END\r\n\0 */
			break;
		memcpy(buffer + bufcurr, temp, len);
		bufcurr += len;
		shown++;
		it = it->next;
	}


	memcpy(buffer + bufcurr, "END\r\n", 6);
	bufcurr += 5;

	*bytes = bufcurr;
	return buffer;
#endif
	(void)slabs_clsid;
	(void)limit;
	(void)bytes;
	return NULL;
}

static void do_item_stats(struct default_engine *engine,
						  ADD_STAT add_stats, const void *c) {
	int i;
#if !defined(BAG_LRU)
	rel_time_t current_time = engine->server.core->get_current_time();
#endif
	for (i = 0; i < POWER_LARGEST; i++) {
#if defined(BAG_LRU)
		//if(lru_bag_lists[i]->oldest_bag->oldest_item != NULL){
                        const char *prefix = "items";
                        add_statistics(c, add_stats, prefix, i, "number", "%u",
                                                   engine->items.sizes[i]);
                  //      add_statistics(c, add_stats, prefix, i, "age", "%u",
                    //                               lru_bag_lists[i]->oldest_bag->oldest_item->time);
                        add_statistics(c, add_stats, prefix, i, "evicted",
                                                   "%u", engine->items.itemstats[i].evicted);
                        add_statistics(c, add_stats, prefix, i, "evicted_nonzero",
                                                   "%u", engine->items.itemstats[i].evicted_nonzero);
                        add_statistics(c, add_stats, prefix, i, "evicted_time",
                                                   "%u", engine->items.itemstats[i].evicted_time);
                        add_statistics(c, add_stats, prefix, i, "outofmemory",
                                                   "%u", engine->items.itemstats[i].outofmemory);
                        add_statistics(c, add_stats, prefix, i, "tailrepairs",
                                                   "%u", engine->items.itemstats[i].tailrepairs);;
                        add_statistics(c, add_stats, prefix, i, "reclaimed",
                                                   "%u", engine->items.itemstats[i].reclaimed);;
		//}
	}
#else
		if (engine->items.tails[i] != NULL) {
			int search = search_items;
			while (search > 0 &&
				   engine->items.tails[i] != NULL &&
				   ((engine->config.oldest_live != 0 && /* Item flushd */
					 engine->config.oldest_live <= current_time &&
					 engine->items.tails[i]->time <= engine->config.oldest_live) ||
					(engine->items.tails[i]->exptime != 0 && /* and not expired */
					 engine->items.tails[i]->exptime < current_time))) {
				--search;
				if (engine->items.tails[i]->refcount == 0) {
					do_item_unlink(engine, engine->items.tails[i]);
				} else {
					break;
				}
			}
			if (engine->items.tails[i] == NULL) {
				/* We removed all of the items in this slab class */
				continue;
			}
			const char *prefix = "items";
			add_statistics(c, add_stats, prefix, i, "number", "%u",
						   engine->items.sizes[i]);
			add_statistics(c, add_stats, prefix, i, "age", "%u",
						   engine->items.tails[i]->time);
			add_statistics(c, add_stats, prefix, i, "evicted",
						   "%u", engine->items.itemstats[i].evicted);
			add_statistics(c, add_stats, prefix, i, "evicted_nonzero",
						   "%u", engine->items.itemstats[i].evicted_nonzero);
			add_statistics(c, add_stats, prefix, i, "evicted_time",
						   "%u", engine->items.itemstats[i].evicted_time);
			add_statistics(c, add_stats, prefix, i, "outofmemory",
						   "%u", engine->items.itemstats[i].outofmemory);
			add_statistics(c, add_stats, prefix, i, "tailrepairs",
						   "%u", engine->items.itemstats[i].tailrepairs);;
			add_statistics(c, add_stats, prefix, i, "reclaimed",
						   "%u", engine->items.itemstats[i].reclaimed);;
		}
	}
#endif
}

/** dumps out a list of objects of each size, with granularity of 32 bytes */
/*@null@*/
static void do_item_stats_sizes(struct default_engine *engine,
								ADD_STAT add_stats, const void *c) {
#if !defined(BAG_LRU)
	/* max 1MB object, divided into 32 bytes size buckets */
	const int num_buckets = 32768;
	unsigned int *histogram = calloc(num_buckets, sizeof(int));

	if (histogram != NULL) {
		int i;

		/* build the histogram */
		for (i = 0; i < POWER_LARGEST; i++) {
			hash_item *iter = engine->items.heads[i];
			while (iter) {
				int ntotal = ITEM_ntotal(engine, iter);
				int bucket = ntotal / 32;
				if ((ntotal % 32) != 0) bucket++;
				if (bucket < num_buckets) histogram[bucket]++;
				iter = iter->next;
			}
		}

		/* write the buffer */
		for (i = 0; i < num_buckets; i++) {
			if (histogram[i] != 0) {
				char key[8], val[32];
				int klen, vlen;
				klen = snprintf(key, sizeof(key), "%d", i * 32);
				vlen = snprintf(val, sizeof(val), "%u", histogram[i]);
				assert(klen < sizeof(key));
				assert(vlen < sizeof(val));
				add_stats(key, klen, val, vlen, c);
			}
		}
		free(histogram);
	}
#else
	/*need to complete this for the bag LRU */
#endif

}

/** wrapper around assoc_find which does the lazy expiration logic */
hash_item *do_item_get(struct default_engine *engine,
					   const char *key, const size_t nkey) {
	rel_time_t current_time = engine->server.core->get_current_time();
	hash_item *it = assoc_find(engine, engine->server.core->hash(key, nkey, 0),key, nkey);
	
	int was_found = 0;

	if (engine->config.verbose > 2) {
		EXTENSION_LOGGER_DESCRIPTOR *logger;
		logger = (void*)engine->server.extension->get_extension(EXTENSION_LOGGER);
		if (it == NULL) {
			logger->log(EXTENSION_LOG_DEBUG, NULL,
						"> NOT FOUND %s", key);
		} else {
			logger->log(EXTENSION_LOG_DEBUG, NULL,
						"> FOUND KEY %s",
						(const char*)item_get_key(it));
			was_found++;
		}
	}

	if (it != NULL && engine->config.oldest_live != 0 &&
		engine->config.oldest_live <= current_time &&
		it->time <= engine->config.oldest_live) {
		do_item_unlink(engine, it);           /* MTSAFE - cache_lock held */
		it = NULL;
	}

	if (it == NULL && was_found) {
		EXTENSION_LOGGER_DESCRIPTOR *logger;
		logger = (void*)engine->server.extension->get_extension(EXTENSION_LOGGER);
		logger->log(EXTENSION_LOG_DEBUG, NULL, " -nuked by flush");
		was_found--;
	}

	if (it != NULL && it->exptime != 0 && it->exptime <= current_time) {
		do_item_unlink(engine, it);           /* MTSAFE - cache_lock held */
		it = NULL;
	}

	if (it == NULL && was_found) {
		EXTENSION_LOGGER_DESCRIPTOR *logger;
		logger = (void*)engine->server.extension->get_extension(EXTENSION_LOGGER);
		logger->log(EXTENSION_LOG_DEBUG, NULL, " -nuked by expire");
		was_found--;
	}

	if (it != NULL) {
		(void)__sync_add_and_fetch(&it->refcount, 1);
		DEBUG_REFCNT(it, '+');
		do_item_update(engine, it);
	}

	return it;
}


/** wrapper around assoc_find which does the lazy expiration logic */
hash_item *do_item_get_unlocked(struct default_engine *engine,
                                           const char *key, const size_t nkey) {
        rel_time_t current_time = engine->server.core->get_current_time();
#if defined(PHT)
        hash_item *it = assoc_find_unlocked(engine, engine->server.core->hash(key, nkey, 0),key, nkey);
#else
        hash_item *it = assoc_find(engine, engine->server.core->hash(key, nkey, 0),key, nkey);
#endif
	int was_found = 0;

        if (engine->config.verbose > 2) {
                EXTENSION_LOGGER_DESCRIPTOR *logger;
                logger = (void*)engine->server.extension->get_extension(EXTENSION_LOGGER);
                if (it == NULL) {
                        logger->log(EXTENSION_LOG_DEBUG, NULL,
                                                "> NOT FOUND %s", key);
                } else {
                        logger->log(EXTENSION_LOG_DEBUG, NULL,
                                                "> FOUND KEY %s",
                                                (const char*)item_get_key(it));
                        was_found++;
                }
        }

        if (it != NULL && engine->config.oldest_live != 0 &&
                engine->config.oldest_live <= current_time &&
                it->time <= engine->config.oldest_live) {
                do_item_unlink(engine, it);           /* MTSAFE - cache_lock held */
                it = NULL;
        }

        if (it == NULL && was_found) {
                EXTENSION_LOGGER_DESCRIPTOR *logger;
                logger = (void*)engine->server.extension->get_extension(EXTENSION_LOGGER);
                logger->log(EXTENSION_LOG_DEBUG, NULL, " -nuked by flush");
                was_found--;
        }

        if (it != NULL && it->exptime != 0 && it->exptime <= current_time) {
                do_item_unlink(engine, it);           /* MTSAFE - cache_lock held */
                it = NULL;
        }

        if (it == NULL && was_found) {
                EXTENSION_LOGGER_DESCRIPTOR *logger;
                logger = (void*)engine->server.extension->get_extension(EXTENSION_LOGGER);
                logger->log(EXTENSION_LOG_DEBUG, NULL, " -nuked by expire");
                was_found--;
        }

        if (it != NULL) {
                (void)__sync_add_and_fetch(&it->refcount, 1);
                DEBUG_REFCNT(it, '+');
                do_item_update(engine, it);
        }

        return it;
}




/*
 * Stores an item in the cache according to the semantics of one of the set
 * commands. In threaded mode, this is protected by the cache lock.
 *
 * Returns the state of storage.
 */
bool bad = false;
static uint32_t bad_hash = 1;
int broken_bucket = 0;
int nkey_bad;
const char *key_bad =  "k-400326";
static ENGINE_ERROR_CODE do_store_item(struct default_engine *engine,
									   hash_item *it, uint64_t *cas,
									   ENGINE_STORE_OPERATION operation,
									   const void *cookie) {
	const char *key = item_get_key(it);
	int missing_item = 0;

	if(bad){
		int bucket =  engine->server.core->hash(key, it->nkey, 0) & hashmask(engine->assoc.hashpower);
		if(broken_bucket ==  bucket){
//			fprintf(stderr, " matching hash\n key %s \n", (char *)key );
			hash_item *found = do_item_get(engine, key_bad, nkey_bad);
                	if(found){
  //      	                fprintf(stderr, "found in hash table\n");
	                }
		}
	}

	if(strncmp(key, "k-400326", 8) == 0){
		bad_hash = engine->server.core->hash(key, it->nkey, 0);
		nkey_bad = it->nkey;
		broken_bucket = bad_hash & hashmask(engine->assoc.hashpower);
//		fprintf(stderr, "found it!\n");
		missing_item = 1;
		bad = true;
	}
	hash_item *old_it = do_item_get(engine, key, it->nkey);
	
	ENGINE_ERROR_CODE stored = ENGINE_NOT_STORED;

	hash_item *new_it = NULL;
	if(old_it){
		//fprintf(stderr, "tried inserting %s, but found %s\n", (char *)key, (char *)item_get_key(old_it));
	}
	if (old_it != NULL && operation == OPERATION_ADD) {
		/* add only adds a nonexistent item, but promote to head of LRU */
		do_item_update(engine, old_it);
	} else if (!old_it && (operation == OPERATION_REPLACE
		|| operation == OPERATION_APPEND || operation == OPERATION_PREPEND))
	{
		/* replace only replaces an existing value; don't store */
	} else if (operation == OPERATION_CAS) {
		/* validate cas operation */
		if(old_it == NULL) {
			// LRU expired
			stored = ENGINE_KEY_ENOENT;
		}
		else if (item_get_cas(it) == item_get_cas(old_it)) {
			// cas validates
			// it and old_it may belong to different classes.
			// I'm updating the stats for the one that's getting pushed out
			do_item_replace(engine, old_it, it);
			stored = ENGINE_SUCCESS;
		} else {
			if (engine->config.verbose > 1) {
				EXTENSION_LOGGER_DESCRIPTOR *logger;
				logger = (void*)engine->server.extension->get_extension(EXTENSION_LOGGER);
				logger->log(EXTENSION_LOG_INFO, NULL,
						"CAS:  failure: expected %"PRIu64", got %"PRIu64"\n",
						item_get_cas(old_it),
						item_get_cas(it));
			}
			stored = ENGINE_KEY_EEXISTS;
		}
	} else {
		/*
		 * Append - combine new and old record into single one. Here it's
		 * atomic and thread-safe.
		 */
		if (operation == OPERATION_APPEND || operation == OPERATION_PREPEND) {
			/*
			 * Validate CAS
			 */
			if (item_get_cas(it) != 0) {
				// CAS much be equal
				if (item_get_cas(it) != item_get_cas(old_it)) {
					stored = ENGINE_KEY_EEXISTS;
				}
			}

			if (stored == ENGINE_NOT_STORED) {
				/* we have it and old_it here - alloc memory to hold both */
				new_it = do_item_alloc(engine, key, it->nkey,
									   old_it->flags,
									   old_it->exptime,
									   it->nbytes + old_it->nbytes,
									   cookie);

				if (new_it == NULL) {
					/* SERVER_ERROR out of memory */
					if (old_it != NULL) {
						do_item_release(engine, old_it);
					}

					return ENGINE_NOT_STORED;
				}

				/* copy data from it and old_it to new_it */

				if (operation == OPERATION_APPEND) {
					memcpy(item_get_data(new_it), item_get_data(old_it), old_it->nbytes);
					memcpy(item_get_data(new_it) + old_it->nbytes, item_get_data(it), it->nbytes);
				} else {
					/* OPERATION_PREPEND */
					memcpy(item_get_data(new_it), item_get_data(it), it->nbytes);
					memcpy(item_get_data(new_it) + it->nbytes, item_get_data(old_it), old_it->nbytes);
				}

				it = new_it;
			}
		}

		if (stored == ENGINE_NOT_STORED) {
			if (old_it != NULL) {

		do_item_replace(engine, old_it, it);


 	if(bad){
                int bucket =  engine->server.core->hash(key, it->nkey, 0) & hashmask(engine->assoc.hashpower);
                if(broken_bucket ==  bucket){
                        fprintf(stderr, " matching hash\n key %s \n", (char *)key );
                        hash_item *found = do_item_get(engine, key_bad,nkey_bad);
                        if(found){
                                fprintf(stderr, "after replace\n");
                        }
                }
        }



			} else {
				do_item_link(engine, it);
			}

			*cas = item_get_cas(it);
			stored = ENGINE_SUCCESS;
		}
	}
/*
 	if(bad){
                int bucket =  engine->server.core->hash(key, it->nkey, 0) & hashmask(engine->assoc.hashpower);
                if(broken_bucket ==  bucket){
 			fprintf(stderr, " matching hash\n key %s \n", (char *)key );
			hash_item *found = do_item_get(engine, key_bad,nkey_bad);
                	if(found){
                        	fprintf(stderr, "found in hash table - 2\n");
                	}
        	}
        }

*/

	if (old_it != NULL) {
		do_item_release(engine, old_it);         /* release our reference */
	}

	if (new_it != NULL) {
		do_item_release(engine, new_it);
	}

	if (stored == ENGINE_SUCCESS) {
		*cas = item_get_cas(it);
	}
	return stored;
}


/*
 * adds a delta value to a numeric item.
 *
 * c     connection requesting the operation
 * it    item to adjust
 * incr  true to increment value, false to decrement
 * delta amount to adjust value by
 * buf   buffer for response string
 *
 * returns a response string to send back to the client.
 */
static ENGINE_ERROR_CODE do_add_delta(struct default_engine *engine,
									  hash_item *it, const bool incr,
									  const int64_t delta, uint64_t *rcas,
									  uint64_t *result, const void *cookie) {
	const char *ptr;
	uint64_t value;
	char buf[80];
	int res;

	if (it->nbytes >= (sizeof(buf) - 1)) {
		return ENGINE_EINVAL;
	}

	ptr = item_get_data(it);
	memcpy(buf, ptr, it->nbytes);
	buf[it->nbytes] = '\0';

	if (!safe_strtoull(buf, &value)) {
		return ENGINE_EINVAL;
	}

	if (incr) {
		value += delta;
	} else {
		if(delta > value) {
			value = 0;
		} else {
			value -= delta;
		}
	}

	*result = value;
	if ((res = snprintf(buf, sizeof(buf), "%" PRIu64, value)) == -1) {
		return ENGINE_EINVAL;
	}

	if (it->refcount == 1 && res <= it->nbytes) {
		// we can do inline replacement
		memcpy(item_get_data(it), buf, res);
		memset(item_get_data(it) + res, ' ', it->nbytes - res);
		item_set_cas(NULL, NULL, it, get_cas_id());
		*rcas = item_get_cas(it);
	} else {
		hash_item *new_it = do_item_alloc(engine, item_get_key(it),
										  it->nkey, it->flags,
										  it->exptime, res,
										  cookie);
		if (new_it == NULL) {
			do_item_unlink(engine, it);
			return ENGINE_ENOMEM;
		}
		memcpy(item_get_data(new_it), buf, res);
		do_item_replace(engine, it, new_it);
		*rcas = item_get_cas(new_it);
		do_item_release(engine, new_it);       /* release our reference */
	}

	return ENGINE_SUCCESS;
}

/********************************* ITEM ACCESS *******************************/

/*
 * Allocates a new item.
 */
hash_item *item_alloc(struct default_engine *engine,
					  const void *key, size_t nkey, int flags,
					  rel_time_t exptime, int nbytes, const void *cookie) {
	hash_item *it;
#if !defined(PHT)
	pthread_mutex_lock(&engine->cache_lock);
	it = do_item_alloc(engine, key, nkey, flags, exptime, nbytes, cookie);
	pthread_mutex_unlock(&engine->cache_lock);
#else
	it = do_item_alloc(engine, key, nkey, flags, exptime, nbytes, cookie);
#endif
	return it;
}

/*
 * Returns an item if it hasn't been marked as expired,
 * lazy-expiring as needed.
 */
hash_item *item_get(struct default_engine *engine,
					const void *key, const size_t nkey) {
	hash_item *it;
#if !defined(PHT)
	pthread_mutex_lock(&engine->cache_lock);
	it = do_item_get(engine, key, nkey);
	pthread_mutex_unlock(&engine->cache_lock);
#else
	it = do_item_get(engine, key, nkey);
#endif
	return it;
}


/*
 *  * Returns an item if it hasn't been marked as expired,
 *   * lazy-expiring as needed.
 *    */
hash_item *item_get_unlocked(struct default_engine *engine,
                                        const void *key, const size_t nkey) {
        hash_item *it;
#if !defined(PHT)
        pthread_mutex_lock(&engine->cache_lock);
        it = do_item_get_unlocked(engine, key, nkey);
        pthread_mutex_unlock(&engine->cache_lock);
#else
        it = do_item_get_unlocked(engine, key, nkey);
#endif
        return it;
}
       




/*
 * Decrements the reference count on an item and adds it to the freelist if
 * needed.
 */
void item_release(struct default_engine *engine, hash_item *item) {
#if !defined(PHT)
	pthread_mutex_lock(&engine->cache_lock);
	do_item_release(engine, item);
	pthread_mutex_unlock(&engine->cache_lock);
#else
	do_item_release(engine, item);
#endif
}

/*
 * Unlinks an item from the LRU and hashtable.
 */
void item_unlink(struct default_engine *engine, hash_item *item) {
#if !defined(PHT)
	pthread_mutex_lock(&engine->cache_lock);
	do_item_unlink(engine, item);
	pthread_mutex_unlock(&engine->cache_lock);
#else
	do_item_unlink(engine, item);
#endif
}

static ENGINE_ERROR_CODE do_arithmetic(struct default_engine *engine,
									   const void* cookie,
									   const void* key,
									   const int nkey,
									   const bool increment,
									   const bool create,
									   const uint64_t delta,
									   const uint64_t initial,
									   const rel_time_t exptime,
									   uint64_t *cas,
									   uint64_t *result)
{
   hash_item *item = do_item_get(engine, key, nkey);
   ENGINE_ERROR_CODE ret;

   if (item == NULL) {
	  if (!create) {
		 return ENGINE_KEY_ENOENT;
	  } else {
		 char buffer[128];
		 int len = snprintf(buffer, sizeof(buffer), "%"PRIu64,
							(uint64_t)initial);

		 item = do_item_alloc(engine, key, nkey, 0, exptime, len, cookie);
		 if (item == NULL) {
			return ENGINE_ENOMEM;
		 }
		 memcpy((void*)item_get_data(item), buffer, len);
		 if ((ret = do_store_item(engine, item, cas,
								  OPERATION_ADD, cookie)) == ENGINE_SUCCESS) {
			 *result = initial;
			 *cas = item_get_cas(item);
		 }
		 do_item_release(engine, item);
	  }
   } else {
	  ret = do_add_delta(engine, item, increment, delta, cas, result, cookie);
	  do_item_release(engine, item);
   }

   return ret;
}

ENGINE_ERROR_CODE arithmetic(struct default_engine *engine,
							 const void* cookie,
							 const void* key,
							 const int nkey,
							 const bool increment,
							 const bool create,
							 const uint64_t delta,
							 const uint64_t initial,
							 const rel_time_t exptime,
							 uint64_t *cas,
							 uint64_t *result)
{
	ENGINE_ERROR_CODE ret;

	pthread_mutex_lock(&engine->cache_lock);
	ret = do_arithmetic(engine, cookie, key, nkey, increment,
						create, delta, initial, exptime, cas,
						result);
	pthread_mutex_unlock(&engine->cache_lock);
	return ret;
}

/*
 * Stores an item in the cache (high level, obeys set/add/replace semantics)
 */
ENGINE_ERROR_CODE store_item(struct default_engine *engine,
							 hash_item *item, uint64_t *cas,
							 ENGINE_STORE_OPERATION operation,
							 const void *cookie) {
	ENGINE_ERROR_CODE ret;

	pthread_mutex_lock(&engine->cache_lock);
	ret = do_store_item(engine, item, cas, operation, cookie);
	pthread_mutex_unlock(&engine->cache_lock);
	return ret;
}

static hash_item *do_touch_item(struct default_engine *engine,
									 const void *key,
									 uint16_t nkey,
									 uint32_t exptime)
{
   hash_item *item = do_item_get(engine, key, nkey);
   if (item != NULL) {
	   item->exptime = exptime;
   }
   return item;
}

hash_item *touch_item(struct default_engine *engine,
						   const void *key,
						   uint16_t nkey,
						   uint32_t exptime)
{
	hash_item *ret;

	pthread_mutex_lock(&engine->cache_lock);
	ret = do_touch_item(engine, key, nkey, exptime);
	pthread_mutex_unlock(&engine->cache_lock);
	return ret;
}

/*
 * Flushes expired items after a flush_all call
 */
void item_flush_expired(struct default_engine *engine, time_t when) {
	int i;
#if !defined(BAG_LRU)
	hash_item *iter, *next;
#endif
	pthread_mutex_lock(&engine->cache_lock);

	if (when == 0) {
		engine->config.oldest_live = engine->server.core->get_current_time() - 1;
	} else {
		engine->config.oldest_live = engine->server.core->realtime(when) - 1;
	}

	if (engine->config.oldest_live != 0) {
		for (i = 0; i < POWER_LARGEST; i++) {

#if defined(BAG_LRU)
	baglru_flush_expired(engine, i);
#else


			/*
			 * The LRU is sorted in decreasing time order, and an item's
			 * timestamp is never newer than its last access time, so we
			 * only need to walk back until we hit an item older than the
			 * oldest_live time.
			 * The oldest_live checking will auto-expire the remaining items.
			 */
			for (iter = engine->items.heads[i]; iter != NULL; iter = next) {
				if (iter->time >= engine->config.oldest_live) {
					next = iter->next;
					if ((iter->iflag & ITEM_SLABBED) == 0) {
						do_item_unlink(engine, iter);
					}
				} else {
					/* We've hit the first old item. Continue to the next queue. */
					break;
				}
			}
#endif
		}
	}
	pthread_mutex_unlock(&engine->cache_lock);
}

/*
 * Dumps part of the cache
 */
char *item_cachedump(struct default_engine *engine,
					 unsigned int slabs_clsid,
					 unsigned int limit,
					 unsigned int *bytes) {
	char *ret;

	pthread_mutex_lock(&engine->cache_lock);
	ret = do_item_cachedump(slabs_clsid, limit, bytes);
	pthread_mutex_unlock(&engine->cache_lock);
	return ret;
}

void item_stats(struct default_engine *engine,
				   ADD_STAT add_stat, const void *cookie)
{
	pthread_mutex_lock(&engine->cache_lock);
	do_item_stats(engine, add_stat, cookie);
	pthread_mutex_unlock(&engine->cache_lock);
}


void item_stats_sizes(struct default_engine *engine,
					  ADD_STAT add_stat, const void *cookie)
{
	pthread_mutex_lock(&engine->cache_lock);
	do_item_stats_sizes(engine, add_stat, cookie);
	pthread_mutex_unlock(&engine->cache_lock);
}

static void do_item_link_cursor(struct default_engine *engine,
								hash_item *cursor, int ii)
{
	cursor->slabs_clsid = (uint8_t)ii;
	cursor->next = NULL;
	cursor->prev = engine->items.tails[ii];
	engine->items.tails[ii]->next = cursor;
	engine->items.tails[ii] = cursor;
	engine->items.sizes[ii]++;
}

typedef ENGINE_ERROR_CODE (*ITERFUNC)(struct default_engine *engine,
									  hash_item *item, void *cookie);

static bool do_item_walk_cursor(struct default_engine *engine,
								hash_item *cursor,
								int steplength,
								ITERFUNC itemfunc,
								void* itemdata,
								ENGINE_ERROR_CODE *error)
{
	int ii = 0;
	*error = ENGINE_SUCCESS;

	while (cursor->prev != NULL && ii < steplength) {
		++ii;
		/* Move cursor */
		hash_item *ptr = cursor->prev;
		//item_unlink_q(engine, cursor);

		bool done = false;
		if (ptr == engine->items.heads[cursor->slabs_clsid]) {
			done = true;
			cursor->prev = NULL;
		} else {
			cursor->next = ptr;
			cursor->prev = ptr->prev;
			cursor->prev->next = cursor;
			ptr->prev = cursor;
		}

		/* Ignore cursors */
		if (ptr->nkey == 0 && ptr->nbytes == 0) {
			--ii;
		} else {
			*error = itemfunc(engine, ptr, itemdata);
			if (*error != ENGINE_SUCCESS) {
				return false;
			}
		}

		if (done) {
			return false;
		}
	}

	return (cursor->prev != NULL);
}

#if defined(BAG_LRU)

static void item_scrub_classes(struct default_engine *engine) {
	get_cleaner_lock();
	clean_lru_lists(engine, true);
	release_cleaner_lock();
}

#else
static ENGINE_ERROR_CODE item_scrub(struct default_engine *engine,
                                                                        hash_item *item,
                                                                        void *cookie) {
        (void)cookie;
        engine->scrubber.visited++;
        rel_time_t current_time = engine->server.core->get_current_time();
        if (item->refcount == 0 &&
                (item->exptime != 0 && item->exptime < current_time)) {
                do_item_unlink(engine, item);
                engine->scrubber.cleaned++;
        }
        return ENGINE_SUCCESS;
}


static void item_scrub_class(struct default_engine *engine,
							 hash_item *cursor) {

	ENGINE_ERROR_CODE ret;
	bool more;
	do {
		pthread_mutex_lock(&engine->cache_lock);
		more = do_item_walk_cursor(engine, cursor, 200, item_scrub, NULL, &ret);
		pthread_mutex_unlock(&engine->cache_lock);
		if (ret != ENGINE_SUCCESS) {
			break;
		}
	} while (more);
}
#endif

static void *item_scubber_main(void *arg)
{
	struct default_engine *engine = arg;

#if defined(BAG_LRU)
       	item_scrub_classes(engine);
#else
	hash_item cursor = { .refcount = 1 };
	for (int ii = 0; ii < POWER_LARGEST; ++ii) {
                pthread_mutex_lock(&engine->cache_lock);
                bool skip = false;

		if (engine->items.heads[ii] == NULL) {
		bool skip = false;
			skip = true;
		} else {
			// add the item at the tail
			do_item_link_cursor(engine, &cursor, ii);
		}
		pthread_mutex_unlock(&engine->cache_lock);
		if (!skip) {
			item_scrub_class(engine, &cursor);
		}
	}
#endif
	
	pthread_mutex_lock(&engine->scrubber.lock);
	engine->scrubber.stopped = time(NULL);
	engine->scrubber.running = false;
	pthread_mutex_unlock(&engine->scrubber.lock);

	return NULL;
}

bool item_start_scrub(struct default_engine *engine)
{
	bool ret = false;
	pthread_mutex_lock(&engine->scrubber.lock);
	if (!engine->scrubber.running) {
		engine->scrubber.started = time(NULL);
		engine->scrubber.stopped = 0;
		engine->scrubber.visited = 0;
		engine->scrubber.cleaned = 0;
		engine->scrubber.running = true;

		pthread_t t;
		pthread_attr_t attr;

		if (pthread_attr_init(&attr) != 0 ||
			pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED) != 0 ||
			pthread_create(&t, &attr, item_scubber_main, engine) != 0)
		{
			engine->scrubber.running = false;
		} else {
			ret = true;
		}
	}
	pthread_mutex_unlock(&engine->scrubber.lock);

	return ret;
}

struct tap_client {
	hash_item cursor;
	hash_item *it;
};

static ENGINE_ERROR_CODE item_tap_iterfunc(struct default_engine *engine,
									hash_item *item,
									void *cookie) {
	struct tap_client *client = cookie;
	client->it = item;
	(void)__sync_add_and_fetch(&client->it->refcount,1);
	return ENGINE_SUCCESS;
}

static tap_event_t do_item_tap_walker(struct default_engine *engine,
										 const void *cookie, item **itm,
										 void **es, uint16_t *nes, uint8_t *ttl,
										 uint16_t *flags, uint32_t *seqno,
										 uint16_t *vbucket)
{
	struct tap_client *client = engine->server.cookie->get_engine_specific(cookie);
	if (client == NULL) {
		return TAP_DISCONNECT;
	}

	*es = NULL;
	*nes = 0;
	*ttl = (uint8_t)-1;
	*seqno = 0;
	*flags = 0;
	*vbucket = 0;
	client->it = NULL;

	ENGINE_ERROR_CODE r;
	do {
		if (!do_item_walk_cursor(engine, &client->cursor, 1, item_tap_iterfunc, client, &r)) {
			// find next slab class to look at..
			bool linked = false;
			for (int ii = client->cursor.slabs_clsid + 1; ii < POWER_LARGEST && !linked;  ++ii) {
				if (engine->items.heads[ii] != NULL) {
					// add the item at the tail
					do_item_link_cursor(engine, &client->cursor, ii);
					linked = true;
				}
			}
			if (!linked) {
				break;
			}
		}
	} while (client->it == NULL);
	*itm = client->it;

	return (*itm == NULL) ? TAP_DISCONNECT : TAP_MUTATION;
}

tap_event_t item_tap_walker(ENGINE_HANDLE* handle,
							const void *cookie, item **itm,
							void **es, uint16_t *nes, uint8_t *ttl,
							uint16_t *flags, uint32_t *seqno,
							uint16_t *vbucket)
{
	tap_event_t ret;
	struct default_engine *engine = (struct default_engine*)handle;
	pthread_mutex_lock(&engine->cache_lock);
	ret = do_item_tap_walker(engine, cookie, itm, es, nes, ttl, flags, seqno, vbucket);
	pthread_mutex_unlock(&engine->cache_lock);

	return ret;
}

bool initialize_item_tap_walker(struct default_engine *engine,
								const void* cookie)
{
	struct tap_client *client = calloc(1, sizeof(*client));
	if (client == NULL) {
		return false;
	}
	client->cursor.refcount = 1;

	/* Link the cursor! */
	bool linked = false;
	for (int ii = 0; ii < POWER_LARGEST && !linked; ++ii) {
		pthread_mutex_lock(&engine->cache_lock);
		if (engine->items.heads[ii] != NULL) {
			// add the item at the tail
			do_item_link_cursor(engine, &client->cursor, ii);
			linked = true;
		}
		pthread_mutex_unlock(&engine->cache_lock);
	}

	engine->server.cookie->store_engine_specific(cookie, client);
	return true;
}

#if defined(BAG_LRU)
/* used to remove items from localhost "FLUSH" command */
void baglru_flush_expired(struct default_engine *engine, int slabid){
	/*need to lock out the cleaner thread and
	lock out the eviction threads to ensure safety */
	get_cleaner_lock();
	get_eviction_lock(slabid);
	add_new_bag(slabid);
	add_new_bag(slabid);
	assert(slabid <= POWER_LARGEST);
	hash_item *search, *prev, *next;
	struct lru_bag *lb = lru_bag_lists[slabid]->oldest_bag;

	while(lb != lru_bag_lists[slabid]->newest_bag && lb->next_bag && lb){
		search = lb->oldest_item;
		prev = NULL;
		while(search != NULL){
			next = search->next;
			if (search->time >= engine->config.oldest_live) {
				if ((search->iflag & ITEM_SLABBED) == 0){
					do_item_unlink(engine, search);
					if(prev == NULL && lb->oldest_item == search){
						lb->oldest_item = search->next;
					}else if(prev != NULL){
						prev->next = search->next;
					}
					lb->item_count--;
					item_free(engine, search);
				}else{
					prev = search;
				}
			}else{
				prev = search;
			}
			search = next;
		}
		lb = lb->next_bag;
	}
	release_eviction_lock(slabid);
	release_cleaner_lock();
}

/* this allows worker threads to evict items from the last bag(s)  *
 * must hold eviction lock before calling this function for safety */
bool try_evict_item(const void *cookie, struct default_engine *engine, int slabid){
	//fprintf(stderr, "evicitng something!");
	if(slabid >= POWER_LARGEST){ return false; }
	//stats_t *stats = STATS_GET_TLS();
	rel_time_t now = engine->server.core->get_current_time();
	hash_item *search, *prev;
	struct lru_bag *lb = lru_bag_lists[slabid]->oldest_bag;

	
	while(lb && lb->item_count == 0 && (lb->next_bag != lru_bag_lists[slabid]->newest_alternate || lb->next_bag != lru_bag_lists[slabid]->newest_bag) ){
		lb = lb->next_bag;
	}


	if(lb && ((lb == lru_bag_lists[slabid]->newest_alternate || lb->next_bag == lru_bag_lists[slabid]->newest_bag) || lb->item_count == 0)){
		get_cleaner_lock();
		add_new_bag(slabid);
		add_new_bag(slabid);
		release_cleaner_lock();
		while(lb->item_count == 0 ){
			lb = lb->next_bag;
			if(!lb || lb ==  lru_bag_lists[slabid]->newest_alternate){return false;}
		}
	}

	if(!lb){
		return false;
	}
	lock_bag(lb); /* lock the bag we will be evicting from - this locks out the cleaner thread */
	search = lb->oldest_item;
	prev = NULL;
	int count = 1;
   	while(count < search_items && search){
		
	   if (search->refcount == 0 || (search->refcount != 0 && search->prev == (hash_item *)lb ) ){
/* boring stats stuff */
			if (search->exptime == 0 || search->exptime > now) {
				engine->items.itemstats[slabid].evicted++;
				engine->items.itemstats[slabid].evicted_time = now - search->time;
				if (search->exptime != 0) {
					engine->items.itemstats[slabid].evicted_nonzero++;
				}
				pthread_mutex_lock(&engine->stats.lock);
				engine->stats.evictions++;
				pthread_mutex_unlock(&engine->stats.lock);
				engine->server.stat->evicting(cookie,
												item_get_key(search),
												search->nkey);
			} else {
				engine->items.itemstats[slabid].reclaimed++;
				pthread_mutex_lock(&engine->stats.lock);
				engine->stats.reclaimed++;
				pthread_mutex_unlock(&engine->stats.lock);
			}
			if(search->refcount != 0){
				engine->items.itemstats[slabid].tailrepairs++;
				search->refcount = 0;
			}
/*end of boring stats stuff */
/*eviction magic */
			do_item_unlink(engine, search);
			if(prev == NULL && lb->oldest_item == search){
				if(search == lb->newest_item){ lb->newest_item = NULL;}
				lb->oldest_item = search->next;
			}else if(prev != NULL){
				if(search == lb->newest_item){ lb->newest_item = prev;}
				prev->next = search->next;
			}
			lb->item_count--;
			item_free(engine, search);
			unlock_bag(lb);
			return true;
/*end of eviction magic*/
		}
		prev = search;
		search = search->next;
		count++;
	}
#if BAG_DEBUG
	fprintf(stderr, "failed - numItems = %d; num tries = %d\n", lb->item_count, count);
	fprintf(stderr, "         oldest_item = %p; search = %p\n",  (void *)lb->oldest_item, (void *) search);
	fprintf(stderr, "         newest Item = %p; bag = %p\n",  (void *)lb->newest_item, (void *)lb);
	fprintf(stderr, "         Oldest Bag = %p; Next = %p\n",  (void *)lru_bag_lists[slabid]->oldest_bag,  (void *)lru_bag_lists[slabid]->oldest_bag->next_bag);
	fprintf(stderr, "         number of bags = %d\n", lru_bag_lists[slabid]->num_bags);
#endif
	unlock_bag(lb);
	return false;
}

/* places a newly alloced item into the correct bag *
 * atomic operations - Thread Safe                  */
void insert_new_item(hash_item *it){
	//assert(ITEM_is_valid(it));
	//assert(!ITEM_is_transient(it));
	//assert(ITEM_refcount(it) >= 2);
	//assert(ITEM_is_linked(it));
	it->next = NULL;
	struct lru_bag *lb = lru_bag_lists[it->slabs_clsid]->newest_bag;
		
	/*insert the first item into the bag (or at least try to) */
	if(lb->newest_item == it){
		return;
	}
	if(lb->newest_item == NULL){
		if(__sync_bool_compare_and_swap(&(lb->newest_item), NULL, it)){
			it->prev = (hash_item *)lb;
			lb->oldest_item = it;
			(void)__sync_add_and_fetch(&lb->item_count,1);
			return;
		}
	}

	hash_item *swap_item =  lb->newest_item;
	while(!__sync_bool_compare_and_swap(&(swap_item->next), NULL, it)){
		while(swap_item->next != NULL){
			if(swap_item == swap_item->next){sleep(10);}
			swap_item = swap_item->next;
		}
	}
	lb->newest_item = it;
	it->prev = (item *)lb;
	(void)__sync_add_and_fetch(&lb->item_count,1);

}

/* updates the timestamp on an item, and makes it point to the correct bag *
 * thread safety can be ignored a race with the timestamp or a race        *
 * with the bag stamp will put it at the almost identical time/bag         */
void touch_existing_item(struct default_engine *engine, hash_item *it){
	
	it->prev = (hash_item *) lru_bag_lists[it->slabs_clsid]->newest_bag;
	it->time = engine->server.core->get_current_time();
}


/* initializes the thread that goes around cleaning bags *
 * could be expanded to have one per slabid              */
static void create_cleaner(struct default_engine *engine){
	pthread_t       thread;
	pthread_attr_t  attr;
	int             ret;

	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
	if ((ret = pthread_create(&thread, &attr, lru_maintenance_loop, engine)) != 0) {
		fprintf(stderr, "Can't create thread: %s\n",
			strerror(ret));
		exit(1);
	}
}

/* sets up the LRU array and initializes * 
 * the first set of bags to be used,     *
 * as well as the recovery bag           */
void lru_init(struct default_engine *engine){
	init_eviction_locks();
	init_cleaner_lock();
	
	struct lru_bag *lb;
	struct lru_bag_array_head *lbah;

	lru_bag_lists =(struct lru_bag_array_head **) malloc( sizeof(struct lru_bag_array_head *) * POWER_LARGEST);

	/* init first bag for each of the LRU Bag Lists sizes */
	int i;
	for( i = 0; i < POWER_LARGEST; i++){

		lru_bag_lists[i] = malloc(sizeof(struct lru_bag_array_head));
		lb = (struct lru_bag *) malloc( sizeof(struct lru_bag));
		lb->next_bag = 0;
		lb->item_count = 0;
		lb->newest_item = 0;
		lb->oldest_item = 0;
		time(&lb->bag_created);
		pthread_mutex_init(&lb->bag_lock, NULL);

		lbah = lru_bag_lists[i];
		lbah->newest_bag = lb;
		lbah->oldest_bag = lb;
		lbah->newest_alternate = 0; 
		lbah->num_bags = 1;
	}
	create_cleaner(engine);
}

/* creates a new bag and adds it to the head  *
 * of the bag list with teh given slab_id     */ 
void add_new_bag(int lrunumber){
	struct lru_bag *holder;
	struct lru_bag *lb = (struct lru_bag *) malloc(sizeof(struct lru_bag));
	lb->item_count = 0;
	lb->next_bag = NULL;
	lb->newest_item = NULL;
	lb->oldest_item = NULL;
	time(&lb->bag_created);
	pthread_mutex_init(&lb->bag_lock, NULL);

	/* <insert here> <---- newest <----.....<-------oldest    *
	 * linking this way will make removing the tail bag       *
	 * easier without needing a double linked list            */
	struct lru_bag_array_head *lbah = lru_bag_lists[lrunumber];

	lbah->newest_bag->next_bag = lb;
	time(&lbah->newest_bag->bag_closed);
	holder = lbah->newest_bag;
	lbah->newest_bag = lb;
	lbah->newest_alternate = holder;

	(void)__sync_add_and_fetch(&lbah->num_bags,1);
}

/* checks all the head bags, and closes them as needed  *
* based on the defined criteria at the top of this file */
static void check_lru_current_bags(){
	struct lru_bag *lb;
	int i;
	for(i = 0; i < POWER_LARGEST; i++){
		lb = lru_bag_lists[i]->newest_bag;
		/* this effectively closes the current bag once all the threads have cleared out */
		if( lb->item_count > MAX_BAG_ITEMS){
			add_new_bag(i);
			time(&lb->bag_closed);
		}
	}
}

/* Call this when moving an item to the new pointer   *
 * wont evict expired items                           *
 * NOT THREAD SAFE - must hold lock                   */
void move_item(hash_item *it, struct lru_bag_array_head *lbah){
	struct lru_bag *lb = (struct lru_bag *)(it->prev);

	if(lb == lbah->newest_bag){
		lb = lbah->newest_alternate;
	}

	it->next = lb->oldest_item;
	lb->oldest_item = it;
	
	lb->item_count++; /* VERY not thread safe, should be holding the lock */
}

/* determines if an item is unlinked and has no references        *
 * could expand further for having the cleaner thread evict items */
static int item_invalid(hash_item *it){
	if(it->refcount == 0 && (it->iflag & (ITEM_LINKED)) == 0){
		return 1;
	}
	return 0;
}

static int item_expired(hash_item *it,rel_time_t current_time){
	
	if (it->refcount == 0 && (it->exptime != 0 && it->exptime < current_time)) {
		return 1;
	}
	return 0;

}

/* traverses the passed bag, moving items to correct bags */
void clean_bag(struct default_engine *engine, struct lru_bag *lb,struct lru_bag_array_head *lbah, bool scrubber){
	
	if(lb == NULL || lb->oldest_item == NULL){
		return;
	}
       
        rel_time_t current_time = engine->server.core->get_current_time();
	bool on_first_item = true;  /* flag for tracking if we are the head object*/
	hash_item *it, *previtem, *nextitem;
	it = lb->oldest_item;
	previtem = 0;

	int total = lb->item_count;
	int count = 0;
	while (it != NULL){
		nextitem = it->next;
		/* three things can happen now                                             *
		 * 1 - its expired/deleted and unlinked, remove it                         *
		 * 2 - it isnt expired/delete and it belongs in a different bag move it    *
		 * 3 - it isnt expired/deleted and it belongs in this bag - leave it alone */
		if(item_invalid(it)){
			/* item is unlinked remove it and free it*/
			if(on_first_item){
				lb->oldest_item = nextitem;
			}else{
				previtem->next = nextitem;
			}

			it->next = NULL;
			it->prev = NULL;
			item_free(engine, it);

			lb->item_count--;
		}else if(item_expired(it, current_time)){
			if(scrubber) 
			{
				engine->scrubber.cleaned++;
			}else{
				pthread_mutex_lock(&engine->stats.lock);
                        	engine->stats.reclaimed++;
                        	pthread_mutex_unlock(&engine->stats.lock);
                        	engine->items.itemstats[it->slabs_clsid].reclaimed++;
			}
			fprintf(stderr, "expired");
	
			do_item_unlink(engine, it);
			if(on_first_item){
                                lb->oldest_item = nextitem;
                        }else{
                                previtem->next = nextitem;
                        }

                        it->next = NULL;
                        it->prev = NULL;
 			item_free(engine, it);
                        lb->item_count--;
		}else if(it->prev != NULL && lb != (struct lru_bag *)(it->prev)){
			/* remove item from current bag and move to the correct one */
			if(lb->oldest_item == it){
				lb->oldest_item = nextitem;
			}else{
				previtem->next = nextitem;
			}

			if(lb->newest_item == it){
				if(previtem){
					lb->newest_item = previtem;
				}else{
					lb->newest_item = it->next;
				}
			}
			move_item(it,lbah);
			lb->item_count--;
		}else{
			if(scrubber){
				engine->scrubber.visited++;
			}
			/* leave item in place */
			on_first_item = false;
			previtem = it;
		}
		if(do_item_get(engine, item_get_key(it), it->nkey) == NULL){
			//fprintf(stderr, "item was in the lru, but missing from the HT, %s\n", (char *)item_get_key(it));
		}
		count++;
		it = nextitem;
	}
	fprintf(stderr, "cleaned bag, found %d/%d\n", count, total);
}

/* merge two bags into one fixing pointer of all  *
 * the items that were in the correct bag         *
 * eviction_lock must be held when calling       */ 
static void merge_bags(struct lru_bag *src,struct lru_bag *dest){
	//while(src->newest_item->next != NULL){
	//	src->newest_item = src->newest_item->next;
	//}
	if(src->newest_item == NULL){
		fprintf(stderr, "number of items in bag = %d\n", src->item_count);
	}
	
	if(dest->newest_item == NULL)
                fprintf(stderr, "number of items in bag = %d\n", dest->item_count);


	
	hash_item *it;

	src->newest_item->next = dest->oldest_item;
	dest->oldest_item = src->oldest_item;
	it = dest->oldest_item;
	int i = 0;
	for(i = 0; i < src->item_count; i++){
		if(it->prev == (hash_item *)src){
			it->prev = (hash_item *)dest;
		}
		it = it->next;
	}
	dest->item_count += src->item_count;
	src->item_count = 0;
#if 1 
	fprintf(stderr, "merged Bags after merging - tail of src points to %p\n",  (void *)src->newest_item->next);
	fprintf(stderr, "src - oldest = %p; newest = %p\n",  (void *)src->oldest_item,  (void *)src->newest_item);
	fprintf(stderr, "dest - oldest = %p; newest = %p\n",  (void *)dest->oldest_item,  (void *)dest->newest_item);
	it = dest->oldest_item;
	int count = 0;
	while(it != NULL){
		it = it->next;
		count++;
	}
	fprintf(stderr, "found %d/%d\n", count, dest->item_count);
#endif
}


/* Loops through all lru lists, which would include all * 
 * non-transient items looking for items to clean up    *
 * and remove or move to a new bag                      */
void clean_lru_lists(struct default_engine *engine, bool scrubber){
	struct lru_bag_array_head *lbah;
	struct lru_bag *lb, *prevlb, *nextlb;
	bool on_first_bag;
	int i;
	prevlb = NULL;

	for(i = 0; i < POWER_LARGEST; i++){
		on_first_bag = true;
		if(scrubber){
			for(int j = 0; j < 3; j++){
				add_new_bag(i);
			}
		}
		lbah = lru_bag_lists[i];
		/* make sure we have enough bags to actually clean something */
		if(lbah->num_bags <= 3) {
			continue;
		}
		lb = lbah->oldest_bag;
		while(lb != lbah->newest_alternate && lb){
			lock_bag(lb);
			clean_bag(engine,lb,lbah,scrubber);
			unlock_bag(lb);
			if(lb->item_count == 0){
				get_eviction_lock(i);
				nextlb = lb->next_bag;

				if(on_first_bag){
					lbah->oldest_bag = nextlb;
				}else{
					prevlb->next_bag = nextlb;
				}

				lb->next_bag = NULL;
				free(lb);
				lb = nextlb;
				(void)__sync_sub_and_fetch(&lbah->num_bags,1);
				release_eviction_lock(i);
			}else{
				if(lb->item_count < MERGE_COUNT && lb->next_bag != lbah->newest_alternate && lb->next_bag->item_count != 0){
					/* Merge a bag that is low on items with the next one */
					get_eviction_lock(i);
					merge_bags(lb, lb->next_bag);
					nextlb = lb->next_bag;

					if(on_first_bag){
						lbah->oldest_bag = nextlb;
					}else{
						prevlb->next_bag = nextlb;
					}

					free(lb);
					lb=nextlb;
					lbah->num_bags--;
					release_eviction_lock(i);
				}else{
					prevlb = lb;
					lb = lb->next_bag;
					on_first_bag = false;
				}
			}
		}
	}
}

/* This is the main loop for the worker thread   *
 * we could possibly make this MT by giving each *
 * thread a chunk of LRU's to look at.			 */
static void *lru_maintenance_loop(void *arg){
	struct default_engine *engine = arg;
	int i = 0;
	while(1){
		get_cleaner_lock();
		check_lru_current_bags();
		/*clean the bags every Nth iteration */
		if(i == 4 ){
			clean_lru_lists(engine, false);
			i = 0;
		}
		release_cleaner_lock();
		sleep(LRU_MAINT_INTERVAL);
		i++;
	}
	return NULL;
}

#endif
