// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <iostream>

#include "rocksdb/cache.h"
#include "port/port.h"
#include "util/hash.h"
#include "util/mutexlock.h"
#include "table/format.h"
// #include "db/table_cache.h"
using namespace std;
bool multi_queue_init = true;
namespace rocksdb
{
  
  //added by ElasticBF
    namespace multiqueue_ns
    {
        struct LRUQueueHandle
        {
            void *value;
            void (*deleter)(const Slice &, void *value);
            LRUQueueHandle *next_hash;
            LRUQueueHandle *next;
            LRUQueueHandle *prev;
            size_t charge;      // TODO(opt): Only allow uint32_t?
            size_t key_length;
            bool in_cache;      // Whether entry is in the cache.
            uint32_t refs;      // References, including cache reference, if present.
            uint32_t hash;      // Hash of key(); used for fast sharding and comparisons
            uint64_t fre_count;   //frequency count
            uint64_t expire_time; //expire_time = current_time_ + life_time_
            uint16_t queue_id;   // queue id  start from 0  and 0 means 0 filter
            uint32_t value_id;
            uint32_t value_refs;
            bool is_split;
            LRUQueueHandle *table_handle, *prev_region, *next_region;

            char key_data[1];   // Beginning of key

            Slice key() const
            {
                // For cheaper lookups, we allow a temporary Handle object
                // to store a pointer to a key in "value".
                if (next == this)
                {
                    return *(reinterpret_cast<Slice *>(value));
                }
                else
                {
                    return Slice(key_data, key_length);
                }
            }
        };

        class HandleTable     // a list store LRUQueueHandle's address , don't care queue id
        {
        public:
            HandleTable() : length_(0), elems_(0), list_(NULL)
            {
                Resize();
            }
            ~HandleTable()
            {
                delete[] list_;
            }

            template <typename T>
            void ApplyToAllCacheEntries(T func) {
              for (uint32_t i = 0; i < length_; i++) {
                LRUQueueHandle* h = list_[i];
                while (h != nullptr) {
                  auto n = h->next_hash;
                  // assert(h->InCache());
                  func(h);
                  h = n;
                }
              }
            }

            LRUQueueHandle *Lookup(const Slice &key, uint32_t hash)
            {
                return *FindPointer(key, hash);
            }

            LRUQueueHandle *Insert(LRUQueueHandle *h)
            {
                LRUQueueHandle **ptr = FindPointer(h->key(), h->hash);
                LRUQueueHandle *old = *ptr;
                h->next_hash = (old == NULL ? NULL : old->next_hash);
                *ptr = h;
                if (old == NULL)
                {
                    ++elems_;
                    if (elems_ > length_)
                    {
                        // Since each cache entry is fairly large, we aim for a small
                        // average linked list length (<= 1).
                        Resize();
                    }
                }
                return old;
            }

            LRUQueueHandle *Remove(const Slice &key, uint32_t hash)
            {
                LRUQueueHandle **ptr = FindPointer(key, hash);
                LRUQueueHandle *result = *ptr;
                if (result != NULL)
                {
                    *ptr = result->next_hash;
                    --elems_;
                }
                return result;
            }

        private:
            // The table consists of an array of buckets where each bucket is
            // a linked list of cache entries that hash into the bucket.
            uint32_t length_;
            uint32_t elems_;
            LRUQueueHandle **list_;

            // Return a pointer to slot that points to a cache entry that
            // matches key/hash.  If there is no such cache entry, return a
            // pointer to the trailing slot in the corresponding linked list.
            LRUQueueHandle **FindPointer(const Slice &key, uint32_t hash)
            {
                LRUQueueHandle **ptr = &list_[hash & (length_ - 1)];
                while (*ptr != NULL &&
                        ((*ptr)->hash != hash || key != (*ptr)->key()))
                {
                    ptr = &(*ptr)->next_hash;
                }
                return ptr;
            }

            void Resize()
            {
                uint32_t new_length = 131072;
                while (new_length < elems_)
                {
                    new_length *= 2;
                }
                LRUQueueHandle **new_list = new LRUQueueHandle*[new_length];
                memset(new_list, 0, sizeof(new_list[0]) * new_length);
                uint32_t count = 0;
                for (uint32_t i = 0; i < length_; i++)
                {
                    LRUQueueHandle *h = list_[i];
                    while (h != NULL)
                    {
                        LRUQueueHandle *next = h->next_hash;
                        uint32_t hash = h->hash;
                        LRUQueueHandle **ptr = &new_list[hash & (new_length - 1)];
                        h->next_hash = *ptr;
                        *ptr = h;
                        h = next;
                        count++;
                    }
                }
                assert(elems_ == count);
                delete[] list_;
                list_ = new_list;
                length_ = new_length;
            }
        };

        class MultiQueue: public Cache
        {
            size_t capacity_;
            int lrus_num_;
            uint64_t life_time_;
            double change_ratio_;
            atomic<size_t> sum_lru_len;
            double expection_;
            // size_t *charges_;
            std::atomic<uint64_t> last_id_;
            mutable port::Mutex mutex_;
            // mutable leveldb::SpinMutex mutex_;  //for hashtable ,usage_ and e
            // Dummy head of in-use list.
            LRUQueueHandle in_use_;
            //Dummy heads of LRU list.
            LRUQueueHandle *lrus_;
            std::vector<size_t> lru_lens_;
            std::vector<size_t> sum_freqs_;
            HandleTable table_;
            bool strict_capacity_limit_;
            std::atomic<size_t> usage_;
            uint64_t current_time_;
            std::atomic<bool> shutting_down_;
            std::vector<double> fps;
            std::vector<size_t> bits_per_key_per_filter_, bits_per_key_per_filter_sum;  //begin from 0 bits
            int insert_count;
            bool need_adjust;
            uint64_t dynamic_merge_counter[2];
        public:
            MultiQueue(size_t capacity, std::vector<int> filter_bits_array, uint64_t life_time = 20000, double cr = 0.0001);
            ~MultiQueue();

            virtual const char *Name() const override
            {
                return "MultiQueueCache";
            }
            virtual void *Value(Handle *handle) override;
            virtual void DisownData() override {};

            virtual void SetCapacity(size_t capacity) override
            {
                MutexLock l(&mutex_);
                capacity_ = capacity;
            }

            virtual void SetStrictCapacityLimit(bool strict_capacity_limit) override
            {
                MutexLock l(&mutex_);
                strict_capacity_limit_ = strict_capacity_limit;
            }

            virtual Status Insert(const Slice &key, void *value, size_t charge,
                                  void (*deleter)(const Slice &key, void *value),
                                  Handle **handle, Priority priority) override;
            virtual Handle *Lookup(const Slice &key, Statistics *stats) override;
            virtual Handle *LookupRegion(const Slice &key, Statistics *stats, bool addFreq) override;
            virtual bool Ref(Handle *handle) override;
            virtual bool Release(Handle *handle, bool /*force_erase*/) override;
            virtual void Erase(const Slice &key) override;
            virtual uint64_t NewId() override;
            virtual size_t GetCapacity() const override {
              MutexLock l(&mutex_);
              return capacity_;
            }
            virtual bool HasStrictCapacityLimit() const override {
              MutexLock l(&mutex_);
              return strict_capacity_limit_;
            }
            virtual size_t GetUsage() const override {
              MutexLock l(&mutex_);
              return usage_;
            }
            virtual size_t GetUsage(Handle *handle) const override {
              return reinterpret_cast<const LRUQueueHandle*>(handle)->charge;
            }
            virtual size_t GetPinnedUsage() const override {
              MutexLock l(&mutex_);
              return usage_;
            }
            virtual void ApplyToAllCacheEntries(void (*callback)(void *, size_t),
                                                bool thread_safe) override;
            virtual void EraseUnRefEntries() override {
              printf("MultiQueue EraseUnRefEntries empty implement\n");
            }
            virtual std::string GetPrintableOptions() const override {
                const int kBufferSize = 200;
                char buffer[kBufferSize];
                {
                  MutexLock l(&mutex_);
                  // snprintf(buffer, kBufferSize, "    usage_: %lu, capacity_: %lu\n", usage_, capacity_);
                  snprintf(buffer, kBufferSize, "    usage_: -1, capacity_: -1\n");
                }
                return std::string(buffer);
            }

            void Ref(LRUQueueHandle *e, bool addFreCount);
            void Unref(LRUQueueHandle *e) ;
            void LRU_Remove(LRUQueueHandle *e);
            void LRU_Append(LRUQueueHandle *list, LRUQueueHandle *e);

            uint64_t LookupFreCount(const Slice &key);
            void SetFreCount(const Slice &key, uint64_t freCount);
            int AllocFilterNums(int freq);

            bool IsCacheFull() const;
            void TurnOnAdjustment();
            void TurnOffAdjustment();

            bool FinishErase(LRUQueueHandle *e);
            bool ShrinkLRU(int k, int64_t remove_charge[], bool force = false);

            uint64_t Num_Queue(int queue_id, uint64_t fre_count);
            virtual std::string LRU_Status() override;
            virtual void inline addCurrentTime() override
            {
                ++current_time_;
            }
            static inline uint32_t HashSlice(const Slice &s)
            {
                return Hash(s.data(), s.size(), 0);
            }
            void MayBeShrinkUsage();
            void RecomputeExp(LRUQueueHandle *e);
            void RecomputeExpTable(LRUQueueHandle *e);
            double FalsePositive(LRUQueueHandle *e);
        };

        MultiQueue::MultiQueue(size_t capacity, std::vector<int> filter_bits_array, uint64_t life_time, double change_ratio): capacity_(capacity), lrus_num_(filter_bits_array.size() + 1), life_time_(life_time)
            , change_ratio_(change_ratio), sum_lru_len(0), expection_(0), usage_(0), shutting_down_(false), insert_count(0), need_adjust(true)
        {
            //TODO: declare outside  class  in_use and lrus parent must be Initialized,avoid Lock crush
            in_use_.next = &in_use_;
            in_use_.prev = &in_use_;
            in_use_.queue_id = lrus_num_; //lrus_num = filter_num + 1
            lrus_ = new LRUQueueHandle[lrus_num_];
            lru_lens_.resize(lrus_num_);
            sum_freqs_.resize(lrus_num_);
            for(int i = 0 ; i  < lrus_num_ ; ++i)
            {
                lrus_[i].next = &lrus_[i];
                lrus_[i].prev = &lrus_[i];
                lrus_[i].queue_id = i;
                lru_lens_[i] = 0;
                sum_freqs_[i] = 0;
            }
            current_time_ = 0;
            // cout << "Multi-Queue Capacity:" << capacity_ << endl;
            int sum_bits = 0;
            fps.push_back(1.0); // 0 filters
            bits_per_key_per_filter_.push_back(0);
            bits_per_key_per_filter_sum.push_back(0);

            for(int i = 1 ; i  < lrus_num_ ; ++i)
            {
                sum_bits += filter_bits_array[i - 1];
                fps.push_back( pow(0.6185, sum_bits) );
                bits_per_key_per_filter_.push_back(filter_bits_array[i - 1]);
                bits_per_key_per_filter_sum.push_back(sum_bits);
            }

            dynamic_merge_counter[0] = dynamic_merge_counter[1] = 0;
        }

        MultiQueue::~MultiQueue()
        {
            assert(in_use_.next == &in_use_);  // Error if caller has an unreleased handle
            std::string lru_status = LRU_Status();
            // fprintf(stderr, "lru_status:\n%s\n", lru_status.c_str());
            // fflush(stderr);
            shutting_down_ = true;
            mutex_.Lock();
            // fprintf(stderr, "optimized expection_ is %lf\n", expection_);
            for(int i = 0 ; i < lrus_num_ ;  i++)
            {
                for (LRUQueueHandle *e = lrus_[i].next; e != &lrus_[i]; )
                {
                    LRUQueueHandle *next = e->next;
                    assert(e->in_cache);
                    e->in_cache = false;
                    assert(e->refs == 1);  // Invariant of lru_ list.
                    Unref(e);
                    e = next;
                }
            }
            // double avg_merge_region_nums = dynamic_merge_counter[0] == 0 ? 0 : dynamic_merge_counter[1] / dynamic_merge_counter[0];
            // fprintf(stderr, "multi_queue_init is %s, expection_ is %lf, avg_merge_region_nums is %lf\n", multi_queue_init ? "true" : "false", expection_, avg_merge_region_nums);
            mutex_.Unlock();
            delete []lrus_;
        }

        void MultiQueue::ApplyToAllCacheEntries(void (*callback)(void*, size_t),
                                                   bool thread_safe) {
          if (thread_safe) {
            mutex_.Lock();
          }
          table_.ApplyToAllCacheEntries(
              [callback](LRUQueueHandle* h) { callback(h->value, h->charge); });
          if (thread_safe) {
            mutex_.Unlock();
          }
        }

        std::string MultiQueue::LRU_Status()
        {
            MutexLock l(&mutex_);
            int count = 0;
            char buf[1024];
            std::string value;
            for(int i = 0 ; i < lrus_num_ ;  i++)
            {
                count = 0;
                for (LRUQueueHandle *e = lrus_[i].next; e != &lrus_[i]; )
                {
                    count++;
                    LRUQueueHandle *next = e->next;
                    e = next;
                }
                snprintf(buf, sizeof(buf), "\nlru %d count %d lru_lens_count:%lu \n", i, count, lru_lens_[i]);
                value.append(buf);
            }
            snprintf(buf, sizeof(buf), "lru insert_count %d\n", insert_count);
            value.append(buf);
            snprintf(buf, sizeof(buf), "current_time_ %lu\n", current_time_);
            value.append(buf);
            
            return value;
        }

        inline uint64_t MultiQueue::Num_Queue(int queue_id, uint64_t fre_count)
        {
            //mutex_.assertHeld();
            if(&lrus_[queue_id] == lrus_[queue_id].next)
            {
                return fre_count >> 1;
            }
            else
            {
                LRUQueueHandle *lru_handle = lrus_[queue_id].next;
                LRUQueueHandle *mru_handle = lrus_[queue_id].prev;
                return (lru_handle->fre_count + mru_handle->fre_count) / 2;
            }
        }

        inline double MultiQueue::FalsePositive(LRUQueueHandle *e)
        {
            return fps[e->queue_id];
        }

        void MultiQueue::RecomputeExpTable(LRUQueueHandle */*e*/)
        {
	  
        }

        void MultiQueue::RecomputeExp(LRUQueueHandle *e)
        {
            if(multi_queue_init || (e->queue_id + 1) == lrus_num_)
            {
                ++e->fre_count;
                sum_freqs_[e->queue_id]++;
                expection_ += FalsePositive(e);
            }
            else
            {
                // uint64_t start_micros = Env::Default()->NowMicros();
                double now_expection  = expection_ + FalsePositive(e) ;
                ++e->fre_count;
                double min_expection = now_expection, change_expection;
                const double new_expection = expection_ - (e->fre_count - 1) * FalsePositive(e) + e->fre_count * fps[e->queue_id + 1]; //TODO: OPTIMIZE
                int need_bits = usage_ - capacity_ + bits_per_key_per_filter_[e->queue_id + 1];
                // if (e->queue_id == 0)
                // {
                //     need_bits += lrus_[1].next->charge;
                // }
                // else
                // {
                //     need_bits += e->charge / bits_per_key_per_filter_sum[e->queue_id] * bits_per_key_per_filter_[e->queue_id + 1];
                // }
                int remove_bits, min_i = -1 ;
                if(need_bits > 0)
                {
                    for(int i = 1 ; i < lrus_num_ ; i++)
                    {
                        remove_bits = 0;
                        change_expection =  new_expection;
                        LRUQueueHandle *old = lrus_[i].next;
                        while(old != &lrus_[i] && remove_bits < need_bits)
                        {
                            if(old->expire_time < current_time_ )  // expired
                            {
                                remove_bits += bits_per_key_per_filter_[i];
                                // remove_bits += old->charge / bits_per_key_per_filter_sum[old->queue_id] * bits_per_key_per_filter_[i];
                                change_expection += (old->fre_count * fps[i - 1] - old->fre_count * FalsePositive(old));
                            }
                            else
                            {
                                break;
                            }
                            old = old->next;
                        }
                        if(remove_bits >= need_bits && change_expection < min_expection)
                        {
                            min_expection = change_expection;
                            min_i = i;
                        }
                    }
                    if(min_i != -1 && now_expection - min_expection > now_expection * change_ratio_)
                    {
                        assert(now_expection > min_expection);
                        remove_bits = 0;
                        while(lrus_[min_i].next != &lrus_[min_i] && remove_bits < need_bits)
                        {
                            LRUQueueHandle *old = lrus_[min_i].next;
                            RegionFilterInfo *rfi = reinterpret_cast<RegionFilterInfo *> (old->value);
                            rfi->adjusted_filter_nums = old->queue_id - 1;
                            remove_bits += bits_per_key_per_filter_[min_i];
                            // remove_bits += old->charge / bits_per_key_per_filter_sum[old->queue_id] * bits_per_key_per_filter_[min_i];
                            size_t delta_charge = bits_per_key_per_filter_[old->queue_id];
                            usage_ -= delta_charge;
                            // MeasureTime(Statistics::GetStatistics().get(), Tickers::REMOVE_EXPIRED_FILTER_TIME_0 + min_i, Env::Default()->NowMicros() - start_micros);
                            --lru_lens_[min_i];
                            sum_freqs_[min_i] -= old->fre_count;

                            LRU_Remove(old);
                            ++lru_lens_[min_i - 1];
                            sum_freqs_[min_i - 1] += old->fre_count;
                            LRU_Append(&lrus_[min_i - 1], old);
                        }
                        sum_freqs_[e->queue_id] -= e->fre_count;
                        sum_freqs_[e->queue_id + 1] += e->fre_count;
                        ++e->queue_id;
                        usage_ += bits_per_key_per_filter_[e->queue_id];

                        expection_ = min_expection;
                        RegionFilterInfo *rfi = reinterpret_cast<RegionFilterInfo *> (e->value);
                        rfi->adjusted_filter_nums = e->queue_id;
                    }
                    else
                    {
                        expection_ = now_expection;
                    }
                }
                else
                {
                    if(now_expection - new_expection > now_expection * change_ratio_)
                    {
                        //if(now_expection > new_expection){
                        sum_freqs_[e->queue_id] -= e->fre_count;
                        sum_freqs_[e->queue_id + 1] += e->fre_count;
                        ++e->queue_id;
                        usage_ += bits_per_key_per_filter_[e->queue_id];

                        expection_ = new_expection;
                        RegionFilterInfo *rfi = reinterpret_cast<RegionFilterInfo *> (e->value);
                        rfi->adjusted_filter_nums = e->queue_id;
                    }
                    else
                    {
                        expection_ = now_expection;
                    }
                }
            }
        }

        bool MultiQueue::Ref(Cache::Handle *handle) {
          LRUQueueHandle *e = reinterpret_cast<LRUQueueHandle*> (handle);
          MutexLock l(&mutex_);
          Ref(e, false);
          return true;
        }

        void MultiQueue::Ref(LRUQueueHandle *e, bool addFreCount)
        {
            //mutex_.assert_held();

            if (e->refs == 1 && e->in_cache)    // If on lru_ list, move to in_use_ list.
            {
                LRU_Remove(e);
                LRU_Append(&in_use_, e);
                --lru_lens_[e->queue_id];
                --sum_lru_len;
            }
            e->refs++;
            if(addFreCount)
            {
                if(e->expire_time > current_time_ )  //not expired
                {
                    RecomputeExp(e);
                }// else{ // if(e->expire_time < current_time_){   //expired
                //   if(e->fre_count > 0){
                //  expection_ -= (e->fre_count*1.0/2.0)*FalsePositive(e);
                //  e->fre_count /= 2;
                //   }
                // }
            }
            e->expire_time = current_time_ + life_time_;
        }

        void MultiQueue::Unref(LRUQueueHandle *e)
        {
            //mutex_.assert_held();
            assert(e->refs > 0);
            e->refs--;
            if (e->refs == 0)   // Deallocate.
            {
// deallocate:
                assert(!e->in_cache);
                expection_ -= e->fre_count * fps[e->queue_id];

                Slice key(e->key_data, sizeof(uint64_t));
                (*e->deleter)(key, e->value);
                free(e);
            }
            else if (e->in_cache && e->refs == 1)      // note:No longer in use; move to lru_ list.
            {
                MayBeShrinkUsage();
                LRU_Remove(e);
                LRU_Append(&lrus_[e->queue_id], e);
                ++lru_lens_[e->queue_id];
                ++sum_lru_len;
            }
        }

        void MultiQueue::LRU_Remove(LRUQueueHandle *e)
        {
            e->next->prev = e->prev;
            e->prev->next = e->next;
        }

        void MultiQueue::LRU_Append(LRUQueueHandle *list, LRUQueueHandle *e)
        {
            // Make "e" newest entry by inserting just before *list
            e->next = list;
            e->prev = list->prev;
            e->prev->next = e;
            e->next->prev = e;
            //if append to in_use , no need to remember queue_id,thus in_use_mutex used independently
            e->queue_id = list->queue_id == lrus_num_ ? e->queue_id : list->queue_id;
        }
        
        Cache::Handle *MultiQueue::Lookup(const Slice &key, Statistics *stats) {
          return LookupRegion(key, stats, false);
        }

        Cache::Handle *MultiQueue::LookupRegion(const Slice &key, Statistics */**/, bool addFreq)
        {
            static int enter = 0;
            if ((++enter) % 50000 == 0) {
                std::string lru_status = LRU_Status();
                // fprintf(stderr, "lru_status:\n%s\n", lru_status.c_str());
            }

            const uint32_t hash = HashSlice(key);

            MutexLock l(&mutex_);
            LRUQueueHandle *e = table_.Lookup(key, hash);
            if (e != NULL)
            {
                if(e->in_cache && e->refs == 1)
                {
                    Ref(e, addFreq);
                }
                else
                {
                    Ref(e, addFreq); //on in-use list or not in cache in the short time
                }
            }
            return reinterpret_cast<Cache::Handle *>(e);
        }

        uint64_t MultiQueue::LookupFreCount(const Slice &key)
        {
            const uint32_t hash = HashSlice(key);
            MutexLock l(&mutex_);
            LRUQueueHandle *e = table_.Lookup(key, hash);
            if (e != NULL)
            {
                return e->fre_count;
            }
            return 0;
        }

        void MultiQueue::SetFreCount(const Slice &key, uint64_t freCount)
        {
            const uint32_t hash = HashSlice(key);
            MutexLock l(&mutex_);
            LRUQueueHandle *e = table_.Lookup(key, hash);
            if (e != NULL)
            {
                expection_ -= e->fre_count * FalsePositive(e);
                e->fre_count = freCount;
                expection_ += e->fre_count * FalsePositive(e);
            }
        }

        bool MultiQueue::Release(Cache::Handle *handle, bool /*force_erase*/)
        {
            auto lru_queue_handle = reinterpret_cast<LRUQueueHandle *>(handle);
            MutexLock l(&mutex_);
            Unref(lru_queue_handle);
            return true;
        }

        void MultiQueue::MayBeShrinkUsage()
        {
            //mutex_.assertheld
            // if(usage_ >= 2 * capacity_ / 3)
            //     multi_queue_init = false;

            if(usage_ >= capacity_)
            {
                multi_queue_init = false;
                int64_t overflow_charge = usage_ - capacity_;
                if(!ShrinkLRU(lrus_num_ - 1, &overflow_charge, false))
                {
                    overflow_charge = usage_ - capacity_;
                    ShrinkLRU(1, &overflow_charge, true);
                }
            }
        }

        inline bool MultiQueue::ShrinkLRU(int k, int64_t remove_charge[], bool force)
        {
            //mutex_.assertHeld
            int64_t removed_usage = 0;
            if(!force)
            {
                while (usage_ > capacity_ && k >= 1)
                {
                    while(lrus_[k].next != &lrus_[k]  && removed_usage < remove_charge[0])
                    {
                        LRUQueueHandle *old = lrus_[k].next;
                        // assert(old->refs >= 1);
                        //TODO: old->fre_count = queue_id
                        if(old->refs == 1 && old->expire_time < current_time_)
                        {
                            RegionFilterInfo *rfi = reinterpret_cast<RegionFilterInfo *>(old->value);
                            rfi->adjusted_filter_nums = old->queue_id - 1;
                            // uint64_t start_micros = Env::Default()->NowMicros();
                            size_t delta_charge = bits_per_key_per_filter_[old->queue_id];
                            // MeasureTime(Statistics::GetStatistics().get(), Tickers::REMOVE_EXPIRED_FILTER_TIME_0 + k, Env::Default()->NowMicros() - start_micros);
                            old->charge -= delta_charge;
                            usage_ -= delta_charge;
                            removed_usage += delta_charge;
                            expection_ -= old->fre_count * fps[k];
                            old->fre_count = Num_Queue(k - 1, old->fre_count); // also decrease fre count
                            expection_ += old->fre_count * fps[k - 1];
                            --lru_lens_[k];
                            sum_freqs_[k] -= old->fre_count;
                            LRU_Remove(old);
                            ++lru_lens_[k - 1];
                            sum_freqs_[k - 1] += old->fre_count;
                            LRU_Append(&lrus_[k - 1], old);
                        }
                        else
                        {
                            break;
                        }
                    }
                    k--;
                }
                if(removed_usage >= remove_charge[0])
                {
                    return true;
                }
                return false;
            }
            else
            {
                size_t max_lru_lens = 0;
                int max_i = -1;
                for(int i = 1 ; i < lrus_num_ ; i++)
                {
                    if(lru_lens_[i] > max_lru_lens)
                    {
                        max_lru_lens = lru_lens_[i];
                        max_i = i;
                    }
                }
                if(max_i != -1)
                {
                    k = max_i;
                    while(removed_usage < remove_charge[0])
                    {
                        // uint64_t start_micros = Env::Default()->NowMicros();
                        LRUQueueHandle *old = lrus_[k].next;
                        // assert(old != &lrus_[k]);
                        if (old == &lrus_[k])
                        {
                            printf("eroor! empty lru!\n");
                            int i = 1;
                            while (i)
                            {
                                int b = 0;
                                b += 1;
                            }
                        }
                        RegionFilterInfo *rfi = reinterpret_cast<RegionFilterInfo *>(old->value);
                        rfi->adjusted_filter_nums = old->queue_id - 1;
                        size_t delta_charge = bits_per_key_per_filter_[old->queue_id];
                        old->charge -= delta_charge;
                        usage_ -= delta_charge;
                        removed_usage += delta_charge;
                        expection_ -= old->fre_count * fps[k];
                        old->fre_count = Num_Queue(k - 1, old->fre_count); // also decrease fre count
                        expection_ += old->fre_count * fps[k - 1];
                        --lru_lens_[k];
                        sum_freqs_[k] -= old->fre_count;
                        LRU_Remove(old);
                        LRU_Append(&lrus_[k - 1], old);
                        ++lru_lens_[k - 1];
                        sum_freqs_[k - 1] += old->fre_count;
                        // MeasureTime(Statistics::GetStatistics().get(), Tickers::REMOVE_HEAD_FILTER_TIME_0 + k, Env::Default()->NowMicros() - start_micros);
                    }
                }
                return true;
            }
        }

        int MultiQueue::AllocFilterNums(int freq)
        {
            int i = 1;
            int last_ = 1;
            size_t freq_t = freq;
            while(i < lrus_num_)
            {
                if(lrus_[i].next != &lrus_[i])
                {
                    if (freq_t < sum_freqs_[i] / lru_lens_[i])
                        return i;
                    last_ = i;
                }
                ++i;
            }
            // return last_ + 1 < lrus_num_ ? last_ + 1 : last_;
            return last_;
        }

        inline bool MultiQueue::IsCacheFull() const
        {
            return usage_ >= capacity_;
        }

        void MultiQueue::TurnOnAdjustment()
        {
            MutexLock l(&mutex_);
            need_adjust = true;
        }

        void MultiQueue::TurnOffAdjustment()
        {
            MutexLock l(&mutex_);
            need_adjust = false;
        }

        Status MultiQueue::Insert(const Slice &key, void *value, size_t charge,
                                  void (*deleter)(const Slice &key, void *value),
                                  Cache::Handle **handle,
                                  Cache::Priority /*priority*/)
        {
            const uint32_t hash = HashSlice(key);
            mutex_.Lock();
            insert_count++;

            // leveldb::TableAndFile *tf = reinterpret_cast<leveldb::TableAndFile *>(value);
            // int regionNum = tf->table->getRegionNum();

            LRUQueueHandle *e = reinterpret_cast<LRUQueueHandle *>(
                                    malloc(sizeof(LRUQueueHandle) - 1 + key.size()));
            e->value = value;
            e->deleter = deleter;
            e->charge = charge;
            e->key_length = key.size();
            e->hash = hash;
            e->in_cache = false;
            e->refs = (handle == nullptr
                 ? 1
                 : 2);  // for the returned handle.
            e->fre_count = 0;
            e->expire_time = current_time_ + life_time_;

            RegionFilterInfo *rfi = reinterpret_cast<RegionFilterInfo *>(e->value);
            e->value_id = rfi->region_num;
            e->queue_id = rfi->cur_filter_nums;

            memcpy(e->key_data, key.data(), key.size());

            if (capacity_ > 0)
            {
                // char buf[sizeof(uint64_t) + sizeof(uint32_t)];
                // memcpy(buf, e->key_data, sizeof(uint64_t));
                // regionId_ = (uint32_t *) (buf + sizeof(uint64_t));
                // *regionId_ = 0;
                // Slice key_(buf, sizeof(buf));
                // uint32_t hash_ = HashSlice(key_);
                // LRUQueueHandle *table_handle = table_.Lookup(key_, hash_);
                // assert(table_handle);
                // table_handle->value_refs++;
                // e->table_handle = table_handle;
                if (e->value_id == 0)
                    e->prev_region = NULL;
                else
                {
                    // *regionId_ = e->value_id - 1;
                    // Slice key2(buf, sizeof(buf));
                    // hash_ = HashSlice(key2);
                    // LRUQueueHandle *prev_region = table_.Lookup(key2, hash_);
                    // e->prev_region = prev_region;
                    // prev_region->next_region = e;
                }
                e->next_region = NULL;

                // e->refs++;  // for the cache's reference.
                // ++e->fre_count; //for the first access
                // expection_ += fps[e->queue_id];     //insert a new element ,expected number should be updated
                e->in_cache = true;
                // mutex_.Lock();
                LRU_Append(&lrus_[e->queue_id], e);
                ++lru_lens_[e->queue_id];
                ++sum_lru_len;

                usage_ += charge;
                auto redun_handle = table_.Insert(e);
                if(redun_handle != NULL)
                {
                    FinishErase(redun_handle);
                }
                // mutex_.Unlock();

            } // else don't cache.  (Tests use capacity_==0 to turn off caching.)

            // mutex_.Lock();
            MayBeShrinkUsage();
            mutex_.Unlock();

            if (handle != nullptr)
              *handle = reinterpret_cast<Cache::Handle *>(e);
            return Status::OK();
        }

        bool MultiQueue::FinishErase(LRUQueueHandle *e)
        {
            if (e != NULL)
            {
                assert(e->in_cache);
                LRU_Remove(e);
                if(e->refs == 1)     //means remove from LRU
                {
                    --lru_lens_[e->queue_id];
                    sum_freqs_[e->queue_id] -= e->fre_count;
                    --sum_lru_len;
                }
                e->in_cache = false;
                usage_ -= e->charge;
                Unref(e);
            }
            return e != NULL;
        }
        //todo
        void MultiQueue::Erase(const Slice &key)
        {
            const uint32_t hash = HashSlice(key);
            MutexLock l(&mutex_);
            auto obsolete_handle = table_.Remove(key, hash);
            if(obsolete_handle != NULL)
            {
                FinishErase(obsolete_handle);
            }
        }

        uint64_t MultiQueue::NewId()
        {
            return last_id_.fetch_add(1, std::memory_order_relaxed);
        }

        void *MultiQueue::Value(Cache::Handle *handle)
        {
            return reinterpret_cast<LRUQueueHandle *>(handle)->value;
        }

    };


    std::shared_ptr<Cache> NewMultiQueue(size_t capacity, std::vector<int> filter_bits_array, uint64_t life_time, double change_ratio)
    {
        return std::make_shared<multiqueue_ns::MultiQueue>(capacity, filter_bits_array, life_time, change_ratio);
    }

    
};

