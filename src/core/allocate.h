#pragma once
#include<lock.h>

class allocate{
    CompactSpinLock lock;
    char *mem_pool = nullptr;
    char *cur_ptr = nullptr;
    const u64 total_size = 0;
    u64 alloc_size = 0;
    u64 cur_alloc_num = 0;
    
public:
    allocate(char *mem,const u64 &sz) : mem_pool(mem), total_size(sz), cur_ptr(mem) {}
/*
    auto alloc(u64 alloc_sz) -> ::r2::Option<char *>{
        lock.lock();
        if(alloc_size + alloc_sz > total_size)
        {
            LOG(4)<<"Memory alloc failed";
            lock.unlock();
            return {};
        }   
        else
        {
           alloc_size += alloc_sz;
           auto temp_ptr = cur_ptr;
           cur_ptr += alloc_sz;
           cur_alloc_num++;
           lock.unlock();
           return temp_ptr;
        }
    }*/
    auto alloc_one(u64 alloc_sz) -> char*{
      lock.lock();
        if(alloc_size + alloc_sz > total_size)
        {
            LOG(4)<<"Memory alloc failed";
            lock.unlock();
            return {};
        }   
        else
        {
           alloc_size += alloc_sz;
           auto temp_ptr = cur_ptr;
           cur_ptr += alloc_sz;
           cur_alloc_num++;
           lock.unlock();
           return temp_ptr;
        }
    }
    void alloc(u64 alloc_sz){
    lock.lock();
    if(alloc_size + alloc_sz > total_size)
    {
      LOG(4)<<"Memory alloc failed";
      lock.unlock();
    }else{
      alloc_size += alloc_sz;	    
      cur_ptr += alloc_sz;
      cur_alloc_num++;
      lock.unlock();
    }
  }
      void reset(char *mem)	  
      {
			   
      	      mem_pool = mem;
	      cur_ptr = mem;
	      alloc_size = 0;
	      cur_alloc_num = 0;
      }
    u64 size() const { return total_size;}
    u64 cur_alloc() { return cur_alloc_num; }
    auto cur() {  return cur_ptr;}
    auto mem() { return mem_pool;}
    u64 alloc_sz() { return alloc_size;}

    auto dealloc()
    {

    }
    //auto dealloc(){ }考虑用指针定位

};
