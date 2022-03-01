#pragma once

#include<sys/mman.h>
#include "../../../deps/rlib/core/rmem/mem.hh"
#include "../../../deps/rlib/core/common.hh"

using namespace rdmaio;

struct MemoryBlock{
    u64 size;
    void *addr = nullptr;
    
    MemoryBlock() = default;
    
    MemoryBlock(u64 sz, void *address)
    {
        size = sz;
        addr = address;
    }
    u64 my_size(){ return size;}
    
    virtual bool valid()
    {
        if(this ->addr == nullptr)
        {  
            return false;  
        }
        else
        {    
            return true;
        }
    }
    void *start_ptr() const { return addr;}

    ::rdmaio::Option<Arc<rmem::RMem>> convert_to_rmem(){
        if(!valid()) return {};
        return std::make_shared<rmem::RMem>(size, [this](u64 s){return addr;},
                                                [](rmem::RMem::raw_ptr_t p){});
        }  
};

class Region : public MemoryBlock {
   static u64 align_to_sz(const u64 &x, const usize &align_sz) {
    return (((x) + align_sz - 1) / align_sz * align_sz);
  }

  public:
    static ::rdmaio::Option<Arc<Region>> 
    create(const u64 &sz, const usize &align_sz = (2<<20)){
      auto region = std::make_shared<Region>(sz, align_sz);
      if (region -> valid()) { return region; }
      else { return {};  }
    }

    explicit Region(const u64& sz, const usize &align_sz = (2 << 20)){
      this -> size = align_to_sz(sz + align_sz, align_sz);
      char *ptr = (char *)mmap(
        nullptr, this->size, PROT_READ | PROT_WRITE,
        MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE | MAP_HUGETLB, -1, 0);

    if (ptr == MAP_FAILED) {
      this->addr = nullptr;
      RDMA_LOG(4) << "error allocating huge page wiht sz: " << this->size
                  << " aligned with: " << align_sz
                  << "; with error: " << strerror(errno);
    } else {
      RDMA_LOG(4) << "alloc huge page size: " << this->size;
      this->addr = ptr;
    }
  }
};
