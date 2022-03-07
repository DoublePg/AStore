#pragma once
#include "Region.h"
#include "../../deps/r2/src/ring_msg/mod.hh"
#include "../../deps/rlib/core/qps/config.hh"
#include "../../deps/rlib/core/rmem/mem.hh"
#include "../../deps/rlib/core/qps/abs_recv_allocator.hh"
using namespace r2;
using namespace rdmaio;
class Allocator {
   RMem::raw_ptr_t buf = nullptr;
   usize total_mem = 0;
   mr_key_t key;
   RegAttr mr;
public:
   Allocator(Arc<RMem> mem, const RegAttr &mr)
     : buf(mem -> raw_ptr), total_mem(mem -> sz), mr(mr), key(mr.key){}
  
   ::r2::Option<std::pair<rmem::RMem::raw_ptr_t, rmem::mr_key_t>>
   alloc_one(const usize &sz) {
       if(total_mem < sz){
         LOG(4) << "total: " << total_mem << "; cur sz:" << sz;
	 return {};
       }
       auto ret = buf;
       total_mem -= sz;
       buf = static_cast<char*>(buf) + sz;
       return std::make_pair(ret, key);
      }
   

   ::rdmaio::Option<std::pair<rmem::RMem::raw_ptr_t, rmem::RegAttr>>
   alloc_one_for_remote(const usize &sz){
       if (total_mem < sz)
          return {};
       auto ret = buf;
       buf = static_cast<char *>(buf) + sz;
       total_mem -= sz;
       return std::make_pair(ret, mr);
    }
};
