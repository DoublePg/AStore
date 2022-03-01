#pragma once

#include <cstring>
#include <memory>

#include "../nic.hh"
#include "../utils/abs_factory.hh"

#include "./config.hh"
#include "./mem.hh"

namespace rdmaio {

namespace rmem {

using mr_key_t = u32;

/*!
  The attr that exchanged between nodes.
  节点之间交换的属性
 */
struct __attribute__((packed)) RegAttr {
  uintptr_t buf;
  u64 sz;
  mr_key_t key;
  mr_key_t lkey;
};

/*!
  RegHandler is a handler for register a block of continusous memory to RNIC.
  After a successful creation,
  one can get the attribute~(like permission keys) so that other QP can read the
  registered memory.
  RegHandler是用于将连续内存块注册到RNIC的处理程序。
  成功创建后，用户可以获得属性~(如权限密钥)，以便其他QP可以读取已注册的内存
  Example:
  `
  std::Arc<RNic> nic = std::makr_shared<RNic>(...);
  auto mem = Arc<RMem>(1024);
  RegHandler reg(mem,rnic);
  Option<RegAttr> attr = reg.get_reg_attr();
  `
 */
class RegHandler : public std::enable_shared_from_this<RegHandler> {

  ibv_mr *mr = nullptr; // mr in the driver

  // a dummy reference to RNic, so that once the rnic is destoried otherwise,
  // its internal ib context is not destoried
  //对RNIC的虚拟引用，这样一旦RNIC被以其他方式销毁，其内部ib上下文就不会被销毁
  Arc<RNic> rnic;

  // record the <ptr,size> of the registered memory
  // the internal buffer will be dealloced when RMem is destoried
  //记录寄存器内存的<ptr，size>。
  //当RMem被销毁时，内部缓冲区将被取消分配
  Arc<RMem>   rmem;

public:
  RegHandler(const Arc<RMem> &mem, const Arc<RNic> &nic,
             const MemoryFlags &flags = MemoryFlags())
      : rmem(mem), rnic(nic) {

    if (rnic->valid()) {

      auto raw_ptr = rmem->raw_ptr;
      auto raw_sz = rmem->sz;

      mr = ibv_reg_mr(rnic->get_pd(), raw_ptr, raw_sz, flags.get_value());

      if (!valid()) {
        RDMA_LOG(4) << "register mr failed at addr: (" << raw_ptr << ","
                    << raw_sz << ")"
                    << " with error: " << std::strerror(errno);
      }
    }
    // end class init
  }

  static Option<Arc<RegHandler>> create(const Arc<RMem> &mem, const Arc<RNic> &nic,
                                const MemoryFlags &flags = MemoryFlags()) {
    auto ret = Arc<RegHandler>(new RegHandler(mem,nic,flags));
    if (ret->valid())
      return ret;
    return {};
  }

  bool valid() const { return mr != nullptr; }

  Option<RegAttr> get_reg_attr() const {
    if (!valid())
      return {};
    return Option<RegAttr>({.buf = (uintptr_t)(rmem->raw_ptr),
                            .sz = rmem->sz,
                            .key = mr->rkey,
                            .lkey = mr->lkey,
      });
  }

  ~RegHandler() {
    if (valid())
      ibv_dereg_mr(mr);
  }

  DISABLE_COPY_AND_ASSIGN(RegHandler);
};

using register_id_t = u64;
using MRFactory = Factory<register_id_t, RegHandler>;

} // namespace rmem

} // namespace rdmaio
