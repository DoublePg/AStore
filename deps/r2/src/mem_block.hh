#pragma once

#include "./common.hh"

namespace r2 {

/*!
    Msg is a safe wrapper over a raw pointer.
    To safely create/destory a message,
    please refer to constructors at msg_factory.hpp
    MSG是原始指针的安全包装器。
为了安全地创建/销毁消息，
请参考msg_factory.hpp上的构造函数
 */

struct MemBlock {
  void *mem_ptr = nullptr;
  u32 sz = 0;

  MemBlock() = default;

  MemBlock(void *data_p, const u32 &sz) : mem_ptr(data_p), sz(sz) {}

  template <typename T> inline T *interpret_as(const u32 &offset = 0) const {
    if (unlikely(sz - offset < sizeof(T)))
      return nullptr;
    { // unsafe
      return reinterpret_cast<T *>((char *)mem_ptr + offset);
    }
  }
};

} // namespace r2
