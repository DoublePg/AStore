#pragma once


#include<iostream>
#include "../../deps/r2/src/common.hh"
#include "ReplyStation.h"
#include "../UDTransport.h"

enum MsgType{
  Req = 1,
  Reply = 2,
  Connect = 3,
};

struct __attribute__((packed)) Addr {
  u32 mac_id : 16; //表示mac_id占16位
  u32 thread_id : 16;
};

struct __attribute__((packed)) Meta {
  u8 cor_id;
  Addr dest;
};

struct __attribute__((packed)) Header {
  u32 type : 2; 
  u32 rpc_id : 5;
  u32 payload :18 ;
  u32 cor_id : 7;

  friend std::ostream &operator<<(std::ostream &os, const Header &h) {
    os << "type:" << h.type << "; rpc_id: " << h.rpc_id << "; payload:"  << h.payload
       << " cor_id: " << h.cor_id;
  }
} __attribute__((aligned(sizeof(u64))));

struct RPCOp{
    MemBlock msg;
    Header header;
    char *cur_ptr = nullptr;
    
  auto set_msg(const MemBlock &b) -> RPCOp & {
    ASSERT(b.sz >= sizeof(Header));
    this->msg = b;
    this->cur_ptr = (char *)this->msg.mem_ptr + sizeof(Header);
    return *this;
  }

  auto set_rpc_id(const u32 &id) -> RPCOp & {
    this->header.rpc_id = id;
    return *this;
  }

  auto set_req() -> RPCOp & {
    this->header.type = Req;
    return *this;
  }

  auto set_connect() -> RPCOp &{
    this->header.type = Connect;
    return *this;
  }

  auto set_reply() -> RPCOp & {
    this->header.type = Reply;
    return *this;
  }

  auto set_corid(const u32 &id) -> RPCOp & {
    this->header.cor_id = id;
    return *this;
  }

  /*!
    Note: the corid must be set using set_corid() before using this function
   */
  auto add_reply_entry(ReplyStation &s, const ReplyEntry &reply) -> RPCOp & {
    s.add_pending_reply(header.cor_id, reply);
    return *this;
  }

  auto add_one_reply(ReplyStation &s, const MemBlock &reply) -> RPCOp & {
    return this->add_reply_entry(s, ReplyEntry(reply));
  }

  /*!
    \ret: whether add succ
    返回是否添加成功
   */
  template <typename T> auto add_arg(const T &arg) -> bool {
    if (unlikely(sizeof(T) + this->cur_sz() > this->msg.sz)) {
      return false;
    }
    *reinterpret_cast<T *>(this->cur_ptr) = arg;
    this->cur_ptr += sizeof(T);
    return true;
  }

  auto add_opaque(const std::string &data) -> bool {
    if (unlikely(data.size() + this->cur_sz() > this->msg.sz)) {
      return false;
    }
    memcpy(this->cur_ptr, data.data(), data.size());
    this->cur_ptr += data.size();
    return true;
  }
  auto add_mem(const char* cur, int size) -> bool {
     if(size > this->msg.sz){
          return false;
     }
     memcpy(this->cur_ptr, cur, size);
     this->cur_ptr += size;
     return true;
  }

  auto finalize() -> RPCOp & {
    this->header.payload = this->cur_sz() - sizeof(Header);//负载大小 = 当前大小 - 头部大小
    *(reinterpret_cast<Header *>(this->msg.mem_ptr)) = this->header;//开始部分对应头部指针
    this->msg.sz = this->header.payload + sizeof(Header);//计算总大小
    return *this;
  }

  template <typename SendTrait>
  auto execute(SendTrait *s) -> Result<std::string> {
    return s->send(this->finalize().msg);
  }

  template <typename SendTrait>
  auto execute_w_key(SendTrait *s, const u32 &lkey) -> Result<std::string> {
    return s->send_with_key(this->finalize().msg,lkey);
  }

  auto cur_sz() -> usize { return cur_ptr - (char *)msg.mem_ptr; }

  static auto get_connect_op(const MemBlock &msg, const std::string &opaque_data)
      -> RPCOp {
    RPCOp op;
    op.set_msg(msg).set_connect().add_opaque(opaque_data);
    return op;
  }
};
