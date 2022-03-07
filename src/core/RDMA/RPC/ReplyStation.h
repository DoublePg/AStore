#pragma once

#include <vector>

#include "../../../deps/r2/src/mem_block.hh"

using namespace r2;


struct ReplyEntry//回应条目结构，包括附加回应条数，回应缓冲区和当前指针
{
  usize pending_replies = 0;
  MemBlock reply_buf;
  char* cur_ptr;

  ReplyEntry(const MemBlock& reply_buf)//构造函数，初始化附加回应数量为1，缓冲区为传入参数的空间，指针为缓冲区开始指针
    : pending_replies(1)
    , reply_buf(reply_buf)
    , cur_ptr(reinterpret_cast<char*>(reply_buf.mem_ptr))
  {}

  ReplyEntry() = default;
};

/*!
  Record the reply entries of all coroutines.
  Currently, we assume fixed coroutines stored using vector.
  The number of coroutines must be passed priori to the creation of the station.
  记录所有协程的回复条目。
  目前，我们假设使用向量存储的固定协程。
  必须在创建站点之前预先传递协程的数量。
 */
struct ReplyStation
{
  // TODO: reply vector with T, which implements a mapping from corid ->
  // ReplyEntry
  std::vector<ReplyEntry> cor_replies;//每一个历程一个回应条目

  explicit ReplyStation(int num_cors)
    : cor_replies(num_cors)
  {}

  /*!
    return whether there are pending replies
   */
  auto add_pending_reply(const int& cor_id, const ReplyEntry& reply) -> bool//每一个历程只能有一个条目
  {
    ASSERT(cor_id < cor_replies.size());
    if (cor_replies[cor_id].pending_replies > 0) {
      return false;
    }
    cor_replies[cor_id] = reply;
    return true;
  }

  auto cor_ready(const int& cor_id) -> bool
  {
    return cor_replies[cor_id].pending_replies == 0;
  }

  /*!
    \ret: whether append reply is ok
   */
  auto append_reply(const int& cor_id, const MemBlock& payload) -> bool
  {
    if (unlikely(this->cor_ready(cor_id))) {
      return false;
    }

    auto& r = this->cor_replies[cor_id];
    ASSERT(r.cur_ptr + payload.sz <=
           (char*)r.reply_buf.mem_ptr + r.reply_buf.sz)
      << "overflow reply sz: " << payload.sz
      << "; total reply: " << r.reply_buf.sz;
    memcpy(r.cur_ptr, payload.mem_ptr, payload.sz);//向回应缓冲区添加负载
    r.cur_ptr += payload.sz;

    r.pending_replies -= 1;
    return true;
  }
};
/*
struct ReplyEntry
{
    MemBlock reply_buf;
    char *cur_ptr;
    bool is_busy = false;
    ReplyEntry(const MemBlock &buf){
        reply_buf = buf;
        cur_ptr = reinterpret_cast<char*>(buf.mem_ptr);
        is_busy = true;
    }
    ReplyEntry() = default;
};

struct ReplyStation
{
    std::vector<ReplyEntry> Reply;
    int number;
    ReplyStation(int num){
        Reply(num);
        number = num;
    }

    auto add_reply(const int& num, const ReplyEntry& re) -> bool
    {
        ASSERT(num < number);
        if(Reply.at(num).is_busy) { return false;}
        Reply[num] = re;
        return true;
    }

    auto reply_add_Payload(const int& num, const MemBlock& payload) -> bool
    {
        if(Reply.at(num).is_busy == false){
            return false;
        }

        auto &r = this -> Reply.at(num);
        ASSERT(r.cur_ptr + payload.sz <= (char*)r.reply_buf.mem_ptr + r.reply_buf.sz)
              <<"Reply buf overflow";
        r.cur_ptr += payload.sz;
        r.is_busy = false;
        return true;
    }  
};
*/
