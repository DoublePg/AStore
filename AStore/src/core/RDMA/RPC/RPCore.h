#pragma once

#include<functional>

#include "../../deps/r2/src/sshed.hh"
#include "RPCOp.h"
#include "../UDTransport.h"

template<class SendTrait, class RecvTrait, class Manager>
struct RPCore
{
   ReplyStation reply_station;
   using rpc_func = std::function<void(const Header& rpc_header, const MemBlock &msg,SendTrait* replyc)>;
   std::vector<rpc_func> callbacks;
   
   SessionManager<Manager, SendTrait, RecvTrait> session_manager;
   explicit RPCore(int num) : reply_station(num) {}

   auto reg_callbacks(rpc_func callback) -> usize
   {
       callbacks.push_back(callback);
       return callbacks.size();
   }

   auto execute(const RPCOp& op, SendTrait* sender) -> Result<std::string>
   {
       return sender->send(op.msg);
   }

   auto execute_with_key(const RPCOp& op, SendTrait* sender, const u32&key)
   -> Result<std::string>
   {
       return sender->send_with_key(op.msg, key);
   }

   auto Recv_loop(RecvTrait* recv) -> usize
   {
       int num = 0; 
       for(recv->begin();recv->has_msg(); recv->next_msg())
       {
	   num+=1;
	   ASSERT(sizeof(Header) <= recv -> cur_msg().sz);
           Header& head = *(reinterpret_cast<Header*>(recv -> cur_msg().mem_ptr));
           ASSERT(head.payload + sizeof(head) <= recv ->cur_msg().sz)
           <<"The cur_msg size is to small with size :"<<recv -> cur_msg().sz
           <<"; and the payload is" << head.payload <<"; and the session id is:"
           <<recv -> cur_session_id();
	   auto session_id = recv->cur_session_id();
           MemBlock payload((char*)recv -> cur_msg().mem_ptr + sizeof(Header), head.payload);
           if(head.type == Connect) {
           this -> session_manager.add_new_session(session_id, payload, *recv);
           }
           else if(head.type == Req){
		   
            try {
            auto reply_channel =
            this->session_manager.incoming_sesions[session_id].get();
            auto f = callbacks.at(head.rpc_id);
            f(head, payload, reply_channel);
            } catch (...) { ASSERT(false) << "rpc called failed with rpc id " << head.rpc_id;}
           }
           else if(head.type == Reply){
            
             auto ret = this->reply_station.append_reply(head.cor_id, payload);//向回应缓冲区写数据
             ASSERT(ret) << "add reply error: " << head;
           }
           else { ASSERT(false)<< "UnKnown msg_header type!"; }
       }
       recv -> end();
       return num;
   }

   auto reg_poll_future(::r2::SScheduler& sshed, RecvTrait* recv)
  {
    poll_func_t poll_future =
      [&sshed, this, recv]() -> Result<std::pair<::r2::Routine::id_t, usize>> {
      for (recv->begin(); recv->has_msg(); recv->next_msg()) {
        auto cur_msg = recv->cur_msg();//接受特征的当前消息
        auto session_id = recv->cur_session_id();//当前会话ID
        ASSERT(cur_msg.sz >= sizeof(Header));

        // parse the RPC header
        Header& h = *(reinterpret_cast<Header*>(cur_msg.mem_ptr));
        ASSERT(h.payload + sizeof(Header) <= cur_msg.sz)
          << "cur msg sz: " << cur_msg.sz << "; payload: " << h.payload
          << "; session id: " << session_id; // sanity check header and content

        MemBlock payload((char*)cur_msg.mem_ptr + sizeof(Header), h.payload);

        switch (h.type) {
          case Req: {
            try {
              auto reply_channel =
                this->session_manager.incoming_sesions[session_id].get();
              callbacks.at(h.rpc_id)(h, payload, reply_channel);
            } catch (...) {
              ASSERT(false) << "rpc called failed with rpc id " << h.rpc_id;
            }
          } break;
          case Reply: {
            // pass
            auto ret = this->reply_station.append_reply(h.cor_id, payload);
            ASSERT(ret) << "add reply for: " << h.cor_id << " error";

            if (this->reply_station.cor_ready(h.cor_id)) {
              sshed.addback_coroutine(h.cor_id);
            }
          } break;
          case Connect: {
            this->session_manager.add_new_session(session_id, payload, *recv);
          } break;
          default:
            ASSERT(false) << "not implemented";
        }
      }
      recv->end();
      // this future in principle should never return
      return NotReady(std::make_pair<::r2::Routine::id_t>(0u, 0u));
    };

    sshed.emplace_future(poll_future);//向sshed中存入该轮询函数
  }

};
