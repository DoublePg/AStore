#pragma once

#include <unordered_map>

#include "../../../deps/r2/src/msg/ud_session.hh"
#include "../../../deps/rlib/core/lib.hh"
#include "../../../deps/rlib/core/qps/ud.hh"
#include "../../../deps/rlib/core/qps/recv_iter.hh"



using namespace rdmaio;
using namespace rdmaio::qp;

struct UDTransport {

  
  r2::UDSession *session = nullptr;

  explicit UDTransport(UDSession *t) : session(t) {}

  UDTransport() = default;

  auto connect(const std::string &addr, const std::string &s_name,
                    const u32 &my_id, Arc<UD> qp) -> Result<> {

    if (this->session != nullptr) {
      return ::rdmaio::Ok();
    }

    // try connect to the server
    ConnectManager cm(addr);
    auto wait_res = cm.wait_ready(1000000, 4);//尝试连接服务器，发送一个虚假请求
    if (wait_res != IOCode::Ok) {
      return transfer_raw(wait_res);
    }

    // fetch the server addr 获取服务器地址
    auto fetch_qp_attr_res = cm.fetch_qp_attr(s_name);
    if (fetch_qp_attr_res != IOCode::Ok) {
      return transfer_raw(fetch_qp_attr_res);
    }

    auto ud_attr = std::get<1>(fetch_qp_attr_res.desc);

    this->session = new UDSession(my_id, qp, ud_attr);

    return ::rdmaio::Ok();
  }

  auto get_connect_data() -> r2::Option<std::string> {
    if (this->session) {
      std::string ret(sizeof(QPAttr),'0');//初始化qp属性大小的字符串
      auto attr = this->session->ud->my_attr();//获取qp属性
      memcpy((void *)ret.data(), &attr, sizeof(QPAttr));//存入对应ret
      return ret;
    }
    return {};
  }

  auto send(const MemBlock &msg, const double &timeout = 1000000)
      -> Result<std::string> {
    return session->send_unsignaled(msg);
  }
  auto send_with_key(const MemBlock &msg, const u32 &key,
                       const double &timeout = 1000000) -> Result<std::string> {
    return session->send_unsignaled(msg,key);
  }
};

template <usize recv_num>
struct UDRecv{
  Arc<UD> qp;
  Arc<RecvEntries<recv_num>> entries;
  RecvIter<UD,recv_num> recv_iter;

  UDRecv(Arc<UD> qp, Arc<RecvEntries<recv_num>> e) 
     : qp(qp), entries(e) {
       recv_iter.set_meta(qp, entries);
  }

  void begin() {  recv_iter.begin(qp,entries->wcs); }
  
  void end() {  recv_iter.clear(); }
  
  bool has_msg() { return recv_iter.has_msgs(); }
  
  void next_msg() { recv_iter.next(); }
  
  auto cur_session_id() -> u32{//get imm_data
    return std::get<0>(recv_iter.cur_msg().value());
  }
  
  auto cur_msg() -> MemBlock{
    return MemBlock(
        reinterpret_cast<char *>(std::get<1>(recv_iter.cur_msg().value())) + kGRHSz,
      4000);
  }
};


template <class Derived, class SendTrait, class RecvTrait>
struct SessionManager {
       	std::unordered_map<u32, std::unique_ptr<SendTrait>> incoming_sesions;
	auto add_new_session(const u32 &id, const MemBlock &raw_connect_data, RecvTrait &recv_trait) -> Result<> {
	   return reinterpret_cast<Derived *>(this)->add_impl(id, raw_connect_data, recv_trait);
	  }
  };
	   
template <usize es>
struct UDSessionManager : public SessionManager<UDSessionManager<es>, UDTransport,UDRecv<es>> 
{
    // assumption: the id is not in the incoming_sessions
   auto add_impl(const u32 &id, const MemBlock &raw_connect_data, UDRecv<es> &recv_trait) -> Result<> {
   // should parse this as a UDPtr
   auto attr_ptr = raw_connect_data.interpret_as<QPAttr>(0);
   ASSERT(attr_ptr != nullptr);    
   auto transport =std::make_unique<UDTransport>(new UDSession(id, recv_trait.qp, *attr_ptr));
   this->incoming_sesions.insert(std::make_pair(id, std::move(transport)));   	                                                                   
   return ::rdmaio::Ok();
   }
 };

