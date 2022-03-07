#include <type_traits>
#include "../../deps/r2/src/thread.hh"
#include "../../deps/r2/src/common.hh"
#include "../core/RDMA/RPC/RPCOp.h"
#include "../core/RDMA/RPC/RPCore.h"
#include "../core/RDMA/batch_rw.h"
#include "../../deps/r2/src/rdma/async_op.hh"
#include "../core/RDMA/UDTransport.h"
#include "../core/RDMA/PBarrier.h"
#include "../core/RDMA/Region.h"
#include "../core/RDMA/Allocator.h"
#include "../../deps/kvs-workload/static_loader.hh"
#include "../../deps/kvs-workload/ycsb/mod.hh"
#include<pthread.h>
#include <chrono>
#include <random>

using namespace kvs_workloads::ycsb;
#define KEY_TYPE u64
#define PAYLOAD_TYPE u64
bool is_random = true;
int threads = 1;
int coros = 1;
std::string addr = "192.168.0.152:8080";
bool vlen = false;
int nkeys = 10000000;
bool load_from_file = true;
//std::string data_file ="ycsb-200M.bin.data";			
//std::string data_file = "lognormal-190M.bin.data";
//std::string data_file = "longlat-200M.bin.data";
std::string data_file = "osm_uni_10m.txt";
int len = 8;
int buf_size = 150;
int client_name = 4;
int data_count = 0;
int data_count1 = 0;
u64 model_buf_start;

//bool has_update = false;
enum RPCId{
  META = 0,
  GET = 1,
  INSERT = 2,
};
using XThread = ::r2::Thread<usize>;
using namespace r2::rdma;
std::vector<u64> all_keys;
int pp = 0;
std::shared_ptr<std::vector<u64>> all_key = 
   std::make_shared<std::vector<u64>>();
volatile bool running = true;
volatile bool running_insert = false;
struct alignas(128) Statics
{

	  typedef struct
		    {
			        u64 counter = 0;
				    u64 counter1 = 0;
				        u64 counter2 = 0;
					    u64 counter3 = 0;
					        double lat = 0;

						    double lat1 = 0;
						      } data_t;
	    data_t data;

	      char pad[128 - sizeof(data)];

	        
		   void increment() { data.counter += 1; }
	
		     void increment_gap_1(u64 d) { data.counter1 += d; }
		
		       void set_lat(const double& l) { data.lat = l; }
       };
template <class T>
T* get_search_keys(T array[], int num_keys, int num_searches) {
	std::mt19937_64 gen(std::random_device{}());
    std::uniform_int_distribution<int> dis(0, num_keys - 1);
    static T keys[10000000];
	for (int i = 0; i < num_searches; i++) {
		 int pos = dis(gen);
		  keys[i] = array[pos];
	 }
	return keys;
}
template <class T>
T* get_insert_keys(T array[], int num_keys, int num_searches) {
	std::mt19937_64 gen(std::random_device{}());
    std::uniform_int_distribution<int> dis(num_keys, 2*num_keys - 1);
    static T keys[10000000];
	for (int i = 0; i < num_searches; i++) {
		 int pos = dis(gen);
		  keys[i] = array[pos];
	 }
	return keys;
}

class ScrambledZipfianGenerator {
 public:
  static constexpr double ZETAN = 26.46902820178302;
  static constexpr double ZIPFIAN_CONSTANT = 0.99;

  int num_keys_;
  double alpha_;
  double eta_;
  std::mt19937_64 gen_;
  std::uniform_real_distribution<double> dis_;

  explicit ScrambledZipfianGenerator(int num_keys)
      : num_keys_(num_keys), gen_(std::random_device{}()), dis_(0, 1) {
    double zeta2theta = zeta(2);
    alpha_ = 1. / (1. - ZIPFIAN_CONSTANT);
    eta_ = (1 - std::pow(2. / num_keys_, 1 - ZIPFIAN_CONSTANT)) /
           (1 - zeta2theta / ZETAN);
  }

  int nextValue() {
    double u = dis_(gen_);
    double uz = u * ZETAN;

    int ret;
    if (uz < 1.0) {
      ret = 0;
    } else if (uz < 1.0 + std::pow(0.5, ZIPFIAN_CONSTANT)) {
      ret = 1;
    } else {
      ret = (int)(num_keys_ * std::pow(eta_ * u - eta_ + 1, alpha_));
    }

    ret = fnv1a(ret) % num_keys_;
    return ret;
  }

  double zeta(long n) {
    double sum = 0.0;
    for (long i = 0; i < n; i++) {
      sum += 1 / std::pow(i + 1, ZIPFIAN_CONSTANT);
    }
    return sum;
  }

  // FNV hash from https://create.stephan-brumme.com/fnv-hash/
  static const uint32_t PRIME = 0x01000193;  //   16777619
  static const uint32_t SEED = 0x811C9DC5;   // 2166136261
  /// hash a single byte
  inline uint32_t fnv1a(unsigned char oneByte, uint32_t hash = SEED) {
    return (oneByte ^ hash) * PRIME;
  }
  /// hash a 32 bit integer (four bytes)
  inline uint32_t fnv1a(int fourBytes, uint32_t hash = SEED) {
    const unsigned char* ptr = (const unsigned char*)&fourBytes;
    hash = fnv1a(*ptr++, hash);
    hash = fnv1a(*ptr++, hash);
    hash = fnv1a(*ptr++, hash);
    return fnv1a(*ptr, hash);
  }
};
u64 keys[20000000];

template <class T>
T* get_search_keys_zipf(T array[], int num_keys, int num_searches) {

  static T keys[10000000];
  ScrambledZipfianGenerator zipf_gen(num_keys);
  for (int i = 0; i < num_searches; i++) {
    int pos = zipf_gen.nextValue();
    keys[i] = array[pos];
  }
  return keys;
}
struct TreeNode
{
  int no;
  double a;
  double b;
  int version;
  int num_children;
  int data_capacity;
  int sz;
  int struct_no;
  int min_error;
  int max_error;
  u64 addr;  
  TreeNode **t;
};

//std::vector<TreeNode> tree;
//std::vector<int> temp_no;
struct address
{
	int no;
	int sz;
	int data_capacity;
	u64 addr;	
        int pulled_version;
        double a;
        double b;
        int min_error;
        int max_error;	
};

std::pair<int,TreeNode*> search(KEY_TYPE key,TreeNode* root)
{
    TreeNode* cur = root;
    TreeNode* temp = cur;
    int parent;
    if(cur ->num_children == 0)
    {
      return std::pair<int,TreeNode*>(0,cur);
    }
    while(1)
    {
      double bucketID_prediciton = cur->a * key + cur->b;
      int bucketID = static_cast<int>(bucketID_prediciton);
      bucketID =
         std::min<int>(std::max<int>(bucketID, 0), cur->num_children - 1);
      parent = cur->no;
      temp = cur->t[bucketID];
      cur = temp;
      
      if(cur->num_children == 0)
      {
	    break;
      }
    }
  return std::pair<int,TreeNode*>(parent,cur);
}
TreeNode *search1(KEY_TYPE key, TreeNode *root)
{
	TreeNode *cur = root;
	if (cur->num_children == 0)
	{
		return cur;
	}
	while (1)
	{
		double bucketID_prediciton = cur->a * key + cur->b;
		int bucketID = static_cast<int>(bucketID_prediciton);//(cur->a * key + cur ->b);
		bucketID =
			std::min<int>(std::max<int>(bucketID, 0), cur->num_children - 1);
	
		cur = cur->t[bucketID];
		if (cur->num_children == 0)
		{
			break;
		}
	}
	return cur;
}
int mm = 0;
using RPCORE = RPCore<UDTransport, UDRecv<2048>, UDSessionManager<2048>>;



auto server_lookup(const KEY_TYPE &key, RPCORE& rpc,
		   UDTransport &sender, 
		   R2_ASYNC) -> PAYLOAD_TYPE{
     char send_buf[64];
     char reply_buf[sizeof(PAYLOAD_TYPE)];
     RPCOp op;
     op.set_msg(MemBlock(send_buf, 64))
     .set_req()
     .set_rpc_id(1)
     .set_corid(R2_COR_ID())
     .add_one_reply(rpc.reply_station, {.mem_ptr = reply_buf, .sz = sizeof(PAYLOAD_TYPE)})
     .add_arg<KEY_TYPE>(key);
     ASSERT(rpc.reply_station.cor_ready(R2_COR_ID()) == false);
     auto ret = op.execute_w_key(&sender, 0);
     ASSERT(ret == IOCode::Ok);
	   // yield the coroutine to wait for reply
     R2_PAUSE_AND_YIELD;	   
     return *(reinterpret_cast<PAYLOAD_TYPE*>(reply_buf));
}


auto core(TreeNode* &p,Allocator alloc1,TreeNode *&root_,const KEY_TYPE& key,bool has_update,
        std::vector<TreeNode>&tree,std::vector<int>&temp_no, const Arc<RC>&rc, RPCORE& rpc,
		UDTransport &sender, char *my_buf,Statics &s,int thread_id,
		R2_ASYNC) -> PAYLOAD_TYPE{
	//return server_lookup(key,rpc,sender,R2_ASYNC_WAIT);  //全部由服务器查找
    
    double bucketID_prediction = p->a * key + p -> b;
    int bucketID = static_cast<int>(bucketID_prediction);
    bucketID =   std::min<int>(std::max<int>(bucketID, 0), p->data_capacity - 1);

   int max_range = 16;
   max_range = p->max_error - p->min_error+1 ;//固定长度版本需注释
   int read_sz = max_range * sizeof(KEY_TYPE);

   int position;  
   BatchOp<3> reqs;
   
   if(max_range < 130000)
   {
	if(bucketID >= 0 - p->min_error && bucketID < p->data_capacity - p->max_error)//动态版本一次拉
   {
	   reqs.emplace();
	   reqs.get_cur_op()
		   .set_read()
		   .set_rdma_rbuf(p->addr + (bucketID + p -> min_error) * sizeof(KEY_TYPE) + 2 * sizeof(int), rc->remote_mr.value().key)
		   .set_payload(my_buf , read_sz,rc->local_mr.value().lkey);
	   reqs.emplace();
	   reqs.get_cur_op()
		   .set_read()
		   .set_rdma_rbuf(p->addr+ 2 * sizeof(int) + (p->data_capacity) * sizeof(KEY_TYPE) + (bucketID + p->min_error) * sizeof(PAYLOAD_TYPE), rc->remote_mr.value().key)
		   .set_payload(my_buf + read_sz , read_sz,rc->local_mr.value().lkey);
	   reqs.emplace();
	   reqs.get_cur_op()
		   .set_read()
		   .set_rdma_rbuf(p->addr + sizeof(int),rc->remote_mr.value().key)
		   .set_payload(my_buf + 2*read_sz ,sizeof(int),rc->local_mr.value().lkey);
//LOG(2)<<"Here";
   }
   else if(bucketID < 0 - p->min_error)
   {
	   //if(read_sz > p->data_capacity)
	//	   read_sz = p->data_capacity;
	   reqs.emplace();
	   reqs.get_cur_op()
		   .set_read()
		   .set_rdma_rbuf(p->addr + 2 * sizeof(int), rc->remote_mr.value().key)
		   .set_payload(my_buf , read_sz,rc->local_mr.value().lkey);
	   reqs.emplace();
	   reqs.get_cur_op()
		   .set_read()
		   .set_rdma_rbuf(p->addr+ 2 * sizeof(int)  + (p->data_capacity) * sizeof(KEY_TYPE), rc->remote_mr.value().key)
		   .set_payload(my_buf + read_sz , read_sz,rc->local_mr.value().lkey);
	              
	   reqs.emplace();
	   reqs.get_cur_op()
		   .set_read()
		   .set_rdma_rbuf(p->addr + sizeof(int),rc->remote_mr.value().key)
		   .set_payload(my_buf + 2*read_sz ,sizeof(int),rc->local_mr.value().lkey);
  // LOG(2)<<"Threerer";
   }
   else
   {
	   
	   max_range = p->data_capacity - bucketID - p->min_error;
	   read_sz = max_range * sizeof(KEY_TYPE);
	   reqs.emplace();
	   reqs.get_cur_op()
		   .set_read()
		   .set_rdma_rbuf(p->addr + (p->min_error + bucketID) * sizeof(KEY_TYPE) + 2 * sizeof(int), rc->remote_mr.value().key)
		   .set_payload(my_buf , read_sz,rc->local_mr.value().lkey);
	   reqs.emplace();
	   reqs.get_cur_op()
		   .set_read()
		   .set_rdma_rbuf(p->addr+ 2 * sizeof(int) + (p->data_capacity) * sizeof(KEY_TYPE) + (p->min_error + bucketID)  * sizeof(PAYLOAD_TYPE), rc->remote_mr.value().key)
		   .set_payload(my_buf + read_sz , read_sz,rc->local_mr.value().lkey);
	   reqs.emplace();
	   reqs.get_cur_op()
		   .set_read()
		   .set_rdma_rbuf(p->addr + sizeof(int),rc->remote_mr.value().key)
		   .set_payload(my_buf + 2*read_sz ,sizeof(int),rc->local_mr.value().lkey);
//LOG(2)<<"lllll";
   }
   }
   else
   {
	    s.increment_gap_1(1);  
		        
	    return server_lookup(key, rpc, sender, R2_ASYNC_WAIT);
   }
  
  auto ret = reqs.execute_async(rc, R2_ASYNC_WAIT);  

  ASSERT(ret == ::rdmaio::IOCode::Ok)<<p->addr;
    KEY_TYPE tpq,tpq1;
    memcpy(&tpq,my_buf,sizeof(KEY_TYPE));
    memcpy(&tpq1,my_buf + (max_range-1) * sizeof(KEY_TYPE),sizeof(KEY_TYPE));
KEY_TYPE tp2;
    
    memcpy(&tpq,my_buf+sizeof(KEY_TYPE) * position,sizeof(KEY_TYPE));
   // RDMA_LOG(4)<<tpq;
    int bound = 1;
    int l,r;
    if(tpq > key)
    {
    int size = position;
    memcpy(&tpq1, my_buf + sizeof(KEY_TYPE) * (position - bound), sizeof(KEY_TYPE));
    while (bound < size &&
		    tpq1 > key)
    {
    bound *= 2;
    memcpy(&tpq1, my_buf + sizeof(KEY_TYPE) * (position - bound), sizeof(KEY_TYPE));
    }
    l = position - std::min<int>(bound, size);
    r = position - bound / 2;
    } 
    else {
	    int size = max_range - position;
	    memcpy(&tpq1, my_buf + sizeof(KEY_TYPE) * (position + bound), sizeof(KEY_TYPE));
	    while (bound < size &&
			    tpq1 <= key)
	    {
		    bound *= 2;
		    memcpy(&tpq1, my_buf + sizeof(KEY_TYPE) * (position + bound), sizeof(KEY_TYPE));
	    }
	    l = position + bound / 2;
	    r = position + std::min<int>(bound, size);
    }
    while(l < r)
    {
	    int mid = l + (r - l) / 2;
	    KEY_TYPE temp_key;
	    memcpy(&temp_key, my_buf + sizeof(KEY_TYPE) * mid, sizeof(KEY_TYPE));
	    if (temp_key <= key)
	    {
		    l = mid + 1;
	    }
	    else
	    {
		    r = mid;
	    }
    }
    int pos = l - 1; 
    KEY_TYPE temp_key;
    memcpy(&temp_key, my_buf + sizeof(KEY_TYPE) * pos, sizeof(KEY_TYPE));

    if(pos < 0)
    {
	    ASSERT(false);
	    return -1;	   
    }
    if (temp_key == key)	      	    
    {
	    PAYLOAD_TYPE tmp;
	    memcpy(&tmp,my_buf + sizeof(KEY_TYPE) * max_range + sizeof(PAYLOAD_TYPE) * pos, sizeof(PAYLOAD_TYPE));
		return tmp;

    }
    else
    { ASSERT(false); 

	  
    }
      
}   
  
template<class T>
std::string
format_value(T value, int precission = 4)
{
	  std::stringstream ss;
	  ss.imbue(std::locale(""));
	  ss << std::fixed << std::setprecision(precission) << value;
    return ss.str();
}

template<class T>
bool load_binary_data(T data[], int length, const std::string& file_path) {
	std::ifstream is(file_path.c_str(), std::ios::binary | std::ios::in);
	if (!is.is_open()) {
		return false;
	}
	is.read(reinterpret_cast<char*>(data), std::streamsize(length * sizeof(T)));
	is.close();
	return true;
}

//RPCore<UDTransport, UDRecv<2048>, UDSessionManager<2048>> rpc(12); 
int main(int argc, char** argv)
{

    std::vector<std::unique_ptr<XThread>> workers;
    if(load_from_file)
    {
      std::ifstream file(data_file);//OSM数据集读取
        std::string line;
        u64 ret;
       // std::istringstream iss;
	for(int i = 0; i < nkeys; ++i)
        {
            std::getline(file, line);
	    std::istringstream iss(line);
            iss >> ret;
	    if(i < 10)
		    std::cout<<ret<<std::endl;
	    all_key->push_back(ret);
		keys[i] = ret;
        }/*
        //auto keys = new long long[nkeys];
	load_binary_data(keys,2 * nkeys,data_file);
	u64 k;
	for(int i = 0; i < nkeys; i++)
	{
	  k = (u64)keys[i];
	  if(i < 10)
		  std::cout<<"key is "<<keys[i]<<" k is "<<k<<std::endl;
	  all_key->push_back(k);
	}//其他数据集读取*/
    }
    else 
    {
       for(int i = 0 ;i < nkeys; i++)
       {
            all_key->push_back(i);
       }
    }
    std::vector<Statics> statics(threads);
    PBarrier bar(threads + 1);
    for(int thread_id = 0; thread_id < threads; ++thread_id){
        workers.push_back(std::move(
        std::make_unique<XThread>([&statics, &bar, thread_id]() -> usize{
        int nic_idx = 0;
        if (thread_id >15){
            nic_idx = 1;
        }

	std::vector<TreeNode> tree;
	std::vector<int> temp_no;
   
	auto nic_for_sender = RNic::create(RNicInfo::query_dev_names().at(nic_idx)).value();
        auto qp = UD::create(nic_for_sender, QPConfig()).value();
        auto mem_region = Region::create(16 * 1024 * 1024).value();
	
        auto mem1 = mem_region -> convert_to_rmem().value();

        auto handler1 = RegHandler::create(mem1, nic_for_sender).value();

        Allocator alloc1(mem1, handler1->get_reg_attr().value());
        auto recv_rs_at_send =
          RecvEntriesFactory<Allocator, 2048, 1024>::create(alloc1);  
        { 
           auto res = qp->post_recvs(*recv_rs_at_send, 2048);
           RDMA_ASSERT(res == IOCode::Ok);
        }

        auto id = 1024 * client_name + thread_id;
        
        UDTransport sender;
        {
          r2::Timer t;
          do {
            auto res = sender.connect(
              addr, "b" + std::to_string(0/*thread_id*/), id, qp);
            if (res == IOCode::Ok) {
              break;
            }
            if (t.passed_sec() >= 10) {
              ASSERT(false) << "conn failed at thread:" << thread_id;
            }
          } while (t.passed_sec() < 10);        
        }
        RPCore<UDTransport, UDRecv<2048>, UDSessionManager<2048>> rpc(12);  
        auto send_buf = std::get<0>(alloc1.alloc_one(1024).value());
        
        ASSERT(send_buf != nullptr);
        auto lkey = handler1->get_reg_attr().value().key;

        memset(send_buf, 0 , 1024);
        auto conn_op = RPCOp::get_connect_op(MemBlock(send_buf, 2048),
                                             sender.get_connect_data().value());
        auto ret = conn_op.execute_w_key(&sender, lkey);//发送一条连接请求
        ASSERT(ret == IOCode::Ok);

        UDRecv<2048> recv_s(qp, recv_rs_at_send);
        SScheduler ssched;
        rpc.reg_poll_future(ssched, &recv_s);

        usize total_processed = 0;
        auto rc = RC::create(nic_for_sender, QPConfig()).value();
        
        ConnectManager cm(addr);
        if(cm.wait_ready(1000000, 2)==IOCode::Timeout)
        RDMA_ASSERT(false) << "cm connect to server timeout";

        auto qp_res =
          cm.cc_rc(client_name + " thread-qp" + std::to_string(thread_id),
                   rc,
                   nic_idx,
                   QPConfig());
                   RDMA_ASSERT(qp_res == IOCode::Ok) << std::get<0>(qp_res.desc);

        auto key = std::get<1>(qp_res.desc);//获取远程RC qp的密钥
        // RDMA_LOG(4) << "t-" << thread_id << " fetch QP authentical key: "
        // << key;

        auto fetch_res = cm.fetch_remote_mr(nic_idx);
        RDMA_ASSERT(fetch_res == IOCode::Ok) << std::get<0>(fetch_res.desc);
        rmem::RegAttr remote_attr = std::get<1>(fetch_res.desc);

        rc->bind_remote_mr(remote_attr);
        rc->bind_local_mr(handler1->get_reg_attr().value());
        char reply_buf[1024];
	//LOG(2)<<"the thread id is "<<thread_id;
	RPCOp op;
	op.set_msg(MemBlock((char*)send_buf, 2048))
	  .set_req()
	  .set_rpc_id(META)
	  .set_corid(0)
	  .add_one_reply(rpc.reply_station,{.mem_ptr = reply_buf, .sz = 1024})
	  .add_arg<u64>(73);
	  ASSERT(rpc.reply_station.cor_ready(0) == false);
	  auto ret1 = op.execute_w_key(&sender, lkey);
	  ASSERT(ret1 == IOCode::Ok);
	  while (rpc.reply_station.cor_ready(0) == false) 
	  {
	     r2::compile_fence();
	     rpc.Recv_loop(&recv_s);
	  }

	  struct t
	  {
	    u64 model_buf;
            int sze;
	    int num_nodes;
	  };
	  t t1;
	  memcpy(&t1,reply_buf,sizeof(t));
	  std::cout<<"服务器模型缓冲区的地址为："<<t1.model_buf<<std::endl;
	  std::cout<<"服务器模型缓冲区的大小为："<<t1.sze<<std::endl;
	  std::cout<<"索引节点总数为："<<t1.num_nodes<<std::endl;
          model_buf_start = t1.model_buf;
	  char* xcache_buf = reinterpret_cast<char*>(
		                   std::get<0>(alloc1.alloc_one(t1.sze).value()));
          {
            AsyncOp<1> op;
            op.set_read().set_payload((const u64*)xcache_buf, t1.sze, rc->local_mr.value().lkey);
	    op.set_rdma_rbuf((const u64*)t1.model_buf, remote_attr.key);
	    auto ret = op.execute(rc, IBV_SEND_SIGNALED);
	    ASSERT(ret == IOCode::Ok);
	    auto res_p = rc->wait_one_comp();
	    ASSERT(res_p == IOCode::Ok);   
	  
	  }
	  
	  char* cur_ptr;

	  cur_ptr = xcache_buf;
		
        TreeNode temp_node;	  
		  for (int i = 0; i < t1.num_nodes; i++)
		  {
			  memcpy(&temp_node.no, cur_ptr, sizeof(int));
			  cur_ptr += sizeof(int);
			  memcpy(&temp_node.a, cur_ptr, sizeof(double));
			  cur_ptr += sizeof(double);
			  memcpy(&temp_node.b, cur_ptr, sizeof(double));
			  cur_ptr += sizeof(double);
			  memcpy(&temp_node.num_children, cur_ptr, sizeof(int));
			  cur_ptr += sizeof(int);
			  if (temp_node.num_children == 0)
			  {
				  memcpy(&temp_node.version, cur_ptr, sizeof(int));
				  cur_ptr += sizeof(int);
				  memcpy(&temp_node.min_error, cur_ptr, sizeof(int));                                  
				  cur_ptr += sizeof(int);
				  memcpy(&temp_node.max_error, cur_ptr, sizeof(int));				  
				  cur_ptr += sizeof(int);

			  }
			  else
			  {
				  int length;
				  memcpy(&length,cur_ptr,sizeof(int));
				  cur_ptr += sizeof(int);	
				  cur_ptr += length * sizeof(int);
			  }
			  std::allocator<TreeNode *> allo;
			  temp_node.t = allo.allocate(temp_node.num_children); //为其num_children个子指针分配相应的空间
			  temp_node.addr = 0;
			  temp_node.data_capacity = 0;
			  temp_node.sz = 0;
			  tree.push_back(temp_node);
		  }
	  
	  
	  cur_ptr = xcache_buf;
	  for(int i = 0; i < t1.num_nodes; i++)
	  {
	  cur_ptr += sizeof(int);
	  cur_ptr += sizeof(double) * 2;
	  cur_ptr += sizeof(int);
	  int tp;
	  int repeat;
	  int length;
	  if(tree[i].num_children == 0)
	  {
		  
		  cur_ptr +=3* sizeof(int);
		  length = 0;
	  }
	  else
          {
		  memcpy(&length, cur_ptr, sizeof(int));
		  cur_ptr += sizeof(int);		
		  ASSERT(length % 2 == 0);
	  int l;
	  int ptr = 0;
          for(int j = 0; j < length / 2; j++)
	  {
             memcpy(&tp,cur_ptr,sizeof(int));
	     cur_ptr += sizeof(int);
	     memcpy(&repeat, cur_ptr, sizeof(int));
	     cur_ptr += sizeof(int);
	     for(int l = 0; l < repeat; l++)
	     {
		     tree[i].t[ptr++] = &tree[tp - 1];
	     }
	  }
          ASSERT(ptr == tree[i].num_children);	  
	  }
	}
	  int qq;
         struct address{
		  int no;
		  int data_capacity;
		  int sz;
		  u64 addr;
	  };
	address tmp;
        cur_ptr += sizeof(int) * 500000;
	
	
        int t = 0;
	int struct_size = t1.sze - (cur_ptr - xcache_buf);
	//std::cout<<"The struct size is "<<struct_size<<std::endl;
	for(int i = 0; i < t1.num_nodes; i++)
	{
	    /*if(tree[i].num_children == 0)
	    {
		    //将struct_no直接放到RDMA区域中
	    }*/
	  if(tree[i].num_children == 0)
	  {
	    memcpy(&tmp,cur_ptr,sizeof(address));
	    qq = tmp.no;
	    if(qq == tree[i].no)
	    {
	      tree[i].addr = tmp.addr;
	      tree[i].data_capacity = tmp.data_capacity;
	      tree[i].sz = tmp.sz;
	      tree[i].struct_no = t;
	      t++;
	      cur_ptr+=sizeof(address);
	    }
	    else
	    {
		    temp_no.push_back(tree[i].no);
	    }
	  }
	}
	int flag = 0;
	while((cur_ptr - xcache_buf) < struct_size)
	{
		memcpy(&tmp, cur_ptr, sizeof(address));
		for(int i = 0; i < temp_no.size(); i++)
		{
			if(tmp.no == temp_no[i])
			{
				tree[i].addr = tmp.addr;
				tree[i].data_capacity = tmp.data_capacity;
				tree[i].sz = tmp.sz;
				tree[i].struct_no = t;
				temp_no.erase(temp_no.begin() + i);
				t++;
				cur_ptr += sizeof(address);
				flag = 1;
			break;

			}
		}
		if(flag == 0)
		{
			ASSERT(false);
		}						                                             
		flag = 0;
	}
        LOG(4)<<"the thread id is "<<thread_id;	
        bar.wait();
        std::cout<<"Client finished!"<<std::endl;  
     
       TreeNode* root_ = &tree[0];
	
     for(int i = 0; i < 1; ++i)
      {
         ssched.spawn([ &statics,
			&sender,
                        &rc,
                        &alloc1,
		
			&root_,
			&rpc,
			&tree,
			&temp_no,
			lkey,
			send_buf,
			thread_id](R2_ASYNC) {
	char reply_buf[1024];
	char *my_buf = reinterpret_cast<char *>(std::get<0>(alloc1.alloc_one(20000).value()));
        int flag = 0;
	PAYLOAD_TYPE value = 0;
	int flag1 = 0;
	KEY_TYPE key;
	KEY_TYPE t = 1000;
    int j = 0;
	int m = 0;
	int l = 1;
	u64* lookup_keys;
    //lookup_keys = get_search_keys_zipf(keys,nkeys,nkeys);
	lookup_keys = get_search_keys(keys,nkeys,nkeys);

	while (running)
	{
	r2::compile_fence(); 
    if(m<10000000)
    {
       key = keys[m++];
    }
	else
    {
        m=0;
		key = keys[m];
    }

	TreeNode *p = search1(key,root_); //只读
	auto res = core(p,alloc1,root_,key,false,tree,temp_no,rc,rpc,sender,my_buf,statics[thread_id],thread_id,R2_ASYNC_WAIT);
	ASSERT(res == key)<<"res is"<< res<<";  target is: "<<key<<"data_count is "<<data_count;
	statics[thread_id].increment();

	}
	
	if (R2_COR_ID() == coros)
	{
		R2_STOP();
	}
	R2_RET;
       });
      } 
     ssched.run();
    })));
} for(auto& w : workers){
     w->start();
   }
   bar.wait();

   r2::Timer timer;
   int data = 0;
   int data1 = 0;
   std::vector<Statics> old_statics(threads);
   for(int epoch = 0 ;epoch < 20; epoch+=1){
       sleep(1);
     u64 sum = 0;
     u64 sum1 = 0;

	for(int i = 0; i < threads; ++i)
     {
     auto temp = statics[i].data.counter;
	 sum += (temp - old_statics[i].data.counter);
	 old_statics[i].data.counter = temp;
	 temp = statics[i].data.counter1;
	 sum1 += (temp - old_statics[i].data.counter1);
	 old_statics[i].data.counter1 = temp;
     }
     double passed_msec = timer.passed_msec();
     double res = static_cast<double>(sum) / passed_msec * 1000000.0;
     double res1 = static_cast<double>(sum1) / passed_msec * 1000000.0;
     r2::compile_fence();
     timer.reset();
     LOG(2) << "epoch @ " << epoch << ": thpt: " << format_value(res, 0)
            << " reqs/sec;"<<" server: "<<format_value(res1,0);
   }
  running = false;
  data_count = 0;
  data = 0;
  running_insert = true;
  timer.reset();


  running_insert = false;
  std::cout<<"running insert is "<<running_insert<<std::endl;
 // delete[] keys;
  for(auto& w : workers){
	  w->join();
  }
LOG(4) << "YCSB client finished";
return 0;
}

