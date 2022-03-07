#include "../core/RDMA/PBarrier.h"
#include "../core/RDMA/Region.h"
#include "../core/RDMA/Allocator.h"

#include  "../core/allocate.h"
#include "../core/RDMA/RPC/RPCOp.h"
#include "../core/RDMA/RPC/RPCore.h"
#include "../../deps/r2/src/timer.hh"
#include "../../deps/r2/src/thread.hh"
#include "../../deps/rlib/core/lib.hh"
#include "../core/alex.h"
#include "../../deps/rlib/core/common.hh"
#include "../../deps/r2/src/common.hh"
#include<queue>
#define KEY_TYPE u64
#define PAYLOAD_TYPE u64

int nic_id = 0;
int nkeys = 10000000;
int nic_name = 0;
int threads = 1;
int port = 8080;
bool load_from_file = true;
std::string data_file = "../B-Plus-Tree-master/osm_uni_10m.txt";
using namespace rdmaio;
using namespace rdmaio::rmem;
//std::string keys_file_path = "ycsb-200M.bin.data";			      
std::string keys_file_path = "lognormal-190M.bin.data";
//std::string keys_file_path = "longlat-200M.bin.data";
volatile bool running = true;
volatile bool init = false;
RCtrl ctrl(port);
unsigned int MB = 1024 * 1024;
using namespace alex;
u64 model_buf;
auto mem = Region::create(1280 * MB).value();
allocate xalloc((char*)mem->start_ptr(), mem->size);
alex::Alex<KEY_TYPE,PAYLOAD_TYPE> index1;
int temp_sz;


int flagt = 0;


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
void Set_RDMA_region(allocate xalloc)
{
	index1.set_no(index1.root_node_);
	auto node = index1.root_node_;
	std::queue<alex::AlexNode<KEY_TYPE, PAYLOAD_TYPE> *> myqueue;
	alex::AlexModelNode<KEY_TYPE, PAYLOAD_TYPE> *cur;
	alex::AlexDataNode<KEY_TYPE, PAYLOAD_TYPE> *cur1;
	int repeats;
	myqueue.push(node);	       
        index1.data_buf_start = (char*)xalloc.cur();       
	int jj = 0;
	//RDMA_LOG(4)<<index1.num_nodes();
	while (1)		
	{		
		if (!node->is_leaf_)		
		{
			cur = static_cast<Alex<KEY_TYPE, PAYLOAD_TYPE>::model_node_type *>(node);
			for (int i = 0; i < cur->num_children_; i++)
			{
				myqueue.push(cur->children_[i]);
				repeats = 1 << cur->children_[i]->duplication_factor_;
				i += repeats - 1;
			}
		}
		else
		{
			cur1 = static_cast<Alex<KEY_TYPE, PAYLOAD_TYPE>::data_node_type *>(node);
    		u64 y_ptr = (u64)xalloc.cur();
			Alex<KEY_TYPE,PAYLOAD_TYPE>::address addr1;
			cur1->RDMA_Addr = y_ptr;
		    addr1.addr = y_ptr;
			addr1.data_capacity = cur1->data_capacity_;
			addr1.no = cur1 -> no;
			if(cur1->no == 2)
			{
				LOG(4)<<y_ptr;
			}
			auto tr = xalloc.cur();
		    memcpy(xalloc.cur(), &cur1->no, sizeof(int));
			xalloc.alloc(sizeof(int));

			memcpy(xalloc.cur(), &cur1->version,sizeof(int));
			xalloc.alloc(sizeof(int));

			for (int i = 0; i < cur1->data_capacity_; i++)
			{
				KEY_TYPE tmp1 = cur1->key_slots_[i];												           
				memcpy(xalloc.cur(), &tmp1, sizeof(KEY_TYPE));				
				xalloc.alloc(sizeof(KEY_TYPE));
			}

            for (int i = 0; i < cur1->data_capacity_; i++)
			{
				PAYLOAD_TYPE tmp2 = cur1->payload_slots_[i];
				memcpy(xalloc.cur(), &tmp2, sizeof(PAYLOAD_TYPE));
				xalloc.alloc(sizeof(PAYLOAD_TYPE));
			}

			addr1.sz = xalloc.cur() - tr;
			index1.ad.push_back(addr1);
			cur1->struct_no = index1.ad.size()-1;
		}
		myqueue.pop();
		if (myqueue.empty())
			break;
		node = myqueue.front();
	}
	index1.data_buf_cur = (char*)xalloc.cur();
	xalloc.alloc(250000000 * sizeof(int));
        RDMA_LOG(4)<<(char*)xalloc.cur() - index1.data_buf_start;
	index1.model_buf_start = (char *)xalloc.cur();//将整个模型放入RDMA区域
	model_buf = (u64)xalloc.cur();
        char *cur_ptr = (char *)xalloc.cur();
        index1.model_address.push_back(reinterpret_cast<u64>(index1.model_buf_start));   

	node = index1.root_node_;
	myqueue.push(node);
	while (1)
	{
		if (!node->is_leaf_)
		{
			cur = static_cast<Alex<KEY_TYPE, PAYLOAD_TYPE>::model_node_type *>(node);
		    index1.model_address.push_back((u64)xalloc.cur());
			memcpy(xalloc.cur(), &cur->no, sizeof(int));
			xalloc.alloc(sizeof(int));
			memcpy(xalloc.cur(), &cur->model_.a_, sizeof(double));
			xalloc.alloc(sizeof(double));
			memcpy(xalloc.cur(), &cur->model_.b_, sizeof(double));
			xalloc.alloc(sizeof(double));
			memcpy(xalloc.cur(), &cur->num_children_, sizeof(int));
			xalloc.alloc(sizeof(int));
			char* temp_addr = (char*)xalloc.cur();
			xalloc.alloc(sizeof(int));
			for (int i = 0; i < cur->num_children_; i++)
			{
				myqueue.push(cur->children_[i]);
				repeats = 1 << cur->children_[i]->duplication_factor_;
				memcpy(xalloc.cur(),&cur->children_[i]->no,sizeof(int));		
		       	xalloc.alloc(sizeof(int));					         
				memcpy(xalloc.cur(),&repeats,sizeof(int));		         
				xalloc.alloc(sizeof(int));
				i += repeats - 1;		
			}

			int length = ((char*)xalloc.cur() - temp_addr - sizeof(int)) / sizeof(int);
			memcpy(temp_addr,&length,sizeof(int));
		}
		else
		{
			cur1 = static_cast<Alex<KEY_TYPE, PAYLOAD_TYPE>::data_node_type *>(node);
			index1.model_address.push_back((u64)xalloc.cur());
			memcpy(xalloc.cur(), &cur1->no, sizeof(int));
			xalloc.alloc(sizeof(int));
			memcpy(xalloc.cur(), &cur1->model_.a_, sizeof(double));
			xalloc.alloc(sizeof(double));
			memcpy(xalloc.cur(), &cur1->model_.b_, sizeof(double));
			xalloc.alloc(sizeof(double));
			int y = 0;
			memcpy(xalloc.cur(), &y, sizeof(int));
			xalloc.alloc(sizeof(int));
			memcpy(xalloc.cur(), &cur1->version, sizeof(int));
			xalloc.alloc(sizeof(int));
			memcpy(xalloc.cur(), &cur1->min_error,sizeof(int));
			xalloc.alloc(sizeof(int));
			memcpy(xalloc.cur(), &cur1->max_error,sizeof(int));
			xalloc.alloc(sizeof(int)); 
		}
		myqueue.pop();
		if (myqueue.empty())
			break;
		node = myqueue.front();
	}
	RDMA_LOG(4)<<(char*)xalloc.cur() - index1.model_buf_start;
	xalloc.alloc(500000 * sizeof(int));
	index1.struct_buf_start = (char*)xalloc.cur();

	for(int i = 0; i < index1.ad.size(); i++)
	{
	  memcpy(xalloc.cur(),&index1.ad[i],sizeof(Alex<KEY_TYPE,PAYLOAD_TYPE>::address));
//	  LOG(2)<<"the size of ad is "<<index1.ad[i].sz;
	  xalloc.alloc(sizeof(Alex<KEY_TYPE,PAYLOAD_TYPE>::address));
	}
	RDMA_LOG(4)<<(char*)xalloc.cur() - index1.struct_buf_start;
	std::cout << "model buf is" << model_buf << std::endl;
	temp_sz = xalloc.cur() - cur_ptr;
}


int tt[40];

void meta_callback(const Header &rpc_header, const MemBlock &args,
                   UDTransport *replyc)
{
  ASSERT(args.sz == sizeof(u64));
  char reply_buf[64];
  struct t{
     u64 buf_ptr;
     int sz;
     int num_nodes;
  };
  memset(tt,0,sizeof(tt));
  t tt;
  tt.buf_ptr = model_buf;
  tt.sz = temp_sz;
  tt.num_nodes = index1.num_nodes();
  LOG(2)<<"a client is connecting!";
  std::cout<<"服务器模型缓冲区的地址为："<<model_buf<<std::endl;
  std::cout<<"服务器模型缓冲区的大小为："<<temp_sz<<std::endl;
  std::cout<<"索引节点总数为："<<tt.num_nodes<<std::endl;
  std::cout<<"buf size is "<<index1.buf_size<<std::endl;
  RPCOp op;
  ASSERT(op.set_msg(MemBlock(reply_buf, 64)).set_reply().add_arg(tt));
  op.set_corid(rpc_header.cor_id);
  auto ret = op.execute(replyc);

  ASSERT(ret == IOCode::Ok);
}

void get_callback(const Header &rpc_header, const MemBlock &args,
                   UDTransport *replyc){

  //ASSERT(args.sz == sizeof(KEY_TYPE));
  KEY_TYPE key = *(reinterpret_cast<KEY_TYPE *>(args.mem_ptr)); 
  char reply_buf[64];
  auto value = index1.get_payload(key);
  RPCOp op;
  if(value!=nullptr)
  {ASSERT(op.set_msg(MemBlock(reply_buf, 64)).set_reply().add_arg(*value));}
  else
  {ASSERT(op.set_msg(MemBlock(reply_buf, 64)).set_reply().add_arg(0));}
  op.set_corid(rpc_header.cor_id);
  auto ret = op.execute(replyc);
  ASSERT(ret == IOCode::Ok);
}
	          
using XThread = ::r2::Thread<usize>;
extern PBarrier *bar;
auto bootstrap_workers(const usize& nthreads) -> std::vector<std::unique_ptr<XThread>>
{
    std::vector<std::unique_ptr<XThread>> res;
    for(int i = 0; i < nthreads; ++i)
    {
        res.push_back(std::move(std::make_unique<XThread>([i]()->usize{
     
	       	auto mem_region = Region::create(64 * MB).value();
			//.VALUE表示如果包含值，则返回值的引用，否则返回异常
            auto mem = mem_region -> convert_to_rmem().value();

            auto nic_for_rec = RNic::create(RNicInfo::query_dev_names().at(0)).value();

            auto qp_recv = UD::create(nic_for_rec, QPConfig()).value();//每个线程创建一个用于接受的qp
            auto handler = RegHandler::create(mem, nic_for_rec).value();//在网卡上注册一块内存
            Allocator alloc(mem, handler->get_reg_attr().value());
	    	
			
			auto recv_rs_ar_recv = 
                RecvEntriesFactory<Allocator, 2048, 4096>::create(alloc);
            {
                auto res = qp_recv->post_recvs(*recv_rs_ar_recv, 2048);
                RDMA_ASSERT(res == IOCode::Ok);
            }
	    
            ctrl.registered_qps.reg("b" + std::to_string(i), qp_recv);
			//每一个服务器线程由b1、b2标记
            //reg the recv qp to the qp factory with key server i 
            LOG(4) << "server thread #" << i << "started!";
            
            RPCore<UDTransport, UDRecv<2048>, UDSessionManager<2048>> rpc(12);
            ASSERT(rpc.reg_callbacks(meta_callback) == 1);//拉模型
	    	ASSERT(rpc.reg_callbacks(get_callback) == 2);//服务器查找
	    	UDRecv<2048> recv(qp_recv, recv_rs_ar_recv);
	    	r2::compile_fence();
	   		bar->wait();
     	    while(!init) {
               r2::compile_fence();
            }


            usize epoches = 0;
            while (running) {
               r2::compile_fence();
               rpc.Recv_loop(&recv);
            }

      
            return 0;
        })));
    }
    return std::move(res);
}

PBarrier *bar;
int main(int argc, char * argv[])
{
   bar = new PBarrier(threads + 1);
  // auto mem = Region::create(64L * MB).value(); //待查
   auto all_nics = RNicInfo::query_dev_names();
   {
       for(int i = 0; i < all_nics.size(); ++i){
         auto nic = RNic::create(all_nics.at(i)).value();//create the nics
    // register the nic with name i to the ctrl
         RDMA_ASSERT(ctrl.opened_nics.reg(i, nic));
       }
       std::cout<<"allnic_size is "<<all_nics.size()<<std::endl;
   }
   


   {
      for(int i = 0; i < all_nics.size(); ++i){
       ctrl.registered_mrs.create_then_reg(
           i, mem->convert_to_rmem().value(), ctrl.opened_nics.query(i).value());
      }
   }

   auto workers = bootstrap_workers(threads);
   for(auto& w : workers) {
       w->start();
   }

   RDMA_LOG(2) << "YCSB bench server started!";
   //The server starts to listen
   ctrl.start_daemon();
   u64 total_sz = 64 *1024 *1024L;
  
   auto values = new std::pair<KEY_TYPE, PAYLOAD_TYPE>[nkeys];
  // std::vector<std::pair<KEY_TYPE, PAYLOAD_TYPE>> values;
  /* for(int i = 0 ; i < nkeys; i++)
   {
      values[i].first = i;
      values[i].second = i;
   }*/
   int num_keys = 10000000;
  if(!load_from_file)
  {
  // auto values = new std::pair<KEY_TYPE, PAYLOAD_TYPE>[nkeys];
   for(int i = 0 ; i < nkeys; i++)
   { 
     values[i].first = i;
     values[i].second = i;
   }
}else{
	r2::Timer tt;
     std::ifstream file(data_file);//读取OSM数据集
      std::string line;
      KEY_TYPE ret;num_keys = 0 ;

      for(int i = 0; i < nkeys; ++i)
      {
         std::getline(file, line);
         std::istringstream iss(line);
         iss >> ret;
		 values[num_keys].first = ret;
         values[num_keys].second = ret;
		 num_keys++;

       } 
	   LOG(2)<<"The passed msec is "<<tt.passed_msec()<<" msec";
      /*
	  
      auto keys = new KEY_TYPE[nkeys];//读取其他数据集
      load_binary_data(keys, nkeys,keys_file_path);
      LOG(2)<<"The passed msec is "<<tt.passed_msec()<<" msec";
      for(int i = 0; i < nkeys; i++)
      {
	      values[i].first = keys[i];
	      values[i].second = keys[i];
      }
      for(int i = 0; i < 10; i++) {
	      std::cout<<values[i].first<<std::endl;
	} //
*/

      std::sort(values, values + num_keys,
		                  [](auto const& a, auto const& b) { return a.first < b.first; });

}	
r2::Timer t;
index1.bulk_load(values, num_keys);
delete[] values;
LOG(2)<<"The passed msec "<<t.passed_msec()<<" msec";
index1.collect_statics_info(index1.root_node_);
long long int size = index1.model_size() + index1.data_size();
ASSERT(mem->size > size) << "ALEX sz needed: "<<size;
Set_RDMA_region(xalloc);


 
   std::cout<<"Server index work done!"<<std::endl;
   r2::compile_fence(); 
   init = true;
   bar->wait();

   if(threads > 0)
   {
       for(int i = 0; i < 250; i++)
       {
           sleep(1);
       }
   }
   
   r2::compile_fence;
   running = false;
   std::cout<<"wake"<<std::endl;
   for(auto& w : workers){
       w->join();
   }
   std::cout<<"Server Exit!"<<std::endl;
}
