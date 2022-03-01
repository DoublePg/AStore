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
std::string data_file = "osm_uni_10m.txt";
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
alex::Alex<KEY_TYPE,PAYLOAD_TYPE>/*,AlexCompare,allocate>*/ index1;
int temp_sz;

//std::vector<char*> model_address;




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

void struct_callback(const Header &rpc_header, const MemBlock &args,
		                   UDTransport *replyc)
{
	ASSERT(args.sz == sizeof(int));
	char reply_buf[64];
	int struct_no = *(reinterpret_cast<int *>(args.mem_ptr));
	struct adt
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
	adt ad1;
	//std::cout<<"Struct no is "<<struct_no<<std::endl;
	
	ad1.no = index1.ad[struct_no].no;
	//std::cout<<"no is "<<ad1.no<<std::endl;
	ad1.sz = index1.ad[struct_no].sz;
	//LOG(2)<<"ad1.sz is "<<ad1.sz;
	ad1.data_capacity = index1.ad[struct_no].data_capacity;
	ad1.addr = index1.ad[struct_no].addr;
        auto cur = static_cast<Alex<KEY_TYPE, PAYLOAD_TYPE>::data_node_type *>(index1.node_index[ad1.no]);
	ad1.pulled_version = cur->version;
	ad1.a = cur->model_.a_;
	ad1.b = cur->model_.b_;
	ad1.min_error = cur->min_error;
	ad1.max_error = cur->max_error;

	RPCOp op;
	ASSERT(op.set_msg(MemBlock(reply_buf, 64)).set_reply().add_arg(ad1));
	op.set_corid(rpc_header.cor_id);
	auto ret = op.execute(replyc);
	ASSERT(ret == IOCode::Ok);
}
void model_callback(const Header &rpc_header, const MemBlock &args,	
		UDTransport *replyc)
{
	ASSERT(args.sz == sizeof(int));
	char reply_buf[64];
	struct model_info
	{
		int model_size;
		int num_nodes;	
	    	int white_size;
	};
	model_info modelinfo;
	modelinfo.model_size = index1.model_region_size;
	modelinfo.num_nodes = index1.num_nodes();
        modelinfo.white_size = index1.white_size;
	//std::cout<<(u64)index1.model_buf_start<<std::endl;
	RPCOp op;
     	ASSERT(op.set_msg(MemBlock(reply_buf, 64)).set_reply().add_arg(modelinfo));
	op.set_corid(rpc_header.cor_id);
	auto ret = op.execute(replyc);
	ASSERT(ret == IOCode::Ok);

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
void insert_callback(const Header &rpc_header, const MemBlock &args,
		                     UDTransport *replyc)
{
        struct insert_el
        {
          KEY_TYPE key;
	  PAYLOAD_TYPE value;
          int no;
	  int parent_no;
          int version;
	  int struct_no;
	  u64 addr;
        };
	bool insert_succ = false;
	int node_index;
	ASSERT(args.sz == sizeof(insert_el));
	insert_el insert_element = *(reinterpret_cast<insert_el*>(args.mem_ptr));
    
	node_index = index1.ad[insert_element.struct_no].no;
    // int tmp_version;
    // char* tmp_addr = (char*)insert_element.addr + sizeof(int);
    //     memcpy(&tmp_version,tmp_addr,sizeof(int));	
    int tp_no;
	int tp_version;
	memcpy(&tp_no,(char*)insert_element.addr,sizeof(int));
    memcpy(&tp_version,(char*)insert_element.addr + sizeof(int),sizeof(int));
	tt[tp_no]++;
	if(/*insert_element.version == index1.node_index[node_index]->version &&*/ insert_element.version == tp_version&&tp_no == insert_element.no)
	{
	   Alex<KEY_TYPE,PAYLOAD_TYPE>::data_node_type* cur = static_cast<Alex<KEY_TYPE, PAYLOAD_TYPE>::data_node_type *>(index1.node_index[tp_no]);

	   if(cur->buf_ptr < index1.buf_size)//直接写入叶子缓冲区和RDMA区域中的缓冲区
	   {	
	      cur->key_buf[cur->buf_ptr] = insert_element.key;
	      cur->payload_buf[cur->buf_ptr] = insert_element.value;
	      cur->buf_ptr++; 
	      insert_succ = true;	 
	   }
           else{ 
		   int capacity;
		   if(insert_element.no != tp_no)
		   LOG(2)<<insert_element.no<<" "<<tp_no;
		   Alex<KEY_TYPE, PAYLOAD_TYPE>::data_node_type *cur = static_cast<Alex<KEY_TYPE, PAYLOAD_TYPE>::data_node_type *>(index1.node_index[insert_element.no]);
		   char* RDMA_Addr = reinterpret_cast<char *>(insert_element.addr);
		   char* cur_ptr = RDMA_Addr;

		   cur_ptr += sizeof(int) * 2;
		   int tp;
		   char* RDMA_Key_Addr;
		   char* RDMA_Payload_Addr;
		   RDMA_Key_Addr = cur_ptr;
		   capacity = cur->data_capacity_;
		   //cur_ptr += sizeof(int) * (index1.buf_size + capacity);
	           cur_ptr += sizeof(int) * capacity;
		   RDMA_Payload_Addr = cur_ptr;	    

			   r2::Timer t;
		   bool is_change = index1.insert_buf(insert_element.no, insert_element.parent_no, RDMA_Key_Addr, RDMA_Payload_Addr,reinterpret_cast<char*>(index1.model_address[insert_element.no]),insert_element.struct_no);
		//    char* ry = (char*)index1.ad[864].addr;
        //          int oo;
        //          memcpy(&oo,ry,sizeof(int));
        //          LOG(2)<<"no is "<<oo;
        //       memcpy(&oo,ry +sizeof(int),sizeof(int));
        //       LOG(2)<<"version is "<<oo;
		  // LOG(2)<<"The full time of insert_buf is "<<t.passed_msec()<<" msec";
		   /*if(insert_element.no == 1368)
		   {
			  char* cur_ptr1 = reinterpret_cast<char *>(index1.ad[insert_element.struct_no].addr);
			  cur_ptr1 += 2 * sizeof(int) + 500 * sizeof(u64);
			  u64 tutu;
			  for(int ii = 0 ;ii < 300; ii++)
			  {
                            memcpy(&tutu,cur_ptr1,sizeof(u64));
			    cur_ptr1 += sizeof(u64);
			    LOG(4)<<tutu;
			  }
			  Alex<KEY_TYPE, PAYLOAD_TYPE>::data_node_type *cur = static_cast<Alex<KEY_TYPE, PAYLOAD_TYPE>::data_node_type *>(index1.node_index[insert_element.no]);
		   }*/
		  cur = static_cast<Alex<KEY_TYPE, PAYLOAD_TYPE>::data_node_type *>(index1.node_index[insert_element.no]);


//		    LOG(2)<<"==============================================================================="<<insert_element.key;

		    index1.insert_different_version(insert_element.key,insert_element.value);
		   insert_succ = true;

		   /*if(cur->no == 16168)
		   {
			   char* cur_ptr1 = reinterpret_cast<char *>(index1.ad[insert_element.struct_no].addr);
			   cur_ptr1 += 2 * sizeof(int);
			   int tmp;
			   for (int i = 0; i < cur->data_capacity_; i++)
			   {
				   memcpy(&tmp,cur_ptr1,sizeof(int));
		                   if(tmp!=cur->key_slots_[i])
				   {std::cout<<"Key_slots "<<i<<" is "<<cur->key_slots_[i]<<std::endl;
				   std::cout<<i<<" is "<<tmp<<std::endl;}
				   cur_ptr1 += sizeof(int);
			   }
			   cur_ptr1 += sizeof(int) * 10;
			   std::cout<<std::endl;
			   for (int i = 0; i < cur->data_capacity_; i++)
			   {
			   memcpy(&tmp,cur_ptr1,sizeof(int));
			   if(tmp!=cur->payload_slots_[i])
			   {
				   std::cout<<"Payload_slots "<<i<<" is "<<cur->payload_slots_[i]<<std::endl;

					   std::cout<<i<<" is "<<tmp<<std::endl;

			     }		   cur_ptr1 += sizeof(int);
			   }
		   }*///	LOG(4)<<index1.node_index[2]->version;
	   }	   
	}
	else
	{
		/*std::cout<<"insert element version is "<<insert_element.version<<std::endl; 
		std::cout<<"no is "<<insert_element.no<<std::endl;
		std::cout<<"The index version is "<<index1.node_index[insert_element.no]->version<<std::endl;
	      *///  ASSERT(false);	
		 LOG(2)<<"inserrrrrrrrrrrrrrrrrrrrt";
		 //LOG(2)<<insert_element.key;
		index1.insert_different_version(insert_element.key,insert_element.value);
	}
	insert_succ = true;
    char reply_buf[64];
	RPCOp op;
	ASSERT(op.set_msg(MemBlock(reply_buf, 64)).set_reply().add_arg<bool>(insert_succ));//index1.insert(insert_element.key,insert_element.key).second));
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
  //LOG(2)<<value;
  //LOG(2)<<"get_callback";
  RPCOp op;
  if(value!=nullptr)
  {ASSERT(op.set_msg(MemBlock(reply_buf, 64)).set_reply().add_arg(*value));}
  else
  {ASSERT(op.set_msg(MemBlock(reply_buf, 64)).set_reply().add_arg(0));}
  op.set_corid(rpc_header.cor_id);
  auto ret = op.execute(replyc);
  ASSERT(ret == IOCode::Ok);
}
void search_callback(const Header &rpc_header, const MemBlock &args,
		                   UDTransport *replyc){
  struct search_struct
  {
     KEY_TYPE key;
     //int no;
	 u64 addr;
  };
  ASSERT(args.sz == sizeof(search_struct));
  search_struct ti = *(reinterpret_cast<search_struct*>(args.mem_ptr));
  char reply_buf[64];
  KEY_TYPE key = ti.key;
  u64 addr = ti.addr;
  //int no = ti.no;
  //int no = *(reinterpret_cast<int*>(args.mem_ptr + sizeof(KEY_TYPE)));
  PAYLOAD_TYPE tp = 0;
  int no;
  char *temp_addr = (char*)ti.addr;
  memcpy(&no,temp_addr,sizeof(int));
  //LOG(2)<<no;
  //Alex<KEY_TYPE, PAYLOAD_TYPE>::data_node_type *cur = static_cast<Alex<KEY_TYPE, PAYLOAD_TYPE>::data_node_type *>(index1.node_index[no]);
  Alex<KEY_TYPE, PAYLOAD_TYPE>::data_node_type *cur = index1.get_leaf(key);
  //LOG(2)<<key<<" "<<no;
  for(int i = 0; i < cur->buf_ptr; i++)
  {
	  if(key == cur->key_buf[i])
	  { 
		  tp = cur->payload_buf[i];
		  break;
	  }		  
  }
  if(tp == 0)
  {
	  if(index1.get_payload(key) != nullptr)
	  {
		  tp = *index1.get_payload(key);	 
	  }
	  else
	  {
		  /*LOG(2)<<"not found!"<<" "<<key<<" "<<no;
		  for(int i = 0; i < cur->buf_ptr; i++)
		  {
			  LOG(2)<<i<<" "<<cur->key_buf[i];
		  }
		  cur = static_cast<Alex<KEY_TYPE, PAYLOAD_TYPE>::data_node_type *>(index1.node_index[no-1]);
		  for(int i = 0; i < cur->buf_ptr; i++)
		  {
			  LOG(2)<<i<<" "<<cur->key_buf[i];
		  }
		   Alex<KEY_TYPE, PAYLOAD_TYPE>::data_node_type *leaf = index1.get_leaf(key);
		   LOG(2)<<"leaf no is "<<leaf->no;*/
		  tp = 0;
	  }
  }
  if(key ==  22873570586116)
  LOG(2)<<cur->buf_ptr;
  /*for(int i = 0; i < cur->buf_ptr; i++)
  {
	  LOG(2)<<cur->key_buf[i];
  }*/


  //LOG(2)<<tp;
  RPCOp op;
  ASSERT(op.set_msg(MemBlock(reply_buf, 64)).set_reply().add_arg(tp));
  op.set_corid(rpc_header.cor_id);
  auto ret = op.execute(replyc);
  ASSERT(ret == IOCode::Ok);
}
void update_callback(const Header &rpc_header, const MemBlock &args,
		UDTransport *replyc){
          struct __attribute__((packed)) UpdateData{
          KEY_TYPE key;
	  PAYLOAD_TYPE payload;
	  int no;
	  int version;
	  };
	  char reply_buf[64];
          ASSERT(args.sz == sizeof(UpdateData));
	  UpdateData data = *(reinterpret_cast<UpdateData*>(args.mem_ptr));
	  bool succeed_flag = false;
	  //Alex<KEY_TYPE, PAYLOAD_TYPE>::data_node_type *cur = static_cast<Alex<KEY_TYPE, PAYLOAD_TYPE>::data_node_type *>(index1.node_index[data.no]);
	  //if(cur->version != data.version)
	  /*{
	      succeed_flag = index1.update_payload(data.key,data.payload);	 
	  } */ 
	 /* else//warning: 当客户端版本和服务器恰好一致
	  {*/
	     
             succeed_flag = index1.update_payload_with_no(data.key,data.payload,data.no);
//	     LOG(2)<<"passed msec is "<<t.passed_msec();
	     /*if(succeed_flag == false)
	     {
		     ASSERT(false);
		     succeed_flag = index1.update_payload(data.key,data.payload);
	     }
	  }*/
	  RPCOp op;
	  ASSERT(op.set_msg(MemBlock(reply_buf, 64)).set_reply().add_arg(succeed_flag));
      	  op.set_corid(rpc_header.cor_id);
	  auto ret = op.execute(replyc);
	  ASSERT(ret == IOCode::Ok);
}
void update_error_callback(const Header &rpc_header, const MemBlock &args,
		                   UDTransport *replyc){
//	  ASSERT(args.sz == sizeof(int));	    
//	  int no = *(reinterpret_cast<int *>(args.mem_ptr));
	  ASSERT(args.sz == sizeof(u64));
	  int no;
	  u64 addr = *(reinterpret_cast<u64*>(args.mem_ptr));
	  char* cur_ptr = (char*)addr;
	  memcpy(&no,cur_ptr,sizeof(int));
          
	  //LOG(4)<<"no is "<<no;
	  auto cur = static_cast<Alex<KEY_TYPE, PAYLOAD_TYPE>::data_node_type *>(index1.node_index[no]);
	  char reply_buf[64];
	  RPCOp op;
	  struct err_info{
		  double a;
		  double b;
		  int min_error;
		  int max_error;
		  bool is_expand;
	  };
	  err_info err1;
	  err1.a = cur->model_.a_;
	  err1.b = cur->model_.b_;
	  /*if(no == 724)
	  {
		  for(int i = 0 ; i < index1.node_index.size();i++)
		  {auto cur1 = static_cast<Alex<KEY_TYPE, PAYLOAD_TYPE>::data_node_type *>(index1.node_index[i]);
			  if(cur1->model_.a_ == 0.00539981)
				  LOG(2)<<cur1->no;
		  }ASSERT(false);
		  //LOG(2)<<cur1->model_.a_;
		  //LOG(2)<<cur1->model_.b_;
	  }*/
	  if(index1.expand_flag == true)
	  {
		  index1.expand_flag = false;
		  err1.is_expand = true;
	  }else
	  {
		  err1.is_expand = false;
	  }
	  err1.min_error = cur->min_error;
	  err1.max_error = cur->max_error;
	  ASSERT(op.set_msg(MemBlock(reply_buf, 64)).set_reply().add_arg(err1));
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
	    	ASSERT(rpc.reg_callbacks(insert_callback) == 3);//插入
            ASSERT(rpc.reg_callbacks(struct_callback) == 4);//客户端拉结构
            ASSERT(rpc.reg_callbacks(model_callback) == 5);//客户端拉模型
	    	ASSERT(rpc.reg_callbacks(search_callback) == 6);//buf中寻找kv
	   	 	ASSERT(rpc.reg_callbacks(update_error_callback) == 7);//更新客户端最大最小误差
	   		ASSERT(rpc.reg_callbacks(update_callback)==8);//更新payload
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
    	// if(ret <= 2000000000)
		 //{
			 values[num_keys].first = ret;
         	 values[num_keys].second = ret;
			 num_keys++;
		 //}
		 //LOG(2) <<"values.size is "<<values.size();
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
      for(int i = 0; i < 10; i++)
      {
        std::cout<<values[i].first<<std::endl;
	}
}	
r2::Timer t;
index1.bulk_load(values, num_keys);
delete[] values;
LOG(2)<<"The passed msec "<<t.passed_msec()<<" msec";
index1.collect_statics_info(index1.root_node_);
long long int size = index1.model_size() + index1.data_size();
ASSERT(mem->size > size) << "ALEX sz needed: "<<size;
//allocate xalloc((char*)mem->start_ptr(), size);
Set_RDMA_region(xalloc);
//std::cout<<"The num of nodes is " <<index1.num_nodes()<<std::endl;
//std::cout<<"The num of node_index is "<<index1.node_index.size()<<std::endl;
/*for(Alex<int,int>::NodeIterator node_it(&index1); !node_it.is_end(); node_it.next())
{
  if(node_it.current()->is_leaf_)
  {
         alex::AlexDataNode<int,int> *c = static_cast<Alex<int,int>::data_node_type*>(node_it.current());
         memcpy(xalloc.cur(),c->key_slots_,sizeof(int)*c->data_capacity_);
         xalloc.alloc(sizeof(int)* c->data_capacity_);
         memcpy(xalloc.cur(),c->payload_slots_,sizeof(int)*c->data_capacity_);
         xalloc.alloc(sizeof(int)* c->data_capacity_);
         memcpy(xalloc.cur(),c->bitmap_,c->bitmap_size_ * sizeof(uint64_t));
         xalloc.alloc(c->bitmap_size_*sizeof(uint64_t));
         memcpy(xalloc.cur(),&c->model_.a_,sizeof(double));
	 xalloc.alloc(sizeof(double)); 
	 memcpy(xalloc.cur(),&c->model_.b_,sizeof(double));
         xalloc.alloc(sizeof(double));
         if(p < 10)
	 {
	    auto temp_ptr = xalloc.cur()-sizeof(double);
	    double aa,bb;
	    memcpy(&bb,temp_ptr,sizeof(double));
	    temp_ptr -= sizeof(double);
	    memcpy(&aa,temp_ptr,sizeof(double));
	    std::cout<<"当前模型的a为："<<aa<<"; 当前模型的b为: "<<bb<<std::endl;
	    std::cout<<"模型实际的a为: "<<c->model_.a_<<"; 模型实际的b为: "<<c->model_.b_<<std::endl;
	    p++;
	 }

  } 
}*/

 /* 
   index1.set_no(index1.root_node_);
   Alex<int,int>::data_node_type* leaf = index1.get_leaf(245169489);
   std::cout<<"The leaf num is: " <<leaf->no<<std::endl;
   std::cout<<"The capa is: "<<leaf->data_capacity_<<std::endl;
   for(int i = 0; i < leaf -> data_capacity_; i++)
   {
   std::cout<<"The key is"<<leaf->key_slots_[i]<<std::endl;
   std::cout<<"The Payload is"<<leaf->payload_slots_[i]<<std::endl;
   }
   auto node = index1.root_node_;
   //alex::AlexModelNode<int,int> *cur = static_cast<Alex<int,int>::model_node_type*>(index1.root_node_);
   std::queue<alex::AlexNode<int,int>*> myqueue;
   alex::AlexModelNode<int,int>*cur;
   alex::AlexDataNode<int,int>* cur1;
   int repeats;
   myqueue.push(node);
while(1)
{
  if(!node->is_leaf_)
  {
    cur = static_cast<Alex<int,int>::model_node_type*>(node);
    for(int i = 0; i < cur->num_children_; i++)
    {
      myqueue.push(cur->children_[i]);
      repeats = 1 << cur->children_[i]->duplication_factor_;
      i += repeats - 1;
    }
  }
  else
  {
    cur1 = static_cast<Alex<int,int>::data_node_type*>(node);
    u64 y_ptr = (u64)xalloc.cur();
    ad[jj].addr = y_ptr;
    ad[jj].data_capacity = cur1->data_capacity_;
    ad[jj].no = cur1->no;
    auto tr = xalloc.cur();
    memcpy(xalloc.cur(),&cur1->data_capacity_,sizeof(int));
    xalloc.alloc(sizeof(int));
    memcpy(xalloc.cur(),&cur1->version,sizeof(int));
    xalloc.alloc(sizeof(int));
    int buf_element = -1;
    for(int i = 0; i < cur1->data_capacity_;i++)
    {
	int tmp1 = cur1->key_slots_[i];
	memcpy(xalloc.cur(),&tmp1, sizeof(int));
	xalloc.alloc(sizeof(int));
    }
    for(int i = 0; i < 10; i++)
    {
	memcpy(xalloc.cur(),&buf_element,sizeof(int));
	xalloc.alloc(sizeof(int));
    }
    for(int i = 0; i < cur1 -> data_capacity_; i++)
    {
	   int tmp2 = cur1->payload_slots_[i];
	    memcpy(xalloc.cur(),&tmp2, sizeof(int));
	    xalloc.alloc(sizeof(int));
    }
    for(int i = 0; i < 10; i++)
    {
       memcpy(xalloc.cur(),&buf_element,sizeof(int));
       xalloc.alloc(sizeof(int));
    }
   for(int i = 0; i < cur1 -> bitmap_size_;i++)
   {
       uint64_t tmp3 = cur1->bitmap_[i];
       memcpy(xalloc.cur(),&tmp3,sizeof(uint64_t));
       xalloc.alloc(sizeof(uint64_t));
   }
   ad[jj].sz = xalloc.cur() - tr;
   jj++;
  }
  myqueue.pop();
  if(myqueue.empty()) break;
  node = myqueue.front();
}
//把模型放到RDMA缓冲区
node = index1.root_node_;
myqueue.push(node);
model_buf = (u64)xalloc.cur();
char* cur_ptr = (char*)xalloc.cur();
while(1)
   {
      if(!node->is_leaf_)       
      {
          cur = static_cast<Alex<int,int>::model_node_type*>(node);
          memcpy(xalloc.cur(),&cur->no, sizeof(int));
	  xalloc.alloc(sizeof(int));
	  memcpy(xalloc.cur(),&cur->model_.a_,sizeof(double));
	  xalloc.alloc(sizeof(double)); 
	  memcpy(xalloc.cur(),&cur->model_.b_,sizeof(double)); 
          xalloc.alloc(sizeof(double));  
         // int y = cur->children_[cur->num_children_ - 1]->no - cur->children_[0]->no + 1;
         // memcpy(xalloc.cur(),&y,sizeof(int));
	  memcpy(xalloc.cur(),&cur->num_children_,sizeof(int));
	  xalloc.alloc(sizeof(int));
          for(int i = 0; i < cur->num_children_;i++)
	  {
            //memcpy(xalloc.cur(), &cur->children_[i]->no, sizeof(int));																		
	    //xalloc.alloc(sizeof(int));
	    myqueue.push(cur->children_[i]);	
	    repeats = 1 << cur->children_[i]->duplication_factor_;
	    for(int j = 0; j < repeats; j++)
	    {
	       memcpy(xalloc.cur(), &cur->children_[i]->no, sizeof(int));
	       xalloc.alloc(sizeof(int));
	    }
	    i += repeats - 1;
	  }
      } 
      else{ 
	      cur1 = static_cast<Alex<int,int>::data_node_type*>(node);
	      memcpy(xalloc.cur(), &cur1->no, sizeof(int));
	      xalloc.alloc(sizeof(int));	
 	      memcpy(xalloc.cur(),&cur1->model_.a_,sizeof(double));
	      xalloc.alloc(sizeof(double)); 
              memcpy(xalloc.cur(),&cur1->model_.b_,sizeof(double)); 
	      xalloc.alloc(sizeof(double));
	      int y = 0;
	      memcpy(xalloc.cur(), &y, sizeof(int));
	      xalloc.alloc(sizeof(int));  
              memcpy(xalloc.cur(), &cur1->version, sizeof(int));
	      xalloc.alloc(sizeof(int));
      }    
      myqueue.pop();
      if(myqueue.empty()) break;
      node = myqueue.front();
}
memcpy(xalloc.cur(),ad,sizeof(address)*jj);
xalloc.alloc(sizeof(address)*jj);
std::cout<<"model buf is"<<model_buf<<std::endl;
int d;
d =xalloc.cur()-cur_ptr;
temp_sz = d;
//model_buf = (char*)mem->start_ptr();
//model_buf = cur_ptr;
//std::cout<<xalloc.cur() -(char*)mem->start_ptr()<<std::endl;
//node = index1.root_node_;
/*struct address{
	int no;
	u64 addr;
};
int jj = 0;
address ad[20000];
myqueue.push(node);
while(1)
{
  if(!node->is_leaf_)
  {
    cur = static_cast<Alex<int,int>::model_node_type*>(node);
    for(int i = 0; i < cur->num_children_; i++)
    {
	myqueue.push(cur->children_[i]);
	repeats = 1 << cur->children_[i]->duplication_factor_;
	i += repeats - 1;
    }
  }
  else
  { 
    cur1 = static_cast<Alex<int,int>::data_node_type*>(node);
     u64 y_ptr = (u64)xalloc.cur();
     ad[jj].addr = y_ptr;
     ad[jj].no = cur1->no;
     jj++;
   // memcpy(xalloc.cur(),&cur1->no,sizeof(int));
   // xalloc.alloc(sizeof(int));
    memcpy(xalloc.cur(),&cur1->data_capacity_,sizeof(int));
    xalloc.alloc(sizeof(int));
   // memcpy(xalloc.cur(),&y_ptr,sizeof(u64));
   // xalloc.alloc(sizeof(u64));
    memcpy(xalloc.cur(),&cur1->key_slots_, cur1->data_capacity_ * sizeof(int));
    xalloc.alloc(sizeof(int)*cur1->data_capacity_); 
    memcpy(xalloc.cur(),&cur1->payload_slots_, cur1->data_capacity_ * sizeof(int));
    xalloc.alloc(sizeof(int)*cur1->data_capacity_);
    memcpy(xalloc.cur(),&cur1->bitmap_, cur1->bitmap_size_ * sizeof(uint64_t));
    xalloc.alloc(sizeof(uint64_t) * cur1->bitmap_size_);
  }
  myqueue.pop();
  if(myqueue.empty()) break;
  node = myqueue.front();
}
memcpy(xalloc.cur(),ad,sizeof(address)*jj);
xalloc.alloc(sizeof(address)*jj);
*/
/*char* cur_ptr1 =(char*) xalloc.cur()- jj *sizeof(address);
address to ;
memcpy(&to,cur_ptr1,sizeof(address));
std::cout<<to.no<<" address "<<to.addr<<std::endl;
cur_ptr1+= sizeof(address);
memcpy(&to,cur_ptr1,sizeof(address));
std::cout<<to.no<<" address "<<to.addr<<std::endl;
std::cout<<"ppp"<<ad[0].no<<std::endl;
std::cout<<"jj is "<<jj<<std::endl;
std::cout<<"data_node size if"<<yq<<std::endl;
std::cout<<xalloc.cur() -(char*)mem->start_ptr()<<std::endl;  
*//*  int d
  char *cur_ptr =(char*) mem->start_ptr();
  d =xalloc.cur()-cur_ptr;
  temp_sz = d;
  model_buf = (u64)mem->start_ptr();
  std::cout<<"整个模型所占用的空间为: "<<d<<std::endl;
  for(int i = 0;i<60; i++)
{
     	memcpy(&d,cur_ptr,sizeof(int));
  ur_ptr += sizeof(int);
  std::cout<<"当前缓冲区中的no是："<<d<<std::endl;
    
  memcpy(&a,cur_ptr,sizeof(double));
  std::cout<<"当前缓冲区中的a是："<<a<<std::endl;
 // std::cout<<"实际的a是: "<<static_cast<Alex<int,int>::model_node_type*>(index1.root_node_)->model_.a_<<std::endl;
  cur_ptr += sizeof(double);
  memcpy(&a,cur_ptr,sizeof(double));
  std::cout<<"当前缓冲区中的b是："<<a<<std::endl;
 // std::cout<<"实际的b是: "<<static_cast<Alex<int,int>::model_node_type*>(index1.root_node_)->model_.b_<<std::endl;
  cur_ptr += sizeof(double);
  memcpy(&d,cur_ptr,sizeof(int));
  std::cout<<"当前缓冲区中的num_children是："<<d<<std::endl;
  cur_ptr+=sizeof(int);
  /*for(int k = 0; k < 100 ;k++)
{
  memcpy(&bl,cur_ptr+k*sizeof(int),sizeof(int));
  std::cout<<"当前子节点的no是："<<bl<<std::endl;
}*/
/*  cur_ptr+=sizeof(int)*d;
 // memcpy(&d,cur_ptr,sizeof(int));
 } *//*std::cout<<"当前页的no为: "<<d<<std::endl;
  cur_ptr+=sizeof(int);
  memcpy(&a,cur_ptr,sizeof(double));
  std::cout<<"当前页的a为: "<<a<<std::endl;
  std::cout<<"实际的a是: "<<static_cast<Alex<int,int>::model_node_type*>(index1.root_node_)->children_[0]->model_.a_<<std::endl;
   */
  /*for(int i = 0; i < 100 ; i++)
 {
   cur_ptr+=sizeof(int);
   memcpy(&d,cur_ptr,sizeof(int));
   //cur_ptr+=sizeof(int);
   std::cout<<"子节点的no为: "<<d<<std::endl;
 }*/	
  
  /* auto cur1 = static_cast<Alex<int,int>::model_node_type*>(index1.root_node_);
   std::cout<<cur1->level_<<std::endl;
   std::cout<<cur1->num_children_<<std::endl;
   std::cout<<cur1->no<<std::endl;
   for(int i = 0; i < cur1->num_children_; i++)
   {
     if(cur1->children_[i]->is_leaf_&&i<=100)
     {int j = 1<<cur1->children_[i]->duplication_factor_;
     std::cout<<"i是: "<<i<<"当前节点号为: "<<cur1->children_[i]->no<<" 复制因子为: "<<j<<std::endl;
     std::cout<<cur1->children_[i]->level_<<"  "<<std::boolalpha<<cur1->children_[i]->is_leaf_<<std::endl;
     }
     if(i == cur1->num_children_ - 1)
     {
	     std::cout<<"123  "<<cur1->children_[i]->no<<std::endl;
     }
   }
  
   std::cout<<"当前索引的节点总数为: "<<index1.num_nodes()<<std::endl;
   td::cout<<"当前叶子节点数量为: "<<index1.num_leaves()<<std::endl;
   std::cout<<"内存分配量为: "<<xalloc.cur()-xalloc.mem() << "实际占用量为: "<<index1.data_size() <<std::endl;
*/
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
