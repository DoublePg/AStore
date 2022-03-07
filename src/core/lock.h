#pragma once

#include "./atomic.hh"

#include "../../deps/r2/src/common.hh"



using namespace r2;

struct CompactSpinLock {
	  volatile u16 guard = 0;

	    CompactSpinLock() = default;

	      inline void lock() {
		          while (1) {
				        if (!xchg16((u16 *)(&guard), 1))
						        return;

					      while (guard) {
						              ::r2::relax_fence();
							            }
					          }
			    }

	        inline void unlock() {
			    r2::compile_fence();
			        this->guard = 0;
				  }

		  inline u16 try_lock() { return xchg16((u16 *)(&guard), 1); }

		    inline u16 is_locked() const { return guard; }
};

