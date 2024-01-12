


#include "Utils/AsyncScheduler.h"
#include "Utils/Scheduler.h"
#include<cassert>
#include <ctime>
#include <liburing.h>




AsyncScheduler::AsyncScheduler(Scheduler& scheduler):scheduler_(scheduler){
    io_uring_queue_init(IO_URING_QUEUE_DEPTH,
    &ring_,IORING_FEAT_NODROP);
    execute();
}


void AsyncScheduler::execute(uint32_t block_thread_num){
    uring_poll_thread_ = std::thread([this](){
        struct io_uring_cqe* cqe;
        std::list<std::coroutine_handle<>> finished_handle_;
        __kernel_timespec time;
        time.tv_sec = 0;
        time.tv_nsec = 0;
        while(!finished_){
            finished_handle_.clear();
            while(true){
                int ret = io_uring_wait_cqe_timeout(&ring_,&cqe,&time);
                if(ret <0)
                    break;;
                
                auto* awaitable = (__async_awaitable_base*)cqe->user_data;
                awaitable->return_value_ = cqe->res;
                finished_handle_.push_back(awaitable->handle_);
                io_uring_cqe_seen(&ring_, cqe);
            }
            scheduler_.add_task(std::move(finished_handle_));
        }
    });
}