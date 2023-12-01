


#include "Utils/AsyncScheduler.h"





AsyncScheduler::AsyncScheduler(){
    io_uring_queue_init(IO_URING_QUEUE_DEPTH,
    &ring_,IORING_FEAT_NODROP);
}


void AsyncScheduler::execute(uint32_t block_thread_num){
    uring_poll_thread_ = std::thread([this](){
        struct io_uring_cqe* cqe;
        while(true){
            std::list<std::coroutine_handle<>> finished_handle_;
            while(true){
                finished_handle_.clear();
                int ret = io_uring_wait_cqe(&ring_,&cqe);
                if(ret <0)
                    break;;
                assert(cqe->res > 0); 
                auto* handle = (std::coroutine_handle<>*)cqe->user_data;
                finished_handle_.push_back(*handle);
            }
            g_scheduler.add_task(std::move(finished_handle_));
        }
    });

    for(uint32_t i=0;i<block_thread_num;++i){
        working_thread_.emplace_back([this](){
            while(true){
                std::unique_lock<std::mutex> lock(internal_mtx_);
                cv_.wait(lock,[this](){
                    return job_list_.size() != 0;
                });
                auto func = std::move(job_list_.front());
                job_list_.pop_front();
                lock.unlock();
                func();
            }
        });
    }

}