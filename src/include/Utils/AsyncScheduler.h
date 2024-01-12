#pragma once
#include "Logger.h"
#include"liburing.h"
#include <atomic>
#include <cstdint>
#include<thread>
#include<vector>
#include<coroutine>
#include<list>
#include<functional>
#include"Scheduler.h"
#include "ThreadPool.h"



class AsyncScheduler{

    using BlockFunction = std::function<void()>;
    static constexpr uint32_t IO_URING_QUEUE_DEPTH = 32768;
    struct io_uring ring_;
    std::thread uring_poll_thread_;
    std::vector<std::thread> working_thread_;
    std::list<BlockFunction> job_list_;
    std::mutex internal_mtx_;
    std::condition_variable cv_;
    
    ThreadPool thread_pool_{10};
    

    Scheduler& scheduler_;


    std::atomic<bool> finished_ = false;

    template<bool resume_in_bk_thread>
    struct async_operation{

        bool await_ready() noexcept{
            return false;
        }
        void await_suspend(std::coroutine_handle<> handle){
            auto _schedule_func = [this,handle](void*){
                function_();
                if constexpr (resume_in_bk_thread){
                    handle.resume();
                }else {
                    // add back to coro scheduler.
                    async_scheduler_.scheduler_.add_task(handle);
                }
            };
            async_scheduler_.thread_pool_.schedule(_schedule_func);
        }
        void await_resume(){
            // donothign.
        }

        AsyncScheduler& async_scheduler_;
        BlockFunction function_;
    };


    struct __async_awaitable_base{
        int return_value_;
        std::coroutine_handle<> handle_;
    };


    //TODO
    struct __async_read : public __async_awaitable_base{

        struct param{
            int fd_;
            void* buf_;
            uint32_t bytes_;
            uint64_t offset_ = -1; 
        };
        __async_read(AsyncScheduler& scheduler,param&& param):async_scheduler_(scheduler),param_(param){

        }

        bool await_ready() noexcept{
            return false;
        }

        void await_suspend(std::coroutine_handle<> handle){
            this->handle_ = handle;
            auto* sqe =  io_uring_get_sqe(&async_scheduler_.ring_);
            io_uring_sqe_set_data(sqe, this);
            io_uring_prep_read(sqe,param_.fd_,param_.buf_,param_.bytes_,param_.offset_);
            io_uring_submit(&async_scheduler_.ring_);
        }
        auto await_resume()->int{
            //async_read over;
            return return_value_;
        }
        AsyncScheduler& async_scheduler_;
        param param_;
    };
    struct __async_write:public __async_awaitable_base{
        struct param{
            int fd_;
            void* buf;
            uint32_t bytes;
            uint64_t offset_ = -1;
        };
        __async_write(AsyncScheduler& scheduler,param&& param):async_scheduler_(scheduler),param_(param){

        }

        bool await_ready() noexcept{
            return false;
        }
        
        void await_suspend(std::coroutine_handle<> handle){
            this->handle_ = handle;
            auto* sqe = io_uring_get_sqe(&async_scheduler_.ring_);
            io_uring_sqe_set_data(sqe, this);
            io_uring_prep_write(sqe, param_.fd_, param_.buf, param_.bytes, param_.offset_);
            io_uring_submit(&async_scheduler_.ring_);
        }
        auto await_resume() ->int{
            // async_write over;
            return return_value_;
        }

        AsyncScheduler& async_scheduler_;
        param param_;
    };


public:

    AsyncScheduler(Scheduler& scheduler);
    ~AsyncScheduler(){
        finished_ = true;
        uring_poll_thread_.join();
    }
    AsyncScheduler(AsyncScheduler&& other)=delete;
    AsyncScheduler(const AsyncScheduler& other)=delete;
    void execute(uint32_t block_thread_num = std::thread::hardware_concurrency()*10);

    template<bool resume_in_bk_thread = true>
    async_operation<resume_in_bk_thread> async_schedule(BlockFunction&& block_function){
        return async_operation<resume_in_bk_thread>{*this,std::move(block_function)};
    }


    __async_read async_read(int fd,void* buf, uint32_t bytes,uint64_t offset=-1){
        return __async_read{*this,{fd,buf,bytes,offset}};
    }

    __async_write async_write(int fd,void* buf,uint32_t bytes,uint64_t offset=-1){
        return __async_write{*this,{fd,buf,bytes,offset}};
    }

};