#pragma once
#include"liburing.h"
#include<thread>
#include<vector>
#include<cassert>
#include<coroutine>
#include<list>
#include<functional>
#include"Scheduler.h"
class AsyncScheduler{

    using BlockFunction = std::function<void()>;
    static constexpr uint32_t IO_URING_QUEUE_DEPTH = 32768;
    struct io_uring ring_;
    std::thread uring_poll_thread_;
    std::vector<std::thread> working_thread_;
    std::list<BlockFunction> job_list_;
    std::mutex internal_mtx_;
    std::condition_variable cv_;

    
    struct async_operation{

        bool await_ready() noexcept{
            return false;
        }
        void await_suspend(std::coroutine_handle<> handle){
            auto _schedule_func = [this,handle](){
                function_();
                handle.resume();
            };
            std::unique_lock<std::mutex> lock(async_scheduler_.internal_mtx_);
            async_scheduler_.job_list_.push_back(std::move(_schedule_func));
            async_scheduler_.cv_.notify_one();
        }

        AsyncScheduler& async_scheduler_;
        BlockFunction function_;
    };


    //TODO
    struct async_read{

    };
    struct async_write{

    };


public:

    AsyncScheduler();
    AsyncScheduler(AsyncScheduler&& other)=delete;
    AsyncScheduler(const AsyncScheduler& other)=delete;
    void execute(uint32_t block_thread_num = std::thread::hardware_concurrency()*10);

    async_operation 
    async_schedule(BlockFunction&& block_function){
        return async_operation{*this,std::move(block_function)};
    }


};