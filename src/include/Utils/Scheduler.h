#pragma once


#pragma once
#include<coroutine>
#include<list>
#include<condition_variable>
#include<mutex>
#include<thread>
#include<vector>


template<typename return_type>
class Task;
class Scheduler{
    std::list<std::coroutine_handle<>> tasks_;
    std::mutex internal_mtx_;
    std::condition_variable cv_;
    std::vector<std::thread> working_thread_;
    std::list<Task<void>> _t_s_;
public:
    struct schedule_operation{
        bool await_ready() noexcept{
            return false;
        }
        void await_suspend(std::coroutine_handle<> h){
            std::unique_lock<std::mutex> l(scheduler_.internal_mtx_);
            scheduler_.tasks_.push_back(h);
            scheduler_.cv_.notify_one();
        }
        void await_resume() noexcept{

        };
        Scheduler& scheduler_;
    };
    schedule_operation schedule(){
        return schedule_operation{*this};
    }
    void schedule(Task<void>&& job);
    void add_task(std::list<std::coroutine_handle<>>&& task);
    void execute(uint32_t thread_num = std::thread::hardware_concurrency());
};


extern Scheduler g_scheduler;