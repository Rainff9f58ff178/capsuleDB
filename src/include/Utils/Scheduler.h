#pragma once


#include <functional>
#include<coroutine>
#include<list>
#include<condition_variable>
#include<mutex>
#include<thread>
#include <type_traits>
#include<vector>
#include "Task.h"
#include "pg_definitions.hpp"






class Scheduler{
    using __Function =  std::function<void(void*)>;

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
    void add_task(std::coroutine_handle<> task){
        std::unique_lock<std::mutex> lock(internal_mtx_);
        tasks_.push_back(std::move(task));
        cv_.notify_one();
    }
    
    template<class ReturnType>
    ReturnType sync_wait(Task<ReturnType>&& task){
        task.handle_.resume();
        while(!task.handle_.done()){
            std::unique_lock<std::mutex> lock(internal_mtx_);
            if(!tasks_.empty()){
                auto n_t = tasks_.front();
                tasks_.pop_front();
                lock.unlock();
                n_t.resume();
            }
        }
        // task.finished.
        return std::move(task.promise().result());  
    }   
    template<class ReturnType>
    std::conditional_t<std::is_void_v<ReturnType>,void,std::vector<ReturnType>>  sync_wait_all(std::list<Task<ReturnType>>&& tasks){
        std::list<std::coroutine_handle<>> tasks_handle;
        for(auto& e:tasks)
            tasks_handle.push_back(std::coroutine_handle<>::from_address(e.handle_.address()));
        std::unique_lock<std::mutex>    lock(internal_mtx_);
        tasks_.splice(tasks_.end(),tasks_handle);
        lock.unlock();
        cv_.notify_all();

        for(auto& t: tasks){
            while(!t.handle_.done()){
                lock.lock();
                if(!tasks_.empty()){
                    auto n_t = tasks_.front();
                    tasks_.pop_front();
                    lock.unlock();
                    n_t.resume();
                }else  
                    lock.unlock();
            }
        }
        // all task finshed.

        auto clear = [&](){
            for(auto& handle : tasks_handle){
                Assert(handle.done());
                handle.destroy();
            }
        };


        if constexpr (!std::is_void_v<ReturnType>){
            std::vector<ReturnType> result;
            for(auto& e:tasks){
                result.push_back (std::move(e.promise().result()));
            }
            clear();
            return result;
        }else {
            clear();
            return;
        }
    }


    void execute(uint32_t thread_num = std::thread::hardware_concurrency());
};


extern Scheduler g_scheduler;