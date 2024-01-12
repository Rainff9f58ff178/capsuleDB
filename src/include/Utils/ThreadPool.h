#pragma once
#include "Logger.h"
#include <cstdint>
#include<functional>
#include<condition_variable>
#include <list>
#include <mutex>
#include<thread>
#include <vector>

class ThreadPool{

    using __Fucntion = std::function<void(void*)>;
    std::vector<std::thread>    work_threads_;

    std::condition_variable     cv_;
    std::list<__Fucntion> jobs_;
    std::mutex lock_for_jobs_;
public:
    ThreadPool(uint32_t thread_num){   
        for(uint32_t i=0;i<thread_num;++i){
            work_threads_.emplace_back([&,this](){
                work(i);
            });
        }
    }

    void work(uint32_t thread_id){
        while(true){
            std::unique_lock<std::mutex> lock(lock_for_jobs_);
            cv_.wait(lock,[&,this](){return jobs_.size() != 0;});
            
            auto job = std::move(jobs_.front());
            jobs_.pop_front();
            lock.unlock();
            try {
                job(nullptr);
            } catch (...) {
                DEBUG("unknowned exception in thread pool");
            }
        }
    }   
    void schedule(const __Fucntion& job){
        std::unique_lock<std::mutex> lock(lock_for_jobs_);
        jobs_.push_back(job);
        cv_.notify_one();
    }

};