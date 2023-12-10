#pragma once


#include<iostream>
#include<fstream>
#include<string>
#include<list>
#include<thread>
#include<condition_variable>

class Logger{
        
public:
    Logger(const std::string& log_name);

    void AppendDEBUG(const std::string& info);

    ~Logger(){
        {
        std::unique_lock<std::mutex> l(internal_lock_);
        finished_ = true;
        cv_.notify_one();
        l.unlock();
        }
        flush_thread_.join();

        // log last 
        while(!queue_.empty()){
            auto s = std::move(queue_.front());
            f_<<s;
            queue_.pop_front();
        }



        f_.flush();
    }
    std::fstream f_;
    std::thread flush_thread_;
    std::atomic<bool> finished_=false;
    std::mutex internal_lock_;
    std::condition_variable cv_;
    std::list<std::string> queue_;

};



extern Logger g_logger;

#define DEBUG(x) g_logger.AppendDEBUG(x);