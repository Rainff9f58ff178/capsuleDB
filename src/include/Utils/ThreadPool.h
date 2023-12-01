#pragma once
#include <cstdint>
#include<functional>
#include<condition_variable>
#include<thread>
#include <vector>

class ThreadPool{

    std::vector<std::thread>    work_threads_;
    std::condition_variable     cv_;
public:
    ThreadPool(uint32_t thread_num){   
    }

};