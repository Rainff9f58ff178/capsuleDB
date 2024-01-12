

#include "./src/include/Utils/Generator.h"
#include "./src/include/Utils/Scheduler.h"
#include "Logger.h"
#include "Planer/InsertLogicalOperator.h"
#include "Utils/AsyncScheduler.h"
#include "Utils/Task.h"
#include "asio2/bho/beast/http/field.hpp"
#include "common/Exception.h"
#include <coroutine>
#include <cstdint>
#include <cstdlib>
#include <fcntl.h>
#include <iostream>
#include <list>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <unistd.h>


Logger g_logger("test_g_logger");
Generator<int> genInt(){
    co_return 1;
}
void t_gen(){
    auto gen=  genInt();
    std::coroutine_handle<> handle = std::coroutine_handle<>::from_address(gen.h_.address());
    handle.resume();
    auto i = gen.h_.promise().value_;
    std::cout<<i<<std::endl;
    std::cout<<gen.finish()<<std::endl;
}

void test_task(){
    
    Scheduler scheduler;


    auto inside = [](Scheduler& s)->Task<void>{
        for(uint32_t i=0;i<3;++i){
            std::cout<<"inside"<<std::this_thread::get_id()<<std::endl;
            co_await s.schedule();
        }
    };

    scheduler.schedule([](Scheduler& s,auto f )->Task<void>{
        while(1){
            std::cout<<"A"<<std::this_thread::get_id()<<std::endl;
            sleep(1);
            co_await s.schedule();
        }
        co_return;
    }(scheduler,inside));

    scheduler.schedule([](Scheduler& s,auto f )->Task<void>{
        while(1){
            std::cout<<"b"<<std::this_thread::get_id()<<std::endl;
            sleep(1);
            co_await s.schedule();
        }
        co_return;
    }(scheduler,inside));

    for(uint32_t i=0;i<14;++i){
        scheduler.schedule([](Scheduler& s,auto f )->Task<void>{
            while(1){
                std::cout<<"b"<<std::this_thread::get_id()<<std::endl;
                sleep(1);
                co_await s.schedule();
            }
            co_return;
        }(scheduler,inside));
    }

    scheduler.execute();

}
struct runtime_state{
        Scheduler* scheduler_;
        AsyncScheduler* async_scheduler_;
};
void async_raed(runtime_state* state){
    std::cout<<"async_reading..."<<std::this_thread::get_id()<<std::endl;
    sleep(5);
    std::cout<<"async_read over"<<std::this_thread::get_id()<<std::endl;

}
void test_async_scheduler(){
    Scheduler scheduler;
    AsyncScheduler async_scheduler(scheduler);
    runtime_state state{&scheduler,&async_scheduler};
    
    auto task1 = [](runtime_state* state)->Task<void>{
        std::cout<<"ready to async operation  "<<std::this_thread::get_id()<<std::endl;
        co_await state->async_scheduler_->async_schedule<false>(std::bind(async_raed,state));
        std::cout<<"backed to coro...  "<<std::this_thread::get_id()<<std::endl;
    };

    scheduler.schedule(std::move(task1(&state)));





    scheduler.execute();

}

Task<uint64_t> async_read(runtime_state* state,const std::string& file_name,char* buf,uint32_t bytes){
    auto f_fd = open(file_name.c_str(), O_RDONLY);
    if(f_fd == -1) {throw std::runtime_error("no such file");};
    std::cout<<"ready to async_read..."<<std::this_thread::get_id()<<std::endl;
    auto b = co_await state->async_scheduler_->async_read(f_fd, buf, bytes);
    std::cout<<"async_read"<<b<<" size over..."<<std::this_thread::get_id()<<std::endl;
    co_return b;
}

void test_async_read(){
    Scheduler scheduler;
    AsyncScheduler async_scheduler(scheduler);
    char buf[256];
    runtime_state state{&scheduler,&async_scheduler};
    auto task1 = [](runtime_state* state,char* buf)->Task<void>{
        std::cout<<"ready to async_read..."<<std::this_thread::get_id()<<std::endl;
        auto b =co_await async_read(state,"Starddb.log", buf, 256);
        std::cout<<"readed content is :"<<std::string(buf,b)<<std::endl;
        co_return;
    }(&state,buf);

    scheduler.schedule(std::move(task1));




    scheduler.execute();
}

void test_sync_wait(){
    Scheduler scheduler;
    AsyncScheduler async_scheduler(scheduler);
    runtime_state state{&scheduler,&async_scheduler};


    auto task1 = [](runtime_state* state)->Task<uint32_t>{
        for(uint32_t i=0;i<10;++i){
            std::cout<<"nice"<<i<<std::this_thread::get_id()<<std::endl;
            co_await state->scheduler_->schedule();
        }
        co_return 1000;
    }(&state);

    auto i =scheduler.sync_wait(std::move(task1));
    std::cout<<"result is "<<i<<std::endl;
}

void test_sync_wait_all(){
    Scheduler scheduler;
    AsyncScheduler async_scheduler(scheduler);
    runtime_state state{&scheduler,&async_scheduler};


    std::list<Task<void>> tasks;
    for(uint32_t i=0;i<10;++i){
        auto task = [](runtime_state* state)->Task<void>{
            for(uint32_t i=0;i<10;++i){
                std::cout<<"nice"<<i<<" "<<std::this_thread::get_id()<<std::endl;
                co_await state->scheduler_->schedule();
            }
        }(&state);
        tasks.push_back(std::move(task));
    }
    auto boot_task = [](runtime_state* state,auto&& tasks)->Task<void>{
        state->scheduler_->sync_wait_all<void>(std::move(tasks));
        co_return;
    }(&state,std::move(tasks));

    scheduler.schedule(std::move(boot_task));



    std::list<Task<uint32_t>> tasks2;
    for(uint32_t i=0;i<10;++i){
        auto task = [](runtime_state* state)->Task<uint32_t>{
            for(uint32_t i=0;i<10;++i){
                std::cout<<"nice"<<i<<" "<<std::this_thread::get_id()<<std::endl;
                co_await state->scheduler_->schedule();
            }
            co_return rand();
        }(&state);

        tasks2.push_back(std::move(task));
    }

    
    auto boot_task2 = [](runtime_state* state,auto&& tasks)->Task<void>{
        auto r = state->scheduler_->sync_wait_all<uint32_t>(std::move(tasks));
        for(auto& e : r){
            std::cout<<e<<std::endl;
        }
        co_return;
    }(&state,std::move(tasks2));

    scheduler.schedule(std::move(boot_task2));

    scheduler.execute();
}
int main(){
    // test_task();
    // test_async_scheduler();
    // t_gen();
    // test_async_read();
    // test_sync_wait();
    test_sync_wait_all();
    return  0;
}