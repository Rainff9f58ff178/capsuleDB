#pragma once

#include<coroutine>

template<typename return_type = void>
class Task;


class promise_base{
public:
    friend struct final_awaitable;
    struct final_awaiter{
        bool await_ready() noexcept{
            return false;
        }
        template<typename promise_type>
        auto await_suspend(std::coroutine_handle<promise_type> h) noexcept
            ->std::coroutine_handle<>{
            if(h.promise().next_coroutinue_){
                return h.promise().next_coroutinue_;
            }else 
                return std::noop_coroutine();
        }
        void await_resume() noexcept{
            
        }

    };
    std::suspend_always initial_suspend() noexcept { return {}; }
    auto final_suspend() noexcept { return final_awaiter{}; }

    std::coroutine_handle<> next_coroutinue_;
};

template<class ReturnType>
class Promise:public promise_base{
public:
    using task_type = Task<ReturnType>;
    auto get_return_object()->task_type{
        return std::coroutine_handle<Promise>::from_promise(*this);
    }
    auto unhandled_exception() noexcept -> void{

    }
    void return_void(){
        
    }
};
template<class ReturnType>
class Task{
public:
    using promise_type = Promise<ReturnType>;
    std::coroutine_handle<promise_type> handle_;
    Task(std::coroutine_handle<promise_type> handle):handle_(handle){
    }
    Task(const Task& other){
        handle_ = other.handle_;
    }
    Task(Task&& other){
        handle_ = other.handle_;
        other.handle_ = nullptr;
    }
    ~Task(){
        if(handle_){
            handle_.destroy();
        }
    }
    auto operator co_await(){
        struct awaitable{
            bool await_ready(){
                return false;
            }
            std::coroutine_handle<promise_type> await_suspend(std::coroutine_handle<promise_type> h){

                coro_.promise().next_coroutinue_ = h;
                return coro_;
            }
            auto await_resume()->void{
                // return something
            }
            std::coroutine_handle<promise_type> coro_;
        };
        return awaitable{handle_};

    }
private:

};