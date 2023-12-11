#pragma once
#include<coroutine>
#include<exception>


template<class T>
class Generator{
public:
    struct promise_type;
    using handle_type = std::coroutine_handle<promise_type>;

    struct promise_type{
        T value_;
        std::exception_ptr exception_;
        Generator get_return_object(){
            return Generator(handle_type::from_promise(*this));
        }
        std::suspend_always initial_suspend() { return {}; }
        std::suspend_always final_suspend() noexcept { return {}; }
        void unhandled_exception() { exception_ = std::current_exception(); } 

        template<std::convertible_to<T> From> // C++20 concept
        std::suspend_always yield_value(From&& from)
        {
            value_ = std::forward<From>(from); // caching the result in promise
            return {};
        }
        template<std::convertible_to<T> From> // C++20 concept
        void return_value(From&& from) {
            value_ = std::forward<From>(from);
        }
    };  

    handle_type h_;

    Generator(){
        h_=nullptr;
    }
    Generator(handle_type h) : h_(h) {}
    Generator(Generator&& gen){
        if(h_)
            h_.destroy();
        h_ = gen.h_;
        gen.h_=nullptr;
    }
    void operator=(Generator&& gen){
        if(h_)
            h_.destroy();
        h_ = gen.h_;
        gen.h_=nullptr;
    }
    bool finish(){
        return h_.done();
    }
    explicit operator bool(){
        if(!h_ || h_.done()) 
            return false;
        return true;
    }
    ~Generator() { 
        if(h_){
            h_.destroy();
        }
     }

    T operator()(){
        if(!h_.done())
            h_.resume();
        if (h_.promise().exception_)
            std::rethrow_exception(h_.promise().exception_);
        
        return  std::move(h_.promise().value_);
    }
 
};