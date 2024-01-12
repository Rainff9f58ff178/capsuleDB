#pragma once

#include<coroutine>
#include <exception>
#include <memory>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include<variant>
template<typename return_type>
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

    struct unset_return_value{
            unset_return_value() {}
            unset_return_value(unset_return_value&&)      = delete;
            unset_return_value(const unset_return_value&) = delete;
            auto operator=(unset_return_value&&)          = delete;
            auto operator=(const unset_return_value&)     = delete;
    };


    static constexpr bool return_type_is_reference = std::is_reference_v<ReturnType>;
    using stored_type                              = std::conditional_t<
                    return_type_is_reference,
                    std::remove_reference_t<ReturnType>*,
                    std::remove_const_t<ReturnType>>;


    using variant_type = std::variant<unset_return_value, stored_type, std::exception_ptr>;


    using task_type =  Task<ReturnType>;
    std::exception_ptr current_exception_;
    auto get_return_object() noexcept ->task_type;

    template<typename  value_type>
    requires(return_type_is_reference and std::is_constructible_v<ReturnType, value_type&&>) or 
        (not return_type_is_reference and std::is_constructible_v<stored_type, value_type&&>) 
    auto return_value(value_type&& value) -> void {
        if constexpr (return_type_is_reference){
            ReturnType ref = static_cast<value_type&&>(value);
            value_.template emplace<stored_type>(std::addressof(ref));
        }else {
            value_.template emplace<stored_type>(std::forward<value_type>(value));
        }
    }
    auto return_value(stored_type value) -> void requires(not return_type_is_reference){
        if constexpr (std::is_move_constructible_v<stored_type>){
            value_.template emplace<stored_type>(std::move(value));
        }else {
            value_.template emplace<stored_type>(value);
        }
    }
    


    auto unhandled_exception() noexcept -> void{
        new (&value_) variant_type(std::current_exception());
    }

    auto result()& ->decltype(auto){
        if(std::holds_alternative<stored_type>(value_)){
            if constexpr (return_type_is_reference){
                return static_cast<ReturnType>(*std::get<stored_type>(value_));
            }else {
                return static_cast<const ReturnType&>(std::get<stored_type>(value_));
            }
        }else if(std::holds_alternative<std::exception_ptr>(value_)){
            std::rethrow_exception(std::get<std::exception_ptr>(value_));
        }else {
            throw std::runtime_error("return value not set.");
        }
        
    }
    auto result() const& ->decltype(auto){
        if(std::holds_alternative<stored_type>(value_)){
            if constexpr (return_type_is_reference){
                return static_cast<std::add_const_t<ReturnType>>(*std::get<stored_type>(value_));
            }else {
                return static_cast<const ReturnType&>(std::get<stored_type>(value_));
            }
        }else if(std::holds_alternative<std::exception_ptr>(value_)){
            std::rethrow_exception(std::get<std::exception_ptr>(value_));
        }else {
            throw std::runtime_error("return value not set.");
        }
    }


    auto result()&& ->decltype(auto){
        if(std::holds_alternative<stored_type>(value_)){
            if constexpr (return_type_is_reference){
                return static_cast<ReturnType>(*std::get<stored_type>(value_));
            }
            else if constexpr (std::is_move_constructible_v<ReturnType>){
                return static_cast<ReturnType&&>(std::get<stored_type>(value_));
            }else {
                return static_cast<const ReturnType&&>(std::get<stored_type>(value_));
            }
        }else if(std::holds_alternative<std::exception_ptr>(value_)){
            std::rethrow_exception(std::get<std::exception_ptr>(value_));
        }else {
            throw std::runtime_error("return value not set.");
        }
    }





    variant_type value_;
};
template<>
struct Promise<void> : public promise_base{
    using task_type = Task<void>;
    
    auto get_return_object() noexcept ->task_type;
    auto return_void() noexcept->void{}

    auto unhandled_exception() noexcept -> void { current_exception_ = std::current_exception();}
    auto result() ->void{
        if(current_exception_){
            std::rethrow_exception(current_exception_);
        }
    }


private:
    std::exception_ptr current_exception_;
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
            std::coroutine_handle<> await_suspend(std::coroutine_handle<> h){

                coro_.promise().next_coroutinue_ = h;
                return coro_;
            }
            auto await_resume()->decltype(auto){
                // return something
                return std::move(this->coro_.promise()).result();
            }
            std::coroutine_handle<promise_type> coro_;
        };
        return awaitable{handle_};

    }

    promise_type& promise(){return handle_.promise();}
private:

};


template<typename  ReturnType>
inline auto Promise<ReturnType>::get_return_object() noexcept ->Task<ReturnType>{
    return Task<ReturnType>{std::coroutine_handle<Promise<ReturnType>>::from_promise(*this) };
}
inline auto Promise<void>::get_return_object() noexcept ->Task<void>{
    return Task<void>{std::coroutine_handle<Promise<void>>::from_promise(*this)};
}

