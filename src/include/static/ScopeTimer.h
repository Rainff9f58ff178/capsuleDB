#pragma once


#include<chrono>
#include"static/RuntimeProfile.h"



class ScopeTimer{
public:
    ScopeTimer(RuntimeProfile::Counter* counter):counter_(counter){
        before_ = std::chrono::system_clock::now().time_since_epoch().count();
    }

    ~ScopeTimer(){
        uint64_t value = std::chrono::system_clock::now().time_since_epoch().count() - before_;
        counter_->update(value);
    }

    RuntimeProfile::Counter* counter_;
    uint64_t before_;  
};


#define  SCOPED_TIMER(c) ScopeTimer __c__(c);