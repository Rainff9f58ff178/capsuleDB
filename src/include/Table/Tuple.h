#pragma once




#include <atomic>
#include "CataLog/Schema.h"
#include "common/type.h"
#include "nodes/primnodes.hpp"
#include <sstream>
class Tuple{
public:

    explicit Tuple(std::vector<Value> vec,Schema& schema);
    Tuple(Tuple&& tuple){
        data_= tuple.data_;
        allocated_ = tuple.allocated_;
        size_ = tuple.size_;
        tuple.data_=nullptr;
        tuple.allocated_=false;
    }

    ~Tuple();
    
    std::vector<Value> ToString(Schema& schema);

private:
    uint32_t size_{0};
    char* data_{nullptr};
    bool allocated_{false};   
};