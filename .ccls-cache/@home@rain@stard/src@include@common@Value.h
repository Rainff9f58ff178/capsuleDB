#pragma once
#include"common/type.h"
#include "Utils/ArrayView.h"
enum ValueType{
    TypeInt =0,
    TypeString 
};

class ValueUnion{
public:
    
    ValueUnion(Value num);
    ValueUnion(const std::string& value);

    ValueUnion(const ValueUnion& other);
    ValueUnion(ValueUnion&& other);
    ~ValueUnion();
    ValueUnion clone()const{
        if(type_==TypeInt){
            return ValueUnion(num_);
        }else {
            return ValueUnion(std::string(data_,value_len_));
        }
    }
    bool operator==(const ValueUnion& other);
    ValueType type_;
    union{
        char* data_;
        Value num_;
    };
    uint32_t value_len_{0};
    bool allocated_{false};

};
using ValueUnionView = array_view<ValueUnion>;
