#pragma once
#include"common/type.h"
#include "Utils/ArrayView.h"
enum ValueType{
    TypeInt =0,
    TypeString 
};
enum class ValueId{
    SignedNumeric=0,
    UnSignedNumeric,
    String
};

class ValueUnion{
public:
    
    ValueUnion(Value num);
    ValueUnion(const std::string& value);

    ValueUnion(char* data,uint32_t len){
        type_ = ValueType::TypeString;
        data_ = data;
        value_len_ = len;
    }
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
    bool operator!=(const ValueUnion& other);
    bool operator>=(const ValueUnion& other);
    bool operator<=(const ValueUnion& other);
    bool operator> (const ValueUnion& other);
    bool operator< (const ValueUnion& other);
    ValueType type_;
    ValueId id_;
    union{
        char* data_;
        Value num_;
    };
    uint32_t value_len_{0};
    bool allocated_{false};

};
using ValueUnionView = array_view<ValueUnion>;
