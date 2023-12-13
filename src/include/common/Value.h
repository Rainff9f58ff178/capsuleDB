#pragma once
#include"common/type.h"
#include "Utils/ArrayView.h"
enum ValueType{
    TypeInt =0,
    TypeString ,
    TypeDouble,
};
enum class ValueId{
    SignedNumeric=0,
    UnSignedNumeric,
    DoubleFloatPoint,
    String
};

class ValueUnion{
public:
    
    ValueUnion(Value num);
    ValueUnion(const std::string& value);

    ValueUnion(char* data,uint32_t len){
        type_ = ValueType::TypeString;
        val_.data_ = data;
        id_ = ValueId::String;
        value_len_ = len;
    }
    ValueUnion(const ValueUnion& other);
    ValueUnion(ValueUnion&& other);
    ~ValueUnion();
    ValueUnion clone()const{
        if(type_==TypeInt){
            return ValueUnion(val_.num_);
        }else {
            return ValueUnion(std::string(val_.data_,value_len_));
        }
    }
    bool operator==(const ValueUnion& other);
    bool operator!=(const ValueUnion& other);
    bool operator>=(const ValueUnion& other);
    bool operator<=(const ValueUnion& other);
    bool operator> (const ValueUnion& other);
    bool operator< (const ValueUnion& other);

    ValueUnion operator+ (const ValueUnion& other);
    ValueUnion operator- (const ValueUnion& other);
    ValueType type_;
    ValueId id_;
    union __value{
        double d_val_;
        char* data_;
        Value num_;
    };
    __value val_;
    uint32_t value_len_{0};
    bool allocated_{false};

};
using ValueUnionView = array_view<ValueUnion>;
