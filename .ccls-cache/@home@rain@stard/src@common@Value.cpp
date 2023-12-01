
#include "common/Value.h"
#include "common/Exception.h"



ValueUnion::ValueUnion(Value num){
    type_ = TypeInt;
    value_len_ = sizeof(num);
    num_ = num;
}

ValueUnion::ValueUnion(const std::string& value){
    value_len_ = value.size();
    type_ = TypeString;
    data_ = new char[value_len_];
    memcpy(data_,value.data(),value_len_);
    allocated_ =  true;
}
ValueUnion::ValueUnion(const ValueUnion& other){
    type_ = other.type_;
    value_len_ = other.value_len_;
    if(type_==ValueType::TypeInt){
        num_=  other.num_;
    }else {
        data_ = other.data_;
    }
}
ValueUnion::ValueUnion(ValueUnion&& other){
    value_len_ = other.value_len_;
    type_ = other.type_;
    if(type_==TypeInt)
        num_ = other.num_;
    else {
        data_ = other.data_;
        other.data_ = nullptr;
        std::swap(allocated_,other.allocated_);
    }
}

bool ValueUnion::operator==(const ValueUnion& other){
    if( (type_!=other.type_) || (value_len_ != other.value_len_))
        return false;
    if(type_==ValueType::TypeInt){
        return num_ == other.num_;
    }else if(type_==ValueType::TypeString){
        return memcpy(data_,other.data_,value_len_);
    }
    throw Exception("Logical Error");
    
}
ValueUnion::~ValueUnion(){
    if(type_== TypeString && data_ && allocated_){
        delete data_;
    }
}