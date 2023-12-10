
#include "common/Value.h"
#include "common/Exception.h"



ValueUnion::ValueUnion(Value num){
    type_ = TypeInt;
    value_len_ = sizeof(num);
    num_ = num;
    id_ = ValueId::SignedNumeric;
}

ValueUnion::ValueUnion(const std::string& value){
    value_len_ = value.size();
    type_ = TypeString;
    data_ = new char[value_len_];
    memcpy(data_,value.data(),value_len_);
    allocated_ =  true;
    id_ = ValueId::String;
}
ValueUnion::ValueUnion(const ValueUnion& other){
    type_ = other.type_;
    value_len_ = other.value_len_;
    id_ = other.id_;
    if(type_==ValueType::TypeInt){
        num_=  other.num_;
    }else {
        data_ = other.data_;
    }
}
ValueUnion::ValueUnion(ValueUnion&& other){
    value_len_ = other.value_len_;
    type_ = other.type_;
    id_ = other.id_;
    if(type_==TypeInt)
        num_ = other.num_;
    else {
        data_ = other.data_;
        other.data_ = nullptr;
        std::swap(allocated_,other.allocated_);
    }
}

bool ValueUnion::operator==(const ValueUnion& other){
    //to do 
    if(id_ != other.id_)
        return false;
    
    if(id_==ValueId::SignedNumeric){
        return (int64_t)(num_) == (int64_t)(other.num_);
    }else if(id_ == ValueId::UnSignedNumeric){
        return (uint64_t)(num_) == (uint64_t)(other.num_);
    }else if(id_==ValueId::String){
        if(value_len_ != other.value_len_) return  false;
        return memcmp(data_,other.data_,value_len_) == 0;
    }

    throw Exception("Logical Error");
    
}
bool ValueUnion::operator!=(const ValueUnion& other){
       //to do 
    return !(*this== other);
}
bool ValueUnion::operator>=(const ValueUnion& other){
    return  !(*this< other);
}
bool ValueUnion::operator<=(const ValueUnion& other){
    return  !(*this > other);
}
bool ValueUnion::operator> (const ValueUnion& other){
    if(id_ != other.id_)
        return false;

    if(id_==ValueId::SignedNumeric){
        return (int64_t)(num_) > (int64_t)(other.num_);
    }else if(id_ == ValueId::UnSignedNumeric){
        return (uint64_t)(num_) > (uint64_t)(other.num_);
    }else if(id_==ValueId::String){
        if(value_len_ != other.value_len_) return  false;
        return memcmp(data_,other.data_,value_len_) > 0;
    }
    UNREACHABLE;
}
bool ValueUnion::operator< (const ValueUnion& other){

    if(id_ != other.id_)
        return false;

    if(id_==ValueId::SignedNumeric){
        return (int64_t)(num_) < (int64_t)(other.num_);
    }else if(id_ == ValueId::UnSignedNumeric){
        return (uint64_t)(num_) < (uint64_t)(other.num_);
    }else if(id_==ValueId::String){
        if(value_len_ != other.value_len_) return  false;
        return memcmp(data_,other.data_,value_len_) < 0;
    }
    UNREACHABLE
}

ValueUnion ValueUnion::operator+ (const ValueUnion& other){
    if(id_ != other.id_)
        throw Exception(_error_msg("type mismatch value can't add "));

    if(id_==ValueId::SignedNumeric){
        return ValueUnion( (int32_t)((int64_t)num_+(int64_t)other.num_));
    }else if(id_ == ValueId::UnSignedNumeric){
        return ValueUnion( (uint64_t)(num_) + (uint64_t)(other.num_));
    }else if(id_==ValueId::String){
        std::string toget = std::string(data_,value_len_) + std::string(other.data_,other.value_len_);
        return  ValueUnion(toget);
    }
    UNREACHABLE
}
ValueUnion ValueUnion::operator- (const ValueUnion& other){
     if(id_ != other.id_)
        throw Exception(_error_msg("type mismatch value can't add "));

    if(id_==ValueId::SignedNumeric){
        return ValueUnion( (int32_t)((int64_t)num_+(int64_t)other.num_));
    }else if(id_ == ValueId::UnSignedNumeric){
        return ValueUnion( (uint64_t)(num_) + (uint64_t)(other.num_));
    }else if(id_==ValueId::String){
        throw Exception("can't string minus for now");
    }
    UNREACHABLE
}   


ValueUnion::~ValueUnion(){
    if(type_== TypeString && data_ && allocated_){
        delete data_;
    }
}