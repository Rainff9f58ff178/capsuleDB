
#include "common/Value.h"
#include "common/Exception.h"



ValueUnion::ValueUnion(Value num){
    memset(&val_,0,sizeof(val_));
    type_ = TypeInt;
    value_len_ = sizeof(num);
    val_.num_ = num;
    id_ = ValueId::SignedNumeric;
}

ValueUnion::ValueUnion(const std::string& value){
    memset(&val_,0,sizeof(val_));

    value_len_ = value.size();
    type_ = TypeString;
    val_.data_ = new char[value_len_];
    memcpy(val_.data_,value.data(),value_len_);
    allocated_ =  true;
    id_ = ValueId::String;
}
ValueUnion::ValueUnion(const ValueUnion& other){
    memset(&val_,0,sizeof(val_));

    type_ = other.type_;
    value_len_ = other.value_len_;
    id_ = other.id_;
    if(type_==ValueType::TypeInt){
        val_.num_=  other.val_.num_;
    }else {
        val_.data_ = new char[other.value_len_];
        memcpy(val_.data_,other.val_.data_,other.value_len_);
        allocated_ = true;
    }
}
bool ValueUnion::operator=(const ValueUnion& other){

    memset(&val_,0,sizeof(val_));
    type_ = other.type_;
    value_len_ = other.value_len_;
    id_ = other.id_;
    if(type_==ValueType::TypeInt){
        val_.num_=  other.val_.num_;
    }else {
        val_.data_ = new char[other.value_len_];
        memcpy(val_.data_,other.val_.data_,other.value_len_);
        allocated_ = true;
    }
    UNREACHABLE;
}

ValueUnion::ValueUnion(ValueUnion&& other){
    memset(&val_,0,sizeof(val_));
    value_len_ = other.value_len_;
    type_ = other.type_;
    id_ = other.id_;
    if(type_==TypeInt)
        val_.num_ = other.val_.num_;
    else {
        val_.data_ = other.val_.data_;
        other.val_.data_ = nullptr;
        std::swap(allocated_,other.allocated_);
    }
}

bool ValueUnion::operator==(const ValueUnion& other) const{
    //to do 
    if(id_ != other.id_)
        return false;
    
    if(id_==ValueId::SignedNumeric){
        return (int64_t)(val_.num_) == (int64_t)(other.val_.num_);
    }else if(id_ == ValueId::UnSignedNumeric){
        return (uint64_t)(val_.num_) == (uint64_t)(other.val_.num_);
    }else if(id_==ValueId::String){
        if(value_len_ != other.value_len_) return  false;
        return memcmp(val_.data_,other.val_.data_,value_len_) == 0;
    }

    throw Exception("Logical Error");
    
}
bool ValueUnion::operator!=(const ValueUnion& other) const{
       //to do 
    return !(*this== other);
}
bool ValueUnion::operator>=(const ValueUnion& other) const{
    return  !(*this< other);
}
bool ValueUnion::operator<=(const ValueUnion& other) const{
    return  !(*this > other);
}
bool ValueUnion::operator> (const ValueUnion& other) const{
    if(id_ != other.id_)
        return false;

    if(id_==ValueId::SignedNumeric){
        return (int64_t)(val_.num_) > (int64_t)(other.val_.num_);
    }else if(id_ == ValueId::UnSignedNumeric){
        return (uint64_t)(val_.num_) > (uint64_t)(other.val_.num_);
    }else if(id_==ValueId::String){
        if(value_len_ != other.value_len_) return  false;
        return memcmp(val_.data_,other.val_.data_,value_len_) > 0;
    }
    UNREACHABLE;
}
bool ValueUnion::operator< (const ValueUnion& other) const{

    if(id_ != other.id_)
        return false;

    if(id_==ValueId::SignedNumeric){
        return (int64_t)(val_.num_) < (int64_t)(other.val_.num_);
    }else if(id_ == ValueId::UnSignedNumeric){
        return (uint64_t)(val_.num_) < (uint64_t)(other.val_.num_);
    }else if(id_==ValueId::String){
        if(value_len_ != other.value_len_) return  false;
        return memcmp(val_.data_,other.val_.data_,value_len_) < 0;
    }
    UNREACHABLE
}

ValueUnion ValueUnion::operator+ (const ValueUnion& other) const{
    if(id_ != other.id_)
        throw Exception(_error_msg("type mismatch value can't add "));

    if(id_==ValueId::SignedNumeric){
        return ValueUnion( (int32_t)((int64_t)val_.num_+(int64_t)other.val_.num_));
    }else if(id_ == ValueId::UnSignedNumeric){
        return ValueUnion( (uint64_t)(val_.num_) + (uint64_t)(other.val_.num_));
    }else if(id_==ValueId::String){
        std::string toget = std::string(val_.data_,value_len_) + std::string(other.val_.data_,other.value_len_);
        return  ValueUnion(toget);
    }
    UNREACHABLE
}
ValueUnion ValueUnion::operator- (const ValueUnion& other) const{
     if(id_ != other.id_)
        throw Exception(_error_msg("type mismatch value can't add "));

    if(id_==ValueId::SignedNumeric){
        return ValueUnion( (int32_t)((int64_t)val_.num_-(int64_t)other.val_.num_));
    }else if(id_ == ValueId::UnSignedNumeric){
        return ValueUnion( (uint64_t)(val_.num_) - (uint64_t)(other.val_.num_));
    }else if(id_==ValueId::String){
        throw Exception("can't string minus for now");
    }
    UNREACHABLE
}   


ValueUnion::~ValueUnion(){
    if(type_== TypeString && val_.data_ && allocated_){
        delete val_.data_;
    }
}