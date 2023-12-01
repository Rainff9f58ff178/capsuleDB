#include "Table/Tuple.h"
#include "nodes/parsenodes.hpp"


void SerializeTo(Value& value,char* data){
    mempcpy(data,(void*)&value,sizeof(Value));
}

Tuple::Tuple(std::vector<Value> vec,Schema& schema){
    assert(vec.size() == schema.columns_.size());
    uint32_t total_length=0;
   
    size_ = vec.size()*4;
    data_ = new char[size_];
    allocated_ = true;
    for(uint32_t i=0;i<vec.size();++i){
        SerializeTo(vec[i],data_+schema.columns_[i].offset_);
    }
}
 std::vector<Value> Tuple::ToString(Schema& schema){
    uint32_t len = schema.columns_.size()*4;
    assert(len<=size_);
    std::vector<Value> v;
    std::stringstream ss;
    for(auto& col : schema.columns_){
        v.push_back(*(int32_t*)(data_+col.offset_));
    }
    return v;
}


Tuple::~Tuple(){
    if(allocated_ ){
        assert(data_!=nullptr);
        delete[] data_;
    }
}