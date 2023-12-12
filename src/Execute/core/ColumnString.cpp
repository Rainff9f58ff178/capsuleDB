#include "Execute/core/ColumnString.h"

#include "CataLog/TableCataLog.h"



void ColumnString::insertFrom(const ValueUnionView& value){
    for(uint32_t i=0;i< value.size();++i){
        DASSERT(value[i].type_ == TypeString);
        data_.push_back(std::string(value[i].data_,value[i].value_len_));
    }
}
uint64_t ColumnString::HashAt(uint32_t idx){
    return std::hash<std::string>()(data_[idx]);
}
void ColumnString::
insertToTable(TableCataLog* table,uint32_t col_idx){
    for(auto& val:data_){
        table->Insert(col_idx,val);
    }
}
std::string ColumnString::toString(uint32_t row_idx){
    return data_[row_idx];
}
void ColumnString::insertFrom(ExecColumn* other,uint32_t idx){
    auto* o = down_cast<ColumnString*>(other);
    CHEKC_THORW(o->data_.size()> idx);
    data_.push_back(o->data_[idx]);
}   


void ColumnString::MergeData(ExecColumn* other){
    auto* o = down_cast<ColumnString*>(other);
    for(uint32_t i=0;i<o->data_.size();++i){
        data_.push_back(o->data_[i]);
    }
}
uint32_t ColumnString::max_char_size() {
    uint32_t max_size = 0;
    for(auto& val: data_){
        max_size = std::max((uint32_t)val.size(),max_size);
    }
    return max_size;
}