#include "Execute/core/ColumnString.h"

#include "CataLog/TableCataLog.h"



void ColumnString::insertFrom(const ValueUnionView& value){
    for(uint32_t i=0;i< value.size();++i){
        DASSERT(value[i].type_ == TypeString);
        data_.push_back(std::string(value[i].data_,value[i].value_len_));
    }
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

uint32_t ColumnString::max_char_size() {
    uint32_t max_size = 0;
    for(auto& val: data_){
        max_size = std::max((uint32_t)val.size(),max_size);
    }
    return max_size;
}