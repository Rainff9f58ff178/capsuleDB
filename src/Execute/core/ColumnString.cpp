#include "Execute/core/ColumnString.h"

#include "CataLog/TableCataLog.h"



void ColumnString::insertFrom(const ValueUnionView& value){
    for(uint32_t i=0;i< value.size();++i){
        DASSERT(value[i].type_ == TypeString);
        data_.push_back(std::string(value[i].val_.data_,value[i].value_len_));
    }
}
void ColumnString::insertFrom(const ValueUnion& value){
    CHEKC_THORW(value.type_ == TypeString);
    data_.push_back(std::string(value.val_.data_,value.value_len_));
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

int ColumnString::compare_at(ExecColumn* _other,
uint32_t l_idx,uint32_t r_idx){
    auto* other = down_cast<ColumnString*>(_other);
    CHEKC_THORW( l_idx < data_.size());
    CHEKC_THORW(r_idx < other->data_.size());
    auto size = data_[l_idx].size();
    auto rhs_size = other->data_[r_idx].size();
    int cmp = memcmp(data_[l_idx].data(),other->data_[r_idx].data(),std::min(size,rhs_size));
    if(cmp != 0)
        return cmp;
    else 
        return size > rhs_size ? 1 :(size<rhs_size ? -1 : 0);
}
ColumnRef ColumnString::GetByRowNumbers(RowNumbers& nums){
    auto col = std::make_shared<ColumnString>(name_);
    CHEKC_THORW(nums.size() == rows());
    col->data_.resize(nums.size());
    for(uint32_t i=0;i<nums.size();++i){
        col->data_[i] = data_[ nums[i] ];
    }
    return col;
};


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


ValueUnion ColumnString::agg_count(){
    return ValueUnion(data_.size());
}
ValueUnion ColumnString::agg_sum(){
    int sum = 0;
    for(auto& data : data_){
        sum+=data.size();
    }
    return ValueUnion(sum);
}
ValueUnion ColumnString::agg_min(){
    int min = INT32_MAX;
    for(auto& data:data_){
        min = std::min((int)data.size(),min);
    }
    return min;
}
ValueUnion ColumnString::agg_max(){
    int max= INT32_MAX;
    for(auto &data: data_){
        max = std::max( (int)data.size(),max ) ;
    }
    return max;
}
ValueUnion ColumnString::agg_avg(){
    return  ValueUnion(agg_sum().val_.num_ / data_.size());
}