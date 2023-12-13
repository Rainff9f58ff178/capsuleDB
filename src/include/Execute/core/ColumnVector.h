#pragma once


#include"Execute/core/Column.h"
#include "CataLog/TableCataLog.h"
template<class T>
class ColumnVector : public ExecColumn{
    friend class ColumnFactory; 
    using Self = ColumnVector<T>;
public:
    ColumnVector(){
        col_type_ = ExecInt;
        max_width_ = sizeof(T);
    }
    ColumnVector(const std::string& name){
        name_ = name;
        max_width_ = sizeof(T);
        col_type_ = ExecInt;
    }
    uint32_t rows()override{
        return data_.size();
    }
    uint64_t HashAt(uint32_t idx) override{
        return std::hash<T>()(data_[idx]);
    }
    ValueUnion ValueAt(uint32_t idx) override{
        CHEKC_THORW(idx<data_.size());
        return ValueUnion(data_[idx]);
    }
    
    void insertFrom(const ValueUnionView& value) override{
        for(uint32_t i=0;i<value.size();++i){
            DASSERT(value[i].type_ == TypeInt);
            data_.push_back(value[i].val_.num_);
        }
    }

    void MergeData(ExecColumn* other) override{
        auto _other = down_cast<Self*>(other);
        for(uint32_t i=0;i<_other->data_.size();++i){
            data_.push_back(_other->data_[i]);
        }
    }

    int compare_at(ExecColumn* _other,uint32_t l_idx,uint32_t r_idx) override{
        auto* other = down_cast<Self*>(_other);
        CHEKC_THORW(l_idx < data_.size());
        CHEKC_THORW(r_idx < other->data_.size());
        return data_[l_idx] > other->data_[r_idx] ? 1 :( data_[l_idx] < other->data_[r_idx] ? -1 : 0);
    }
    ColumnRef GetByRowNumbers(RowNumbers& nums) override{
        auto col = std::make_shared<ColumnVector<T>>(name_);        
        CHEKC_THORW(nums.size() == rows());
        col->data_.resize(rows());
        for(uint32_t i=0;i<nums.size();++i){
            col->data_[i] = data_[ nums[i] ];
        }
        return col;
    }
    void insertToTable(TableCataLog* table,uint32_t col_idx) override{
        for(uint32_t i=0;i<data_.size();++i){
            table->Insert(col_idx,data_[i]);
        }
    }
    void insertFrom(ExecColumn* other,uint32_t idx) override{
        auto* o = down_cast<Self*>(other);
        CHEKC_THORW(o->data_.size()>idx);
        data_.push_back(o->data_[idx]);
    }
    void insertFrom(const ValueUnion& val) override{
        CHEKC_THORW(val.type_== TypeInt);
        data_.push_back(val.val_.num_);
    }
    std::string toString(uint32_t row_idx) override{
        return std::to_string(data_[row_idx]);
    }
    uint32_t max_char_size() override{
        uint32_t max_size = 4;
        for(auto v:data_){
            auto num=std::to_string(v);
            max_size = std::max((uint32_t)num.size(),max_size);
        }
        return max_size;
    }
    
    ColumnRef clone(uint32_t rows=0) override{
       auto new_col =
        std::make_shared< ColumnVector>(name_);
        for(uint32_t i=0;i<rows;++i){
            new_col->data_[i] = data_[i];
        }
        return new_col;
    }



    ValueUnion agg_count() override{
        return  ValueUnion(data_.size());
    }
    ValueUnion agg_sum() override{
        int32_t sum = 0;
        for(uint32_t i=0;i<data_.size();++i){
            sum+=data_[i];
        }
        return ValueUnion(sum);
    }
    ValueUnion agg_min() override{
        int32_t min = INT32_MAX;
        for(uint32_t i=0;i<data_.size();++i){
            min = std::min((int32_t) data_[i],min);
        }
        return min;
    }
    ValueUnion agg_max() override{
        int32_t max = INT32_MIN;
        for(uint32_t i=0;i<data_.size();++i){
            max = std::max((int32_t)data_[i],max);
        }
        return max;
    }
    ValueUnion agg_avg() override{
        ValueUnion v =agg_sum();
        return ValueUnion(v.val_.num_/data_.size());
    }

    std::vector<T> data_;
};