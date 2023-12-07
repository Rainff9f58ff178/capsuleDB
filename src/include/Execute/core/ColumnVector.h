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
    }
    ColumnVector(const std::string& name){
        name_ = name;
        max_width_ = sizeof(T);
        col_type_ = ExecInt;
    }
    uint32_t rows()override{
        return data_.size();
    }
    ValueUnion ValueAt(uint32_t idx) override{
        CHEKC_THORW(idx<data_.size());
        return ValueUnion(data_[idx]);
    }
    void insertFrom(const ValueUnionView& value) override{
        for(uint32_t i=0;i<value.size();++i){
            DASSERT(value[i].type_ == TypeInt);
            data_.push_back(value[i].num_);
        }
    }
   

    void insertToTable(TableCataLog* table,uint32_t col_idx) override{
        for(uint32_t i=0;i<data_.size();++i){
            table->Insert(col_idx,data_[i]);
        }
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

    std::vector<T> data_;
};