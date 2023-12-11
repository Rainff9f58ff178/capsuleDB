#pragma once
#include"Execute/core/Column.h"



class ColumnString:public ExecColumn{
    friend class ColumnFactory;
public:
    
    ColumnString(){
        col_type_ = ExecString;
    }
    ColumnString(const std::string& name){
        name_=name;
        col_type_ = ExecString;
    }
    uint32_t rows() override{
        return data_.size();
    }

    ValueUnion ValueAt(uint32_t row_id) override{
        CHEKC_THORW(row_id < data_.size());
        return ValueUnion(data_[row_id].data(),data_[row_id].size());
    }
    uint64_t HashAt(uint32_t idx) override;
    void insertFrom(const ValueUnionView& value) override;
    void insertFrom(ExecColumn* other,uint32_t idx) override;
    void insertToTable(TableCataLog* table,uint32_t col_idx) override;
    std::string toString(uint32_t row_idx) override;

    //find the max size of chars, stupid;
    uint32_t max_char_size() override;
    
    ColumnRef clone(uint32_t row = 0) override{
        auto new_col = std::make_shared<ColumnString>(name_);
        new_col->max_width_ = max_width_;
        for(uint32_t i=0;i<row;++i){
            new_col->data_[i] = data_[i];
        }
        return new_col;
    }
    std::vector<std::string> data_;
};




