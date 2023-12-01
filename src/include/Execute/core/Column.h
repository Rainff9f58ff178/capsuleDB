#pragma once

#include<cstdint>
#include<memory>
#include<vector>
#include"common/Exception.h"
#include "common/Value.h"
#define COLUMN_IMPOSSBLE throw Exception("Impossiable run to here");
#define COLUMN_FUNC_NOT_IMP throw NotImplementedException("Function Not imp yet");

class TableCataLog;


enum ExecColumnType{
    ExecInt,
    ExecString
};
class ExecColumn{
public:
    uint32_t max_width_{0};
    std::string name_;
    ExecColumnType col_type_;

    virtual uint32_t rows(){
        COLUMN_IMPOSSBLE;
    }

    virtual void insertFrom(const ValueUnionView& value){
        COLUMN_IMPOSSBLE;
    }

    virtual void insertToTable(TableCataLog* table,uint32_t col_idx){
        COLUMN_IMPOSSBLE;
    }
    virtual std::string toString(uint32_t row_idx){
        COLUMN_IMPOSSBLE;
    }


    //find the max size of chars, stupid;
    virtual uint32_t max_char_size(){
        COLUMN_IMPOSSBLE;
    }
};

using ColumnRef  =  std::shared_ptr<ExecColumn>;
using ColumnRefs =  std::vector<ColumnRef>;