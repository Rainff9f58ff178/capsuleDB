#pragma once

#include<cstdint>
#include<memory>
#include<vector>
#include"common/Exception.h"
#include "common/Value.h"
#define COLUMN_IMPOSSBLE throw Exception("Impossiable run to here");
#define COLUMN_FUNC_NOT_IMP throw NotImplementedException("Function Not imp yet");

class TableCataLog;

class ExecColumn;
using ColumnRef  =  std::shared_ptr<ExecColumn>;
using ColumnRefs =  std::vector<ColumnRef>;
enum ExecColumnType{
    ExecInt,
    ExecString
};
class ExecColumn{
public:
    uint32_t max_width_{0};
    std::string name_;
    ExecColumnType col_type_;


    bool sameAs(ExecColumn* other){
        if(col_type_== other->col_type_ && col_type_== ExecString)
            return true;
        return col_type_ == other->col_type_ && max_width_ == other->max_width_;
    }

    virtual uint32_t rows(){
        COLUMN_IMPOSSBLE;
    }
    virtual uint64_t HashAt(uint32_t idx){
        COLUMN_IMPOSSBLE;
    }

    virtual ValueUnion ValueAt(uint32_t row_id){
        COLUMN_IMPOSSBLE;
    }

    virtual void insertFrom(const ValueUnionView& value){
        COLUMN_IMPOSSBLE;
    }
    virtual void insertFrom(const ValueUnion& value){
        COLUMN_IMPOSSBLE;
    }
    virtual void insertFrom(ExecColumn* other,uint32_t idx){
        COLUMN_IMPOSSBLE;
    }
    virtual void MergeData(ExecColumn* other){
        COLUMN_IMPOSSBLE;
    }
    virtual void insertToTable(TableCataLog* table,column_idx_t col_idx){
        COLUMN_IMPOSSBLE;
    }
    virtual std::string toString(uint32_t row_idx){
        COLUMN_IMPOSSBLE;
    }
    virtual int compare_at(ExecColumn* other,uint32_t l_idx,uint32_t r_idx){
        COLUMN_IMPOSSBLE;
    }

    virtual ColumnRef GetByRowNumbers(RowNumbers& row_numer){
        COLUMN_IMPOSSBLE;
    }
    //find the max size of chars, stupid;   
    virtual uint32_t max_char_size(){
        COLUMN_IMPOSSBLE;
    }
    virtual ColumnRef clone(uint32_t row = 0){
        COLUMN_IMPOSSBLE
    }

    virtual ValueUnion agg_count(){
        COLUMN_IMPOSSBLE;
    }
    virtual ValueUnion agg_sum(){
        COLUMN_IMPOSSBLE;
    }
    virtual ValueUnion agg_min(){
        COLUMN_IMPOSSBLE;
    }
    virtual ValueUnion agg_max(){
        COLUMN_IMPOSSBLE;
    }
    virtual ValueUnion agg_avg(){
        COLUMN_IMPOSSBLE;
    }
};

