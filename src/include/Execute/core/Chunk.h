#pragma once
#include<vector>
#include"Column.h"
#include<cstring>
#include<unordered_map>

class Chunk;

using ChunkRef = std::shared_ptr<Chunk>;

class Chunk{

    struct __metadata{
        uint32_t rows_{0};
        uint32_t colums_{0};
    };


public:
    Chunk();
    
    void appendColumn(ColumnRef&& col);
    void appendColumns(ColumnRefs&& cols);
    void updateMetadata();
    ColumnRef getColumnByname(const std::string& col_name){
        for(auto& col : columns_){
            if(col->name_ == col_name)
                return col;
        }
        return nullptr;
    }
    void eraseColumn(const std::string& col_name){
        for(auto it = columns_.begin() ; it!= columns_.end();++it){
            if(it->get()->name_ == col_name){
                columns_.erase(it);
                return;
            }
        }

    }
    void insertFrom(const ValueUnionView& value){
        CHEKC_THORW(value.size() == columns_.size());
        __check_all_column_row_same();
        for(uint32_t i=0;i<value.size();++i){
            columns_[i]->insertFrom(value[i]);
        }
        __check_all_column_row_same();
        metadata.rows_++;
    }
    void MergeBlock(Chunk* other_chunk);
    void insertFrom(Chunk* otherchunk,uint32_t idx);
    uint32_t rows();
    uint32_t columns(){
        return columns_.size();
    }

    void __check_all_column_row_same(){
        
#ifndef  NDEBUG
        if(columns_.empty())
            return;
        uint32_t row = columns_[0]->rows();
        for(auto& col : columns_){
            CHEKC_THORW(col->rows() == row);
        }
#endif
    }

    ChunkRef cloneEmpty();
    
    __metadata metadata;
    ColumnRefs columns_;
    std::unordered_map<std::string,ColumnRef> map_;
};

