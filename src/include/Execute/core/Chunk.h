#pragma once
#include<vector>
#include"Column.h"
#include<cstring>


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
    void insertFrom(Chunk* otherchunk,uint32_t idx);
    uint32_t rows();
    uint32_t columns(){
        return columns_.size();
    }
    ChunkRef cloneEmpty();
    

    __metadata metadata;
    ColumnRefs columns_;
};

