#include "Execute/core/Chunk.h"

#include"common/Exception.h"


Chunk::Chunk(){

}
void Chunk::appendColumn(ColumnRef&& col){
    columns_.push_back(std::move(col));
    updateMetadata();
}
void Chunk::appendColumns(ColumnRefs&& cols){
    for(auto& col: cols){
        appendColumn(std::move(col));
    }
}
uint32_t Chunk::rows(){
    return columns_[0]->rows();
}

void Chunk::updateMetadata(){
    if(columns_.size() > 1){
        if(columns_.back()->rows() != metadata.rows_){
            throw Exception("LogicalError,Column rows doesnt equal");
        }
    }else {
        metadata.rows_ = columns_.back()->rows();
        metadata.colums_++;
    }
}

ChunkRef Chunk::cloneEmpty(){
    std::vector<ColumnRef> cols;
    for(auto& col : columns_){
        cols.push_back(col->clone());
    }
    ChunkRef new_chunk = std::make_shared<Chunk>();
    new_chunk->appendColumns(std::move(cols));
    return new_chunk;
}