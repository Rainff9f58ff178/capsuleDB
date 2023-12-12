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
void Chunk::insertFrom(Chunk* otherchunk,uint32_t idx){
    CHEKC_THORW(otherchunk->columns_.size() == columns_.size());
    for(uint32_t i=0;i<columns_.size();++i){
        columns_[i]->insertFrom(otherchunk->columns_[i].get(),idx);
    }
    metadata.rows_++;
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


void Chunk::MergeBlock(Chunk* other_chunk){
    __check_all_column_row_same();
    other_chunk->__check_all_column_row_same();
    uint32_t other = other_chunk->rows();
    CHEKC_THORW(columns_.size() == other_chunk->columns_.size());
    for(uint32_t i=0;i<columns_.size();++i){
        columns_[i]->MergeData(other_chunk->columns_[i].get());
    }
    metadata.rows_+= other;
}