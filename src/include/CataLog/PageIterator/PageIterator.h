#pragma once

#include<cstdint>
#include<memory>
enum class ColumnIteratorType:uint8_t{
    ColumnNumIterator,
    ColumnStringIterator
};
class ColumnHeap;
class  ColumnIterator{
public:
    ColumnIterator(uint32_t data_chunk_sIze,ColumnIteratorType type,ColumnHeap* heap):data_chunk_size_(data_chunk_sIze),type_(type),col_heap_(heap){}
    virtual bool operator==(const ColumnIterator& other_iterator) = 0;
    virtual bool operator!=(const ColumnIterator& other_iterator) = 0;
    virtual void operator++() = 0;
    uint32_t data_chunk_size_;
    ColumnIteratorType type_;
    ColumnHeap* col_heap_;

    //for debug
    uint32_t acumulate_rows_; // record returned rows

};