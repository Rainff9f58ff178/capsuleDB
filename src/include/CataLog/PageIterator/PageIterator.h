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
    ColumnIterator(uint32_t data_chunk_sIze,ColumnIteratorType type,ColumnHeap* heap);

    virtual bool operator==(const ColumnIterator& other_iterator) = 0;
    virtual bool operator!=(const ColumnIterator& other_iterator) = 0;
    virtual void operator++() = 0;
    uint32_t data_chunk_size_;
    ColumnIteratorType type_;
    ColumnHeap* col_heap_;

    //for debug
    uint32_t acumulate_rows_=0; // record returned rows
    uint32_t total_rows_ ;// record current time record number,if we just read untill empty,insert select will run forever;


};