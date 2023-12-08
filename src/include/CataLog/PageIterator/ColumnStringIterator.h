#pragma once
#include "CataLog/PageIterator/PageIterator.h"
#include "CataLog/Pages.h"

class ColumnHeap;
class ColumnStringIterator:public ColumnIterator{
public: 
    ColumnStringIterator(uint32_t data_chunk_size,ColumnHeapStringPage* page,ColumnHeap* col_heap);


    bool operator==(const ColumnIterator& other) override;
    bool operator!=(const ColumnIterator& other) override;
    void operator++() override;
    std::vector<String>& operator*(){
        return cache_;
    }
    ColumnHeapStringPage* page_;
    ColumnHeapStringPage::ColumnHeapStringPageIterator current_page_iterator_;
    std::vector<String> cache_;
};