#pragma once
#include"CataLog/PageIterator/PageIterator.h"
#include"CataLog/Pages.h" 


class ColumnHeap;
class ColumnNumIterator:public ColumnIterator{
public:
    ColumnNumIterator(uint32_t data_chnk_size,ColumnHeapNumPage* num_page,ColumnHeap* heap);
    ~ColumnNumIterator();

    bool operator==(const ColumnIterator& other_iterator) override;
    bool operator!=(const ColumnIterator& other_iterator) override;
    void operator++() override;

    std::vector<Value>& operator*(){
        return cache_;
    }
    

    ColumnHeapNumPage* page_;
    ColumnHeapNumPage::ColumnNumPageIterator current_page_iterator_;
    std::vector<Value> cache_; 
    

};