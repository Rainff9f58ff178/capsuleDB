#include "CataLog/PageIterator/ColumnStringIterator.h"
#include "CataLog/TableCataLog.h"
#include "CataLog/CataLog.h"
#include "CataLog/Pages.h"



ColumnStringIterator::
ColumnStringIterator(uint32_t data_chunk_size,
ColumnHeapStringPage* page,
ColumnHeap* col_heap):
ColumnIterator(data_chunk_size,ColumnIteratorType::ColumnStringIterator,col_heap),
page_(page){
    if(page_){
        current_page_iterator_= page_->begin();
        DASSERT(page->GetColumnType()==ColumnType::STRING);
        operator++();
    }
}

bool ColumnStringIterator::operator!=(const ColumnIterator& other){
    return !(*this == other);
}

bool ColumnStringIterator::operator==(const ColumnIterator& other){
    auto& other_it = down_cast<const ColumnStringIterator&>(other);
    if(page_ == other_it.page_ && col_heap_ == other_it.col_heap_){
        return true;
    }
    return false;
}

void ColumnStringIterator::operator++(){
    if(current_page_iterator_ == page_->end() && page_->GetNextPageId() == NULL_PAGE_ID){
        //to the end .
        page_ =nullptr;
        return;
    }

    uint32_t chunk_size=0;
    for(;chunk_size<col_heap_->DATA_CHUNK_SIZE && 
        (current_page_iterator_!=page_->end() || page_->GetNextPageId() != NULL_PAGE_ID)
        ;++chunk_size,++current_page_iterator_){
        
        if(current_page_iterator_== page_->end()){
            auto& heap_handle  = col_heap_->log_->column_heap_handle_;
            auto next_page_id = page_->GetNextPageId();
            heap_handle.Unpin(page_->page_id,false);
            page_ = reinterpret_cast<ColumnHeapStringPage*>(heap_handle.GetPage(next_page_id));
            DASSERT(page_->GetColumnType() == ColumnType::STRING);
            current_page_iterator_ = page_->begin();
        }
        std::string val = *current_page_iterator_;
        cache_.push_back(std::move(val));
    }
    

}