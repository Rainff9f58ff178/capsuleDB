#include "CataLog/PageIterator/ColumnNumIterator.h"
#include "CataLog/CataLog.h"
#include "CataLog/Pages.h"




ColumnNumIterator::
ColumnNumIterator(uint32_t data_chunk_size_,ColumnHeapNumPage* page,ColumnHeap* col_heap):
ColumnIterator(data_chunk_size_,ColumnIteratorType::ColumnNumIterator,col_heap),page_(page){
    if(page_){
        current_page_iterator_ = page_->begin();
        DASSERT(page_->GetColumnType()==ColumnType::INT);
        operator++();
    }
}

bool ColumnNumIterator::operator!=(const ColumnIterator& other){
    return !(*this==other);
}
bool ColumnNumIterator::operator==(const ColumnIterator& other){
    auto& other_it = down_cast<const ColumnNumIterator&>(other);
    if(page_ != other_it.page_)
        return false;
    if(data_chunk_size_ != other.data_chunk_size_)
        return false;
    if(col_heap_!=other_it.col_heap_)
        return false;

    return true;
}
ColumnNumIterator::~ColumnNumIterator(){
    DASSERT(page_);
    auto* heap_handle = &col_heap_->log_->column_heap_handle_;
    SafeUnpin(*heap_handle,page_,false);
}
void ColumnNumIterator::operator++(){
    if(acumulate_rows_ == total_rows_){
        page_ = nullptr;
        return;
    }

    if(current_page_iterator_ == page_->end() && page_->GetNextPageId() == NULL_PAGE_ID){
        page_ = nullptr;
        return;
    }

    uint32_t chunk_num=0;
    for(; chunk_num< col_heap_->DATA_CHUNK_SIZE && acumulate_rows_ < total_rows_ &&
     (current_page_iterator_!=page_->end()|| page_->GetNextPageId()!=NULL_PAGE_ID ); chunk_num++, ++current_page_iterator_){
        if(current_page_iterator_==page_->end()){
            // current page to the end.
            auto* heap_handle = &col_heap_->log_->column_heap_handle_;
            auto next_page_id = page_->GetNextPageId();

            heap_handle->Unpin(page_->page_id,false);
            page_ = reinterpret_cast<ColumnHeapNumPage*>(heap_handle->GetPage(next_page_id));
            DASSERT(page_->GetColumnType() == ColumnType::INT);
            current_page_iterator_ = page_->begin();
        }
        Value val = *current_page_iterator_;
        cache_.push_back(val);
        acumulate_rows_++;
    }
}