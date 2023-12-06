#include "Tableheap/TableHeap.h"
#include"CataLog/PageIterator/ColumnNumIterator.h"
#include "CataLog/PageIterator/ColumnStringIterator.h"
#include "CataLog/CataLog.h"

std::unique_ptr<ColumnIterator> ColumnHeap::begin(){
    auto* heap_handle =  &log_->column_heap_handle_;
    auto* page = heap_handle->GetPage(heap_page_id_);
    if(def_.col_type_==ColumnType::INT){
        return std::make_unique<ColumnNumIterator>(DATA_CHUNK_SIZE,reinterpret_cast<ColumnHeapNumPage*>(page),this);
    }else if(def_.col_type_==ColumnType::STRING){
        return std::make_unique<ColumnStringIterator>(DATA_CHUNK_SIZE,reinterpret_cast<ColumnHeapStringPage*>(page),this);
    }
    UNREACHABLE
}
std::unique_ptr<ColumnIterator> ColumnHeap::end(){
    auto* heap_handle = &log_->column_heap_handle_;
    auto* page = heap_handle->GetPage(heap_page_id_);
    if(def_.col_type_==ColumnType::INT){
        return std::make_unique<ColumnNumIterator>(DATA_CHUNK_SIZE,nullptr,this);
    }else if(def_.col_type_==ColumnType::STRING){
        return std::make_unique<ColumnStringIterator>(DATA_CHUNK_SIZE,nullptr,this);
    }
    UNREACHABLE
}