#include "CataLog/PageIterator/PageIterator.h"
#include "CataLog/TableCataLog.h"

ColumnIterator::ColumnIterator(uint32_t data_chunk_sIze,ColumnIteratorType type,ColumnHeap* heap):data_chunk_size_(data_chunk_sIze),type_(type),col_heap_(heap){
    total_rows_ = heap->metadata.total_rows;
}