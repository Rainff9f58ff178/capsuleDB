#include "Execute/ExecutorNode/SeqScanPhysicalOperator.h"
#include "Planer/SeqScanLogicalOperator.h"
#include "Execute/ExecuteContext.h"
#include "CataLog/PageIterator/ColumnNumIterator.h"
#include "CataLog/PageIterator/ColumnStringIterator.h"


SourceResult
SeqScanPhysicaloperator::Source(ChunkRef& chunk){
    if(  ( (*column_iterators_[0]->col_heap_->end()) == *column_iterators_[0])){
#ifndef  NDEBUG
        for(auto& it : column_iterators_){
            CHEKC_THORW( (*it == (*it->col_heap_->end())));
        }
#endif
        return  SourceResult::FINISHED;
    }

    ChunkRef new_chunk = std::make_shared<Chunk>();
    auto& plan = GetPlan()->Cast<SeqScanLogicalOperator&>();
    DASSERT(column_iterators_.size() == plan.input_schema_->columns_.size());

    auto _columns = plan.input_schema_->columns_;
    for(uint32_t i=0; i<_columns.size();++i){
        auto& col = _columns[i];
        if(col.type_ == ColumnType::INT){
            auto _col = std::make_shared<ColumnVector<int32_t>>(col.name_);
            auto& column_num_iterator = down_cast<ColumnNumIterator&>(*column_iterators_[i].get());
            _col->data_ = std::move(*column_num_iterator);
            ++column_num_iterator;
            new_chunk->appendColumn(std::move(_col));
        }else if(col.type_ == ColumnType::STRING){
            auto _col = std::make_shared<ColumnString>(col.name_);
            auto& column_string_iterator = down_cast<ColumnStringIterator&>(*column_iterators_[i].get());
            _col->data_ = std::move(*column_string_iterator);
            ++column_string_iterator;
            new_chunk->appendColumn(std::move(_col));
        }else {
            UNREACHABLE
        }
    }
    chunk = std::move(new_chunk);
    return SourceResult::HAVE_MORE;
}

void SeqScanPhysicaloperator::source_init(){
    auto& plan = GetPlan()->Cast<SeqScanLogicalOperator>();
    auto* tb_cata_log = context_->cata_log_->GetTable(plan.table_oid_);
    std::vector<ColumnRef> cols;
    for(auto& col : plan.input_schema_->columns_){
        auto* col_heap = tb_cata_log->GetColumnHeapByName(col.name_);
        THROW_IF_NULL(col_heap);
        column_iterators_.push_back(col_heap->get()->begin());

    }
}
void SeqScanPhysicaloperator::source_uninit(){

}



SinkResult
SeqScanPhysicaloperator::Sink(ChunkRef& chunk){
    throw NotImplementedException("reach Impossiable");
}
OperatorResult
SeqScanPhysicaloperator::Execute(ChunkRef& chunk){
    throw NotImplementedException("reach Impossiable");
}
