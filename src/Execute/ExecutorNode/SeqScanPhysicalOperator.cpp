#include "Execute/ExecutorNode/SeqScanPhysicalOperator.h"
#include "Planer/SeqScanLogicalOperator.h"
#include "Execute/ExecuteContext.h"
#include "CataLog/PageIterator/ColumnNumIterator.h"
#include "CataLog/PageIterator/ColumnStringIterator.h"


SourceResult
SeqScanPhysicaloperator::Source(ChunkRef& chunk){
    auto& plan = GetPlan()->Cast<SeqScanLogicalOperator&>();
    

    if(  ( (*column_iterators_[0]->col_heap_->end()) == *column_iterators_[0])){
#ifndef  NDEBUG
        for(auto& it : column_iterators_){
            CHEKC_THORW( (*it == (*it->col_heap_->end())));
        }
#endif
        return  SourceResult::FINISHED;
    }

    ChunkRef new_chunk = std::make_shared<Chunk>();
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
    // filter new_chunk
    if(plan.pridicator_.size()>1)
        throw Exception("Filter pridicator num > 2 ");

    if(!plan.pridicator_.empty()){
        auto p = plan.pridicator_[0];
        std::vector<ValueUnion> new_value;
        for(uint32_t i=0;i<new_chunk->rows();++i){
            new_value.push_back(p->Evalute(&new_chunk,i).clone());
        }
        if(new_value.empty()){
            throw Exception("filter empty ,what is it?");
        }
        if(new_value[0].type_== ValueType::TypeString){
            // if expr result is string /
            new_value.clear();
            for(uint32_t i=0;i<new_chunk->rows();++i){
                new_value.emplace_back(1);
            }
        }
        auto filter = std::dynamic_pointer_cast<ColumnVector<int32_t>>(ColumnFactory::CreateColumn(new_value));
        auto& data = filter->data_;
        CHEKC_THORW(data.size() == new_chunk->rows());
        auto filterd_chunk = new_chunk->cloneEmpty();
        for(uint32_t i=0;i<data.size();++i){
            if(static_cast<bool>(data[i])){
                filterd_chunk->insertFrom(new_chunk.get(),i);
            }
        }
        chunk = std::move(filterd_chunk);
    }else{
        chunk = std::move(new_chunk);
    }
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

SeqScanPhysicaloperator::~SeqScanPhysicaloperator(){
}

SinkResult
SeqScanPhysicaloperator::Sink(ChunkRef& chunk){
    throw NotImplementedException("reach Impossiable");
}
OperatorResult
SeqScanPhysicaloperator::Execute(ChunkRef& chunk){
    throw NotImplementedException("reach Impossiable");
}
