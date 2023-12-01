



#include "CataLog/Schema.h"
#include "parser/scanner.hpp"




Schema::Schema(std::vector<Column> columns):columns_(std::move(columns)){}

Schema::Schema(Schema&& schema){
    columns_ = std::move(schema.columns_);
}
Schema::Schema(const Schema& schema){
    columns_ = schema.columns_;
}
Schema::Schema(const TableHeap* table_heap){
    for(auto& column_heap :table_heap->columns_heap_){
        auto* col = column_heap.get();
        columns_.emplace_back(std::move(Column(col->def_.column_name,col->def_.col_idx_*4)));
    }
}
void Schema::Merge(const Schema& other_schema){
    columns_.insert(
        columns_.begin(),other_schema.columns_.begin(),
        other_schema.columns_.end());
}
void Schema::AddColumn(const std::string& col_name){
    auto it = std::find(columns_.begin(),columns_.end(),col_name);
    if(it !=columns_.end())
        return;
    columns_.push_back(Column(col_name));
}



Schema::Schema(const std::vector<ColumnDef>& cols){
    for(auto& col : cols){
        Column c(col.column_name,4*col.col_idx_);
        columns_.push_back(std::move(c));
    }
}
Schema::Schema(const std::vector<std::string>& cols){
    for(uint32_t i=0;i<cols.size();++i){
        Column c(cols[i],i*4);
        columns_.push_back(std::move(c));
    }
}

uint32_t Schema::GetColumnIdx(const std::string& name){
    for(uint32_t i =0;i<columns_.size();++i){
        if(columns_[i].name_==name)
            return i;
    }
    return UINT32_MAX;
}



