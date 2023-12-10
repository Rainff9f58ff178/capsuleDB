



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
        columns_.emplace_back(std::move(Column(col->def_.column_name,column_heap->def_.col_type_)));
    }
}
void Schema::Merge(const Schema& other_schema){
    AddColumns(other_schema.columns_);
}
void Schema::AddColumns(const std::vector<Column>& cols ){
    for(auto& col:cols){
        AddColumn(col.name_,col.type_);
    }
}
void Schema::AddColumn(const std::string& col_name,ColumnType type){
    Column col(col_name,type);
    auto it = std::find(columns_.begin(),columns_.end(),col);
    if(it !=columns_.end())
        return;
    columns_.push_back(col);
}
void Schema::AddColumn(Column&& column){
    columns_.push_back(column);
}
void Schema::AddColumn(const Column& col){
    columns_.push_back(col);
}
bool Schema::exist(const Column& col){
    for(auto& c : columns_){
        if(c.name_ == col.name_){
            return true;
        }
    }
    return false;
}

void Schema::erase(const Column& col){
    for(auto it = columns_.begin();it!=columns_.end();++it){
        if(it->name_ == col.name_)    {
            columns_.erase(it);
            return;
        }
    }
}

Schema::Schema(const std::vector<ColumnDef>& cols){
    for(auto& col : cols){
        Column c(col.column_name,col.col_type_);
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



