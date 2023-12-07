#pragma once




#include"CataLog/Column.h"
#include<vector>
#include "Tableheap/TableHeap.h"
#include<optional>

class Schema;
using SchemaRef = std::shared_ptr<Schema>;

class Schema{
public:

    explicit Schema(std::vector<Column> columns);
    Schema(Schema&& schema);
    Schema(const Schema& schema);
    Schema(const TableHeap* table_heap);
    Schema(const std::vector<ColumnDef>& cols);
    Schema() = default;
    
    std::optional<Column> getColumnByname(const std::string& name){
        for(auto& col : columns_){
            if(col.name_ == name)
                return col;
        }
        return Column();
    }

    SchemaRef Copy(){
        return std::make_shared<Schema>(*this);
    }
    bool exist(const Column& col);
    void erase(const Column& col);
    void Merge(const Schema& other_schema);
    void AddColumn(const std::string& col_name,ColumnType type);
    void AddColumn(Column&& col);
    void AddColumn(const Column& col);
    void AddColumns(const std::vector<Column>& cols );
    uint32_t GetColumnIdx(const std::string& name);
    std::vector<Column> columns_;
};