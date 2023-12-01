#pragma once




#include"CataLog/Column.h"
#include<vector>
#include "Tableheap/TableHeap.h"

class Schema{
public:

    explicit Schema(std::vector<Column> columns);
    Schema(Schema&& schema);
    Schema(const Schema& schema);
    Schema(const TableHeap* table_heap);
    Schema(const std::vector<ColumnDef>& cols);
    Schema(const std::vector<std::string>& cols);
    Schema() = default;
        

    void Merge(const Schema& other_schema);
    void AddColumn(const std::string& col_name);
    uint32_t GetColumnIdx(const std::string& name);
    std::vector<Column> columns_;
};
using SchemaRef = std::shared_ptr<Schema>;