

#pragma once
#include "CataLog/Column.h"
#include "Tableheap/TableHeap.h"
#include <semaphore>
#include <string>
#include <memory>
#include <vector>
#include "CataLog/Pages.h"
#include "common/type.h"
struct ColumnDef{
    explicit ColumnDef(ColumnDef&& col){
        column_name=std::move(col.column_name);
        col_idx_ =col.col_idx_;
    }
    explicit ColumnDef(const ColumnDef& col) = default;

    ColumnDef(std::string column_name,uint32_t col_idx_,ColumnType type):column_name(std::move(column_name)),
        col_idx_(col_idx_),
        col_type_(type){}
    std::string column_name;

    ColumnType col_type_{INT};
    // column type is int for now .

    // ColumnType type_;
    // Constraint constraint_;
    uint32_t col_idx_;

};
class ColumnHeap{

    struct __metadata{
        uint32_t total_obj{0};
    };
public:
    explicit ColumnHeap(const ColumnDef& def,page_id_t heap_page_id,
    page_id_t static_page_id):def_(def),
        heap_page_id_(heap_page_id),
        heap_static_page_id_(static_page_id){}
    ColumnDef def_;
    
    page_id_t heap_page_id_;
    page_id_t heap_static_page_id_;

    __metadata metadata;
};
class TableHeap{
public:
    TableHeap(std::vector<std::unique_ptr<ColumnHeap>>&& vec):
        columns_heap_(std::move(vec)){}
    std::vector<std::unique_ptr<ColumnHeap>> columns_heap_;

};