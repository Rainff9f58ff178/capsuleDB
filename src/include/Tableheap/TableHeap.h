

#pragma once
#include "CataLog/Column.h"
#include "Tableheap/TableHeap.h"
#include <semaphore>
#include <string>
#include <memory>
#include <vector>
#include "CataLog/Pages.h"
#include "common/type.h"
#include "CataLog/PageIterator/PageIterator.h"
struct ColumnDef{
    explicit ColumnDef(ColumnDef&& col){
        column_name=std::move(col.column_name);
        col_idx_ =col.col_idx_;
        col_type_ = col.col_type_;
        col_length_ = col.col_length_;
    }
    explicit ColumnDef(const ColumnDef& col){
        column_name = col.column_name;
        col_idx_=col.col_idx_;
        col_type_ = col.col_type_;
        col_length_ = col.col_length_;
    }

    ColumnDef(std::string column_name,
    uint32_t col_idx_,
    ColumnType type,uint32_t variable_length):column_name(std::move(column_name)),
        col_idx_(col_idx_),
        col_type_(type),
        col_length_(variable_length){}
    std::string column_name;

    // column type is int for now .

    // ColumnType type_;
    // Constraint constraint_;
    uint32_t col_idx_;
    ColumnType col_type_;
    uint32_t col_length_;
};
class TableHeap;
class CataLog;
class TableCataLog;
class ColumnHeap{
    struct __metadata{
        uint32_t total_rows{0};
    };

public:
    static constexpr const uint32_t DATA_CHUNK_SIZE = 4096;
    explicit ColumnHeap(const ColumnDef& def,page_id_t heap_page_id,
    page_id_t static_page_id):def_(def),
        heap_page_id_(heap_page_id),
        heap_static_page_id_(static_page_id){}
    ColumnDef def_;
    
    page_id_t heap_page_id_;
    page_id_t heap_static_page_id_;

    __metadata metadata;

    CataLog* log_;

    std::unique_ptr<ColumnIterator> begin();
    std::unique_ptr<ColumnIterator> end();
};
class TableHeap{
public:
    TableHeap(std::vector<std::unique_ptr<ColumnHeap>>&& vec):
        columns_heap_(std::move(vec)){}
    std::vector<std::unique_ptr<ColumnHeap>> columns_heap_;
};