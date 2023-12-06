#pragma once
#include "Schema.h"
#include "common/type.h"
#include "common/Value.h"
#include <optional>

class CataLog;
class TableCataLog{
public:
    explicit TableCataLog(table_oid_t table_oid,Schema schema,
        std::string table_name,
        std::unique_ptr<TableHeap> table_heap,
        FileHandle<DB_COLUMN_HEAP_PAGE_SIZE> heap_handle,
        CataLog* cata_log):
            table_oid_(table_oid),
            schema_(std::move(schema)),
            table_name_(std::move(table_name)),
            table_heap_(std::move(table_heap)),
            heap_handle_(heap_handle),
            cata_log_(cata_log){}
    
    
    inline Schema GetSchema(){
        return schema_;   
    }
    inline SchemaRef GetSchemaRef(){
        return std::make_shared<Schema>(schema_);
    }
    
    /*
        Get DatChnk by this bitmap ,DataChunksize should be 4096
    */



    std::vector<Value> GetNumDataChunk(uint32_t col_idx);
    std::vector<Value>
    __GetNumDataChunk(ColumnHeapNumPage* page);
    std::vector<String> GetStringDataChunk(uint32_t col_idx);
    std::vector<String>
    __GetStringDataChunk(ColumnHeapStringPage* page);

    /*
        this function retrun all the column to eavlute,
    GetAllData once time waste memory,use iterator take up 
    the lock.
    */
    std::vector<ValueUnion>
    GetDataChunk(uint32_t col_idx);

    std::optional<Column> GetColumnFromSchema(const std::string& col_name){
        for(auto& col : schema_.columns_){
            if(col.name_ == col_name)
                return col;
        }
        return std::nullopt;
    }
    std::unique_ptr<ColumnHeap>* GetColumnHeapByName(const std::string& name);
    
    

    void Insert(uint32_t col_idx,Value val);
    void Insert(uint32_t col_idx,String& val);
    void InsertNum(Value val,ColumnHeapNumPage* page,uint32_t row_idx);
    void InsertString(String& val,ColumnHeapStringPage* page,uint32_t row_idx);

    uint32_t GetTotalObj(){
        DASSERT(table_heap_->columns_heap_.size() > 0);
        return table_heap_->columns_heap_[0]->metadata.total_rows;
    }
    TableCataLog() = delete;
    CataLog* cata_log_;
    table_oid_t table_oid_;
    Schema schema_;
    std::string table_name_;
    std::unique_ptr<TableHeap> table_heap_;
    FileHandle<DB_COLUMN_HEAP_PAGE_SIZE> heap_handle_;
private:
    template<ColumnType type>
    ColumnHeapPage* 
    CreateNewColumnHeapPage();


    ColumnHeapStaticPage* CreateNewColumnStaticPage(ColumnType type);

    
};