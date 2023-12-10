



#pragma  once

#include "Binder/BoundTabRef.h"
#include "BufferManager.h"
#include "CataLog/Schema.h"
#include "CataLog/Pages.h"
#include <unordered_map>
#include "Tableheap/TableHeap.h"
#include "CataLog/TableCataLog.h"


//this class represent whole meta infomation of database;
/*
    include database ,column,
*/
/*
    table page layout

*/


class CataLog{
    static constexpr const page_id_t META_PAGE_ID = 1;
public:
    std::unordered_map<table_oid_t,std::unique_ptr<TableCataLog>> tables_;
    explicit CataLog(FileManager* file_manager,std::string database_name);
    ~CataLog();
    FileManager* file_manager_;
    FileHandle<DB_INFO_FILE_PAGE_SIZE> db_handle_;
    FileHandle<DB_COLUMN_HEAP_PAGE_SIZE> column_heap_handle_;
    
    
    std::string database_name_;

    
    
    
    void CreateTable(const std::string& table_name,const std::vector<ColumnDef>& columns);
    TableCataLog* GetTable(std::string table_name);
    TableCataLog* GetTable(table_oid_t table_oid);
private:
    void InitTableMetaInfo();
    void LoadAllTablesInformation();
    std::unique_ptr<TableCataLog> LoadTableInformation(uint32_t table_page_id);
    
    std::unique_ptr<TableHeap> LoadTableHeapInformation(PageReader<DB_INFO_FILE_PAGE_SIZE>& reader);
};