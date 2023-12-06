#include "CataLog/CataLog.h"
#include "CataLog/Pages.h"
#include "Tableheap/TableHeap.h"
#include "common/Exception.h"
#include "pg_definitions.hpp"







CataLog::CataLog(FileManager* file_manager,std::string database_name):
file_manager_(file_manager),database_name_(std::move(database_name)){
    //first open column heap file 
    auto [column_handle,is_first_load] =
        file_manager_->open
        <DB_COLUMN_HEAP_PAGE_SIZE>(database_name_+".columnHeap",
         std::ios::in | std::ios::out | std::ios::binary,80);
    
    column_heap_handle_= column_handle;
    // column_heap hasn't meta infomation .

    
    InitTableMetaInfo();
}
void
CataLog::InitTableMetaInfo(){
     auto [db_handle,is_first_load] =
         file_manager_->open<DB_INFO_FILE_PAGE_SIZE>(database_name_+"db",
        std::ios::in | std::ios::out | std::ios::binary,
    10);
    db_handle_ = db_handle;
    page_id_t page_id;
    if(is_first_load){
        // the second page is use for CatalogMetaPage
        auto* page = db_handle.GetNewPage(&page_id);
        assert(page->page_id==1);
        db_meta_header_page_ = reinterpret_cast<CataLogMetaPage*>(page);
        db_meta_header_page_->SetLSN(-1);
        db_meta_header_page_->SetPageIdToData(db_meta_header_page_->page_id);
        db_meta_header_page_->SetPrevPageId(NULL_PAGE_ID);
        db_meta_header_page_->SetNextPageId(NULL_PAGE_ID);
        db_meta_header_page_->SetOffset(CataLogMetaPage::TABLE_NUMS+4);
        db_meta_header_page_->SetTableNum(0);
    }else{
        auto* page = db_handle.GetPage(1);
        db_meta_header_page_ = reinterpret_cast<CataLogMetaPage*>(page);
        LoadAllTablesInformation();
    }
}

void CataLog::CreateTable(const std::string& table_name,const std::vector<ColumnDef>& columns){
    for(auto& it:tables_){
        auto* table_catalog = it.second.get();
        if(table_catalog->table_name_ == table_name){
            throw Exception("Create table failed,"+table_name+" exist already");
        }   
    }
    page_id_t page_id;
    //set table_page
    auto* table_page = reinterpret_cast<TableCataLogPage*>(db_handle_.GetNewPage(&page_id));
    table_page->SetNextPageId(NULL_PAGE_ID);
    table_page->SetPrevPageId(NULL_PAGE_ID);
    table_page->SetLSN(0);
    table_page->SetPageIdToData(page_id);
    PageWrite<DB_INFO_FILE_PAGE_SIZE> writer(table_page->GetData());
    writer.Write<16>(nullptr);
    char tb_name_buf[128];
    memset(tb_name_buf,0,sizeof(tb_name_buf));
    memcpy(tb_name_buf,table_name.c_str(),
    (uint32_t)sizeof(table_name.c_str()));

    writer.Write<128>(tb_name_buf);
    writer.Write<page_id_t>(page_id);
    writer.Write<uint32_t>(columns.size());
    char buf[TableCataLogPage::COLUMN_DEF_LENGTH];
    std::vector<std::unique_ptr<ColumnHeap>> v;
    for(uint32_t i=0 ;i<columns.size() ;++i){
        memset(buf,0,sizeof(buf));
        auto& column = columns[i];
        assert(i==column.col_idx_);
        auto t_col_name = column.column_name;
        memcpy(buf,t_col_name.c_str(),(uint32_t)sizeof(column.column_name.c_str()));
        *(uint32_t*)(buf+64) = column.col_type_;  //column type
        *(uint32_t*)(buf+68) = column.col_length_; // ColumnLeng
        *(uint32_t*)(buf+72)=NULL_PAGE_ID; //ColumnHeap
        *(uint32_t*)(buf+76)=NULL_PAGE_ID;  //ColumnStaticHeap
        writer.Write<TableCataLogPage::COLUMN_DEF_LENGTH>(buf);
        v.push_back(std::make_unique<ColumnHeap>(column,*(uint32_t*)(buf+72),
            *(uint32_t*)(buf+76)));
        v.back()->log_ = this;
    }

    Schema schema(columns);
    auto tca_log = std::make_unique<TableCataLog>(table_page->page_id,  
        std::move(schema),table_name,
      std::make_unique<TableHeap>(std::move(v)),column_heap_handle_,this);

    tables_.insert({table_page->page_id,std::move(tca_log)});
    db_meta_header_page_->AddTable(table_page->page_id);
    db_handle_.Unpin(table_page->page_id, true);
}

void CataLog::LoadAllTablesInformation(){
    PageReader<DB_INFO_FILE_PAGE_SIZE> reader(db_meta_header_page_->GetData());
    reader.Read<8>(nullptr); //needn't for now;
    char buf[4];
    reader.Read<4>(buf);
    assert(*(uint32_t*)buf == NULL_PAGE_ID);
    uint32_t np= reader.Read<uint32_t>();
    assert(np=NULL_PAGE_ID && "doestn't support multi page catalog for now");

    reader.Read<8>(nullptr);

    auto table_nums = db_meta_header_page_->GetTableNum();
    for(uint32_t i = 0;i<table_nums;++i){
        auto table_page_id = reader.Read<uint32_t>();
        auto table_catalog  = std::move(LoadTableInformation(table_page_id));
        tables_.insert({table_catalog->table_oid_,std::move(table_catalog)});
    }
}

std::unique_ptr<TableCataLog> CataLog::LoadTableInformation(uint32_t table_page_id){
    auto* table_page= 
            reinterpret_cast<TableCataLogPage*>(db_handle_.GetPage(table_page_id));
    
    PageReader<DB_INFO_FILE_PAGE_SIZE> reader(table_page->GetData());
    reader.Read<8>(nullptr);
    D_assert(reader.Read<uint32_t>() == -1 && "not support multi table page for now");
    D_assert(reader.Read<uint32_t>() == -1 && "not support multi table page for now");
    char table_name[128];
    reader.Read<128>(table_name);
    // table_oid is his page_id
    uint32_t table_oid = reader.Read<uint32_t>();
    assert(table_oid == table_page->page_id);
    auto table_heap = LoadTableHeapInformation(reader);
    Schema schema(table_heap.get());

    return std::make_unique<TableCataLog>(table_oid,std::move(schema),std::string(table_name),
        std::move(table_heap),column_heap_handle_,this);
}
std::unique_ptr<TableHeap> CataLog::LoadTableHeapInformation(PageReader<DB_INFO_FILE_PAGE_SIZE>& reader){
    std::vector<std::unique_ptr<ColumnHeap>> vec;
    auto column_nums = reader.Read<uint32_t>();
    for(uint32_t i =0;i<column_nums ; ++i){
        char buf[TableCataLogPage::COLUMN_DEF_LENGTH];
        reader.Read<TableCataLogPage::COLUMN_DEF_LENGTH>(buf);
        auto type = (ColumnType)(*(buf+64));
        auto col_heap = std::make_unique<ColumnHeap>(ColumnDef(std::string(buf),i,type,*(uint32_t*)(buf+68)),*(uint32_t*)(buf+72),*(uint32_t*)(buf+76));
        if(col_heap->heap_static_page_id_ != NULL_PAGE_ID){
            auto* static_page = reinterpret_cast<ColumnHeapStaticPage*>(column_heap_handle_.GetPage(col_heap->heap_static_page_id_));
            col_heap->metadata.total_rows = static_page->GetTotalObj();
            column_heap_handle_.Unpin(static_page->page_id,false);
        }else {
            col_heap->metadata.total_rows=0;
        }
        col_heap->log_ = this;
        vec.emplace_back(std::move(col_heap));
    }
    return std::make_unique<TableHeap>(std::move(vec));
}

CataLog::~CataLog(){
    db_handle_.Unpin(db_meta_header_page_->page_id,true);
}
TableCataLog*
CataLog::GetTable(std::string table_name){
    for(auto& en:tables_){
        if(en.second->table_name_== table_name){
            return en.second.get();
        }
    }
    return nullptr;
}
TableCataLog*
CataLog::GetTable(table_oid_t table_oid){
    auto& table = tables_[table_oid];
    return table.get();
}