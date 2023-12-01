#include "CataLog/TableCataLog.h"
#include "CataLog/CataLog.h"
#include "common/Exception.h"
#include <ranges>




ColumnHeapStaticPage* TableCataLog::CreateNewColumnStaticPage(ColumnType type){
    page_id_t page_id_;
    auto* page = reinterpret_cast<ColumnHeapStaticPage*> (heap_handle_.GetNewPage(&page_id_));
    page->SetColumnType(type);
    page->SetTotalObj(0);
    page->SetTotalMaxValue(INT32_MIN);
    page->SetTotalSumValue(0);
    page->SetTotalMinValue(INT32_MAX);
    auto pid = page->page_id;
    heap_handle_.Unpin(page->page_id,true);
    return reinterpret_cast<ColumnHeapStaticPage*>(heap_handle_.GetPage(pid));
}

void TableCataLog::InsertString(String& val,ColumnHeapStringPage* page,uint32_t row_idx){
    auto* current_page = page;
    while(!current_page->HasSpace(val.size())){
        auto next_page_id = current_page->GetNextPageId();
        ColumnHeapStringPage* next_page =nullptr;
        if(next_page_id == NULL_PAGE_ID){
            next_page  = reinterpret_cast<ColumnHeapStringPage*>(CreateNewColumnHeapPage<ColumnType::STRING>());
        }else {
            next_page = reinterpret_cast<ColumnHeapStringPage*>(heap_handle_.GetPage(next_page_id));
        }
        heap_handle_.Unpin(current_page->page_id,false);
        current_page = next_page;
        
    }
    // now current_page has enough space
    current_page->Append(val,row_idx);
    heap_handle_.Unpin(current_page->page_id,true);
}   

void TableCataLog::Insert(uint32_t col_idx,String& val){
    auto& col_heap = table_heap_->columns_heap_[col_idx];
    DASSERT(col_heap->def_.col_type_ == ColumnType::STRING);
    ColumnHeapStringPage* string_page=nullptr;
    ColumnHeapStaticPage* static_page = nullptr;
    if(col_heap->heap_page_id_ == NULL_PAGE_ID){
        string_page = reinterpret_cast<ColumnHeapStringPage*>(CreateNewColumnHeapPage<ColumnType::STRING>());
        //create static page .
        static_page = CreateNewColumnStaticPage(ColumnType::STRING);

        auto* table_page = reinterpret_cast<TableCataLogPage*>(
            cata_log_->db_handle_.GetPage(table_oid_));
        table_page->SetColumnHeapPage(col_idx,string_page->page_id);
        table_page->SetColumnStaticPage(col_idx,static_page->page_id);

        col_heap->heap_page_id_ = string_page->page_id;
        col_heap->heap_static_page_id_ = static_page->page_id;

        heap_handle_.Unpin(table_page->page_id,true);
    }else {
        string_page  = reinterpret_cast<ColumnHeapStringPage*>(
            heap_handle_.GetPage(col_heap->heap_page_id_));
        
        static_page = reinterpret_cast<ColumnHeapStaticPage*>(
            heap_handle_.GetPage(col_heap->heap_static_page_id_));

        DASSERT(string_page);
        DASSERT(static_page);
    }
    uint32_t total_rows = static_page->GetTotalObj();
    InsertString(val,string_page,total_rows);
    static_page->SetTotalObj(total_rows+1);
    heap_handle_.Unpin(static_page->page_id,true);
    col_heap->metadata.total_rows = total_rows+1;
}


void 
TableCataLog::Insert(uint32_t col_idx,Value val){
    auto& col_heap = table_heap_->columns_heap_[col_idx];
    DASSERT(col_heap->def_.col_type_ == ColumnType::INT);

    ColumnHeapPage* colunm_heap_page = nullptr;
    ColumnHeapStaticPage* static_page = nullptr;

    if(col_heap->heap_page_id_==NULL_PAGE_ID){
        //first_insert ,create a new Page;
        
        colunm_heap_page = CreateNewColumnHeapPage<ColumnType::INT>();
        static_page = CreateNewColumnStaticPage(ColumnType::INT);

        auto* table_page = 
            reinterpret_cast<TableCataLogPage*>
            (cata_log_->db_handle_.GetPage(table_oid_));
        table_page->SetColumnHeapPage(col_idx,colunm_heap_page->page_id);
        cata_log_->db_handle_.Unpin(table_page->page_id,true);
        col_heap->heap_page_id_ = colunm_heap_page->page_id;

    }else{
        colunm_heap_page = 
        reinterpret_cast<ColumnHeapPage*>(heap_handle_.GetPage(col_heap->heap_page_id_));
        D_assert(colunm_heap_page);
    }
    

    auto total_rows = static_page->GetTotalObj();
    InsertNum(val,
    reinterpret_cast<ColumnHeapNumPage*>(colunm_heap_page),total_rows);
    static_page->SetTotalObj(total_rows+1);
    col_heap->metadata.total_rows = total_rows+1;


}

template<ColumnType type>
ColumnHeapPage* 
TableCataLog::CreateNewColumnHeapPage(){
    if(type==ColumnType::INT){
        page_id_t page_id;
        auto* page =  
        reinterpret_cast<ColumnHeapNumPage*> (heap_handle_.GetNewPage(&page_id));
        page->SetFreeSpaceOffset(ColumnHeapNumPage::VALUE_BEGIN);
        page->SetBeginRowId(NULL_PAGE_ID);
        page->SetEndRowId(NULL_PAGE_ID);
        page->SetLSN(NULL_LSN_NUM);
        page->SetPageIdToData(page->page_id);
        page->SetPrevPageId(NULL_PAGE_ID);
        page->SetNextPageId(NULL_PAGE_ID);
        page->SetColumnType(ColumnType::INT);
        // TODO(wxy):for now ,only support 4 byte type int.
        page->SetColumnLength(4);
        page->SetTotalObj(0);
        page->SetLocalMaxValue(INT32_MIN);
        page->SetLocalSumValue(0);
        page->SetLocalMinValue(INT32_MAX);
        
        heap_handle_.Unpin(page_id,true);

        return reinterpret_cast<ColumnHeapPage*>
            (heap_handle_.GetPage(page_id));
    }else {
        //String
        page_id_t page_id;
        auto* page =  reinterpret_cast<ColumnHeapStringPage*>(heap_handle_.GetNewPage(&page_id));
        page->SetFreeSpacePointerOffset(DB_COLUMN_HEAP_PAGE_SIZE);
        page->SetBeginRowId(NULL_PAGE_ID);
        page->SetEndRowId(NULL_PAGE_ID);
        page->SetLSN(NULL_LSN_NUM);
        page->SetPageIdToData(page->page_id);
        page->SetPrevPageId(NULL_PAGE_ID);
        page->SetColumnType(ColumnType::STRING);
        page->SetColumnLength(UINT32_MAX);
        page->SetTotalObj(0);
        page->SetLocalMaxValue(INT32_MAX);
        page->SetLocalSumValue(0);
        page->SetLocalMinValue(INT32_MAX);
        page->SetSlotPointerOffset(ColumnHeapStringPage::FIRST_SLOT_OFFSET);
        page->SetSlotNum(0);
        heap_handle_.Unpin(page_id,true);

        return reinterpret_cast<ColumnHeapPage*>(heap_handle_.GetPage(page_id));
    }

}
void 
TableCataLog::InsertNum(Value val,ColumnHeapNumPage* page,uint32_t row_idx){
    ColumnHeapNumPage* current_page = page;
    uint32_t total_obj=0;
    while(current_page->GetFreeSpaceOffset()+current_page->GetColumnLength()
        >DB_COLUMN_HEAP_PAGE_SIZE){
        auto next_page_id  = current_page->GetNextPageId();
        //event here Unpin a new Create Page that Alright,
        // We Unpin (true) before
        heap_handle_.Unpin(current_page->page_id,false);
        if(next_page_id == NULL_PAGE_ID){
            auto* current_page = 
            reinterpret_cast<ColumnHeapNumPage*>
            (CreateNewColumnHeapPage<ColumnType::INT>());
            
        }else{
            current_page = reinterpret_cast<ColumnHeapNumPage*>(
            heap_handle_.GetPage(next_page_id));
            total_obj+=current_page->GetTotalObj();
        }
    }
    // now , current page  is page that we wanto insert .
    current_page->Append(val,total_obj);
    heap_handle_.Unpin(current_page->page_id,true);
}

std::vector<Value> TableCataLog::GetNumDataChunk(uint32_t col_idx){
    auto& col_heap = this->table_heap_->columns_heap_[col_idx];
    auto* data_page = reinterpret_cast<ColumnHeapNumPage*>(heap_handle_.GetPage(col_heap->heap_page_id_));

    return __GetNumDataChunk(data_page);
}
std::vector<String> TableCataLog::GetStringDataChunk(uint32_t col_idx){
    auto& col_heap  = table_heap_->columns_heap_[col_idx];
    auto* data_page = reinterpret_cast<ColumnHeapStringPage*>(heap_handle_.GetPage(col_heap->heap_page_id_));

    return __GetStringDataChunk(data_page);
}

std::vector<Value> TableCataLog::
__GetNumDataChunk(ColumnHeapNumPage* page){
    DASSERT(page->GetColumnType()== ColumnType::INT);
    std::vector<Value> r;
    auto* current = page;
    current->ReadLock();
    while(current){
        auto _begin = current->GetBeginRowId();
        auto _end = current->GetEndRowId();

        for(uint32_t i=_begin;i<=_end;++i){
            auto [falg,val] = current->GetObj(i);
            r.emplace_back(val);
        }
        auto n_id = current->GetNextPageId();
        if(n_id!=NULL_PAGE_ID){
            current->ReadUnlock();
            heap_handle_.Unpin(current->page_id,false);
            current= reinterpret_cast<ColumnHeapNumPage*>(  heap_handle_.GetPage(n_id));
            current->ReadLock();
        }else {
            current = nullptr;
        }
            
    }
    current->ReadUnlock();
    heap_handle_.Unpin(current->page_id,false);
    return r;

}
std::vector<String> TableCataLog::
__GetStringDataChunk(ColumnHeapStringPage* page){
    DASSERT(page->GetColumnType()== ColumnType::STRING);
    std::vector<String> result;
    auto* current = page;
    current->ReadLock();
    while(current){
        auto _begin = current->GetBeginRowId();
        auto _end = current->GetEndRowId();
        for(auto i=_begin;i<_end;++i){
            auto [flag,val] = current->GetObj(i);
            result.push_back(val);
        }
        auto n_id = current->GetNextPageId();
        if(n_id!=NULL_PAGE_ID){
            current->ReadUnlock();
            heap_handle_.Unpin(current->page_id,false);
            current= reinterpret_cast<ColumnHeapStringPage*>(  heap_handle_.GetPage(n_id));
            current->ReadLock();
        }else {
            current = nullptr;
        }
    }
    current->ReadUnlock();
    heap_handle_.Unpin(current->page_id,false);
    return result;
}


std::vector<ValueUnion>
TableCataLog::GetDataChunk(uint32_t col_idx){
    std::vector<ValueUnion> result;
    auto& column_heap = this->table_heap_->columns_heap_[col_idx];
    assert(column_heap);
    auto page = reinterpret_cast<ColumnHeapPage*>(heap_handle_.GetPage(column_heap->heap_page_id_));
    if(page->GetColumnType()==ColumnType::INT){
        auto r = GetNumDataChunk(col_idx);
        std::vector<ValueUnion> result;
        for(auto val : r){
            result.emplace_back(val);
        }
        return result;
    }else {
        auto r = GetStringDataChunk(col_idx);
        std::vector<ValueUnion> result;
        for(auto& val:r){
            result.emplace_back(val);
        }
        return result;
    }
    
}

std::vector<Value>
TableCataLog::GetDataChunk(const std::string& col_name,std::vector<bool>& bitmap){
    std::vector<Value> result;
    /*
        fill ONE_DATACHUNKSIZE with bitmap.
    */
    auto v= bitmap.size();

    ColumnHeap* table_heap = nullptr;
    for(auto& cn: table_heap_->columns_heap_){
        if(cn->def_.column_name==col_name)
            table_heap= cn.get();
    }
    assert(table_heap);
    auto* current_page =reinterpret_cast<ColumnHeapNumPage*> (heap_handle_.GetPage(table_heap->heap_page_id_));
    auto no_obj = current_page->GetTotalObj();
    for(uint32_t i=0;i<bitmap.size();++i){
        auto b =bitmap[i];
        if(!b) continue;
        if(i>=no_obj){
            auto next_page = current_page->GetNextPageId();
            if(next_page==NULL_PAGE_ID)
                throw Exception("Logical error when GetDataChunk");
            heap_handle_.Unpin(current_page->page_id,false);
            current_page = reinterpret_cast<ColumnHeapNumPage*>(heap_handle_.GetPage(next_page));
            no_obj+= current_page->GetTotalObj();
        }
        auto [flag,value] = current_page->GetObj(i);
        //TODO::Check if this obj has been deleted.
        result.push_back(value);
    }
    heap_handle_.Unpin(current_page->page_id,false);
    return result;
}   


/*
    first ColumnPage should used to meta information page.but now traverse all page to get total obj.
*/
