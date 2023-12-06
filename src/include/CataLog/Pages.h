#pragma once
#include "BufferManager.h"
#include "nodes/primnodes.hpp"
#include "pg_definitions.hpp"
#include "common/type.h"
#include "common/Exception.h"
#include <type_traits>

#ifndef NDEBUG
#define D_assert(x) assert(x)
#define DASSERT(x) assert(x)
#else 
#define D_assert(x)
#define DASSERT(x) 
#endif


#define SafeUnpin(handle,page,dirty)              \
    DASSERT((handle).GetPageSize() == page->GetPageSize()); \
    (handle).Unpin(page->page_id,dirty);                     \


constexpr const uint32_t NULL_PAGE_ID = UINT32_MAX;
constexpr const uint32_t NULL_LSN_NUM = UINT32_MAX;

static constexpr uint32_t DB_INFO_FILE_PAGE_SIZE = 4096;
constexpr uint32_t DB_COLUMN_HEAP_PAGE_SIZE = 256*1024;





#define SETTER_UINT32(name,OFFSET)                  \
        inline void name(uint32_t value){               \
            *(uint32_t*)(GetData()+OFFSET) = value;     \
        }

    #define GETTER_UINT32(name,OFFSET)                  \
        inline uint32_t name(){                         \
            return *(uint32_t*)(GetData()+OFFSET);      \
        }

    #define SETTER_INT32(name,OFFSET)                  \
        inline void name(int32_t value){               \
            *(int32_t*)(GetData()+OFFSET) = value;     \
        }

     #define GETTER_INT32(name,OFFSET)                  \
        inline int32_t name(){                         \
            return *(int32_t*)(GetData()+OFFSET);      \
        }


#define GETTER_AND_SETTER_UINT32(name,OFFSET)          \
    SETTER_UINT32(Set##name,OFFSET)                      \
    GETTER_UINT32(Get##name,OFFSET)                      \

#define GETTER_AND_SETTER_INT32(name,OFFSET)        \
    SETTER_INT32( Set##name,OFFSET)                       \
    GETTER_INT32( Get##name,OFFSET)               


/*
    GernalPage store some GernalData
    LAYOUT :
----------------------------------------------------------
LSN(4Bbyte)|PAGE_ID(4Byte)|
----------------------------------------------------------
*/
template<uint32_t PAGE_SIZE>
class GernalPage:public Page<PAGE_SIZE>{
private:
    static constexpr const uint32_t LSN_OFFSET = 0;
    static constexpr const uint32_t PAGE_ID_OFFSET = 4;
    static constexpr const uint32_t GERNAL_HEADER = 8;
public:
    inline uint32_t  GetLSN(){
        return *(uint32_t*)(this->GetData()+LSN_OFFSET);
    }
    inline  void SetLSN(uint32_t value){
        *(uint32_t*)(this->GetData()+LSN_OFFSET)=value;
    }
    inline  uint32_t GetPageIdFromData(){
        return *(uint32_t*)(this->GetData()+PAGE_ID_OFFSET);
    }
    inline void SetPageIdToData(uint32_t value){
        *(uint32_t*)(this->GetData()+PAGE_ID_OFFSET)=value;
    }

};

// ----------------------------------------------------------------
// GERNAL_HEADER(8byte)|PREV_PAGE_ID(4byte)|NEXT_PAGE_ID(4bte)    |
// ----------------------------------------------------------------
template<uint32_t PAGE_SIZE>
class LinkPage:public GernalPage<PAGE_SIZE>{
public:
    static constexpr uint32_t PREV_PAGE_ID = 8;
    static constexpr uint32_t NEXT_PAGE_ID = 12;
    static constexpr uint32_t LINK_PAGE_HEADER = 16;
    inline void SetPrevPageId(uint32_t value){
        *(uint32_t*)(this->GetData()+PREV_PAGE_ID) = value;
    }
    inline uint32_t GetPrevPageId(){
        return *(uint32_t*)(this->GetData()+PREV_PAGE_ID);
    }
    inline void SetNextPageId(uint32_t value){
        *(uint32_t*)(this->GetData()+NEXT_PAGE_ID) = value;
    }
    inline uint32_t GetNextPageId(){
        return *(uint32_t*)(this->GetData()+NEXT_PAGE_ID);
    }
};

/*this page is the fisrt page of .db file */
/*

----------------------------------------------------------------
GERNAL_HEADER(8byte)|PREV_PAGE_ID(4byte)|NEXT_PAGE_ID(4bte)    |
----------------------------------------------------------------
OFFSET(4byte|TABLE_NUMS(4byte)|TABLE_PAGE_ID|....|TABLE_PAGE_id|
----------------------------------------------------------------

*/
class CataLogMetaPage:public LinkPage<DB_INFO_FILE_PAGE_SIZE>{
public:
    static constexpr const uint32_t OFFSET = 16;
    static constexpr const uint32_t TABLE_NUMS=20;
    inline void SetOffset(uint32_t value){
        *(uint32_t*)(this->GetData()+OFFSET)=value;
    }
    inline void SetTableNum(uint32_t value){
        *(uint32_t*)(this->GetData()+TABLE_NUMS)=value;
    }
    inline uint32_t GetOffset(){
        return *(uint32_t*)(this->GetData()+OFFSET);
    }
    inline uint32_t GetTableNum(){
        return *(uint32_t*)(this->GetData()+TABLE_NUMS);
    }
    inline void AddTable(page_id_t table_page_id){
        SetTableNum(GetTableNum()+1);
        auto of = GetOffset();
        *(uint32_t*)(GetData()+of) = table_page_id;
        SetOffset(of+4);
    }
};
/*
Table_oid is his page_id
----------------------------------------------------------------
GERNAL_HEADER(8byte)|PREV_PAGE_ID(4byte)|NEXT_PAGE_ID(4bte)    |
----------------------------------------------------------------
TABLE_NAME(128byte)|TABLE_OID|               |COLUMN_NUMS(4BYTE)|
----------------------------------------------------------------
COLUMN_NAME(64byte)|COLUMN_TYPE(4byte)|ColumnLength(4byte)|COLUMN_HEAP(4byte)
----------------------------------------------------------------
STATIC_PAGE(4byte)|...
----------------------------------------------------------------

*/
class TableCataLogPage:public LinkPage<DB_INFO_FILE_PAGE_SIZE>{
public:
    static constexpr const uint32_t TABLE_NAME = 16;
    static constexpr const uint32_t TABLE_OID  = 128+TABLE_NAME;
    static constexpr const uint32_t COLUMN_NUMS = TABLE_OID+4;

    static constexpr const uint32_t FISRT_COLUMN = COLUMN_NUMS+4;

    SETTER_UINT32(GetColumnNum,COLUMN_NUMS);
    GETTER_UINT32(GetColumnNum,COLUMN_NUMS);
    static constexpr const uint32_t COLUMN_DEF_LENGTH = 80;
    inline void SetColumnHeapPage(uint32_t col_idx,uint32_t column_page_id){
        assert(GetColumnNum()>col_idx);
        auto off = FISRT_COLUMN+col_idx*COLUMN_DEF_LENGTH;
        auto column_heap_off=off+72;
        *(uint32_t*)(GetData()+column_heap_off)=column_page_id;
    }
    inline void SetColumnStaticPage(uint32_t col_idx,uint32_t column_static_page_id){
        DASSERT(GetColumnNum() > col_idx);
        auto offset = FISRT_COLUMN + col_idx* COLUMN_DEF_LENGTH;
        auto column_static_off = offset+ 76;
        *(uint32_t*)(GetData()+column_static_off) = column_static_page_id;
    }
    inline uint32_t GetColumnHeapPage(uint32_t col_idx){
        assert(GetColumnNum()>col_idx);
        auto off = FISRT_COLUMN+col_idx*COLUMN_DEF_LENGTH;
        auto column_heap_off=off+72;
        return *(uint32_t*)(GetData()+column_heap_off);
    }
    inline uint32_t GetColumnStaticPage(uint32_t col_idx){
        assert(GetColumnNum()>col_idx);
        auto off = FISRT_COLUMN+col_idx*COLUMN_DEF_LENGTH;
        auto column_static_off=off+76;
        return *(uint32_t*)(GetData()+column_static_off);
    }
    
};


/*
----------------------------------------------------------------
LINKED_PAGE_HEADER(16byte)
----------------------------------------------------------------
COLUMN_TYPE(INT OR STRING(4Byte))|COLUMN_LENGTH(4byte)|
----------------------------------------------------------------
TOTAL_OBJ(4byte)|
----------------------------------------------------------------
TOTAL_MAX_VALUE(4byte)|TOTAL_SUM_VALUE(4byte)|TOTAL_MIN_VALUE(4byte)
----------------------------------------------------------------
----------------------------------------------------------------
----------------------------------------------------------------
*/

class ColumnHeapStaticPage:public LinkPage<DB_COLUMN_HEAP_PAGE_SIZE>{
    
    static constexpr const uint32_t COLUMN_TYPE=16;
    static constexpr const uint32_t COLUMN_LENGTH=20;
    static constexpr const uint32_t TOTAL_OBJ = COLUMN_LENGTH+4;
    static constexpr const uint32_t TOTAL_MAX_VALUE = TOTAL_OBJ+4;
    static constexpr const uint32_t TOTAL_SUM_VALUE = TOTAL_MAX_VALUE +4;
    static constexpr const uint32_t TOTAL_MIN_VALUE = TOTAL_SUM_VALUE +4;
public:
    GETTER_AND_SETTER_UINT32(ColumnType,COLUMN_TYPE);
    GETTER_AND_SETTER_UINT32(ColumnLength,COLUMN_LENGTH);

    GETTER_AND_SETTER_UINT32(TotalObj,TOTAL_OBJ);
    GETTER_AND_SETTER_INT32(TotalMaxValue,TOTAL_MAX_VALUE);

    GETTER_AND_SETTER_INT32(TotalSumValue,TOTAL_SUM_VALUE);

    GETTER_AND_SETTER_INT32(TotalMinValue,TOTAL_MIN_VALUE);

    void UpdateMaxValue(Value val){
        DASSERT(GetColumnType() == ColumnType::INT);
        if(val > GetTotalMaxValue())
            SetTotalMaxValue(val);

    }
    void UpdateSumValue(Value val){
        DASSERT(GetColumnType() == ColumnType::INT);
        if(val < GetTotalMinValue())
            SetTotalMinValue(val);
    }
    void updateSumVlaue(Value val){
        DASSERT(GetColumnType() == ColumnType::INT);
        SetTotalSumValue(val+GetTotalSumValue());
    }
    
    void UpdateMaxValue(String& val){
        DASSERT(GetColumnType() == ColumnType::STRING);
        if(val.size() > GetTotalMaxValue())
            SetTotalMaxValue(val.size());

    }
    void UpdateSumValue(String& val){
        DASSERT(GetColumnType() == ColumnType::STRING);
        if(val.size() < GetTotalMinValue())
            SetTotalMinValue(val.size());
    }
    void updateSumVlaue(String& val){
        DASSERT(GetColumnType() == ColumnType::STRING);
        SetTotalSumValue(val.size()+GetTotalSumValue());
    }
    
};

class ColumnHeapPage:public LinkPage<DB_COLUMN_HEAP_PAGE_SIZE>{
public:
    

    static constexpr const uint32_t COLUMN_TYPE=16;
    static constexpr const uint32_t COLUMN_LENGTH=20;
    static constexpr const uint32_t BEGIN_ROW_ID=24;
    static constexpr const uint32_t END_ROW_ID=28;
    static constexpr const uint32_t TOTAL_OBJ=32;
    static constexpr const uint32_t LOCAL_MAX_VALUE=36;
    static constexpr const uint32_t LOCAL_SUM_VALUE=40;
    static constexpr const uint32_t LOCAL_MIN_VALUE=44;

    inline void SetColumnType(ColumnType type){
        *(int32_t*)(GetData()+COLUMN_TYPE) = (int32_t)type;
    }
    inline ColumnType GetColumnType(){
        return (ColumnType)(*(int32_t*)(GetData()+COLUMN_TYPE));
    }
  
    SETTER_UINT32(SetColumnLength,COLUMN_LENGTH);
    GETTER_UINT32(GetColumnLength,COLUMN_LENGTH);
    SETTER_UINT32(SetBeginRowId,BEGIN_ROW_ID);
    GETTER_UINT32(GetBeginRowId,BEGIN_ROW_ID);
 

    SETTER_UINT32(SetEndRowId,END_ROW_ID);
    GETTER_UINT32(GetEndRowId,END_ROW_ID);
    SETTER_UINT32(SetTotalObj,TOTAL_OBJ);
    GETTER_UINT32(GetTotalObj,TOTAL_OBJ);
    SETTER_INT32(SetLocalMaxValue,LOCAL_MAX_VALUE);
    GETTER_INT32(GetLocalMaxValue,LOCAL_MAX_VALUE);
    SETTER_INT32(SetLocalSumValue,LOCAL_SUM_VALUE);
    GETTER_INT32(GetLocalSumValue,LOCAL_SUM_VALUE);
    SETTER_INT32(SetLocalMinValue,LOCAL_MIN_VALUE);
    GETTER_INT32(GetLocalMinValue,LOCAL_MIN_VALUE);


};
/*
/ STRING STROE LAYOUT,if store value is String ,slot is useful,
but num ,slot is wasteful.
----------------------------------------------------------------
LINKED_PAGE_HEADER(16byte)
----------------------------------------------------------------
COLUMN_TYPE(INT OR STRING(4Byte))|COLUMN_LENGTH(4byte)|
----------------------------------------------------------------
BEGIN_ROW_ID(4byte)|END_ROW_ID(4byte)|TOTAL_OBJ(4byte)|
----------------------------------------------------------------
LOCAL_MAX_VALUE(4byte)|LOCAL_SUM_VALUE(4byte)|LOCAL_MIN_VALUE(4byte)
----------------------------------------------------------------
----------------------------------------------------------------
........................
............................................META_INFORMATION(512byte)
SLOT_POINTER_OFFSET(4yte)|FREE_SPACE_POINTER_OFFSET(4byte)|
----------------------------------------------------------------
SOLT_NUM(4byte)|
----------------------------------------------------------------
SLOT(8byte(4byte offset | 4 byte others )) .................
----------------------------------------------------------------
....SLOT|                                |{length},read data    |
----------------------------------------------------------------
*/


class ColumnHeapStringPage:public  ColumnHeapPage{
public:
    class ColumnHeapStringPageIterator{
    public:
        ColumnHeapStringPageIterator(){
            page_ = 0;
            offset_=0;
        }
        ColumnHeapStringPageIterator(ColumnHeapStringPage* page):page_(page){
            if(page){
                offset_ = FIRST_SLOT_OFFSET;
                operator++();
            }
        }
        void operator++(){
            if(page_==nullptr || offset_==0){
                throw Exception("You ++ a end ColumnheapStringIterator ");
            }
            page_->ReadLock();
            if(offset_ == page_->GetSlotPointerOffset()){
                //to end 
                page_=nullptr;
                offset_=0;
                return;
            }
            auto* slot = page_->data+offset_;
            auto offset = *(uint32_t*)slot;
            auto other = *(uint32_t*)(slot+4);
            auto string_size = *(uint32_t*)(page_->data+ offset);
            current_value_ = std::string_view(page_->data+offset+4,string_size);
            offset_+=8;
            page_->ReadUnlock();
        }
        bool operator!=(const ColumnHeapStringPageIterator& other){
            return ! (*this==other);
        }
        const std::string operator*(){
            return current_value_;
        }
        bool operator==(const ColumnHeapStringPageIterator& other){
            if(page_ == other.page_ && offset_ == other.offset_)
                return true;
            return false;
        }
        std::string current_value_;
        ColumnHeapStringPage* page_;
        uint32_t offset_=0;
    };
    static constexpr const uint32_t META_INFORMATION = 512;
    static constexpr const uint32_t SLOT_POINTER_OFFSET = 516;
    static constexpr const uint32_t FREE_SPACE_POINTER_OFFSET = 520;
    static constexpr const uint32_t SLOT_NUM = 524;
    static constexpr const uint32_t FIRST_SLOT_OFFSET = SLOT_NUM+4;

    static constexpr const uint32_t SLOT_SIZE = 8;

    // SLOT_POINTER_OFFSET point to next free slot location,
    // FREE_SPACE_POINTER_OFFSET point to next free space.

    GETTER_AND_SETTER_UINT32(SlotPointerOffset,SLOT_POINTER_OFFSET);
    GETTER_AND_SETTER_UINT32(FreeSpacePointerOffset,FREE_SPACE_POINTER_OFFSET);
    GETTER_AND_SETTER_UINT32(SlotNum,SLOT_NUM);
    ColumnHeapStringPageIterator begin(){
        return ColumnHeapStringPageIterator(this);
    }
    ColumnHeapStringPageIterator end(){
        return ColumnHeapStringPageIterator(nullptr);
    }
    void SetSlot(uint32_t slow_idx,uint32_t offset,uint32_t mask){
        D_assert(slow_idx <GetSlotNum());
        auto* dst=data+FIRST_SLOT_OFFSET+(slow_idx*SLOT_SIZE);
        *(uint32_t*)dst= offset;
        *(uint32_t*)(dst+4) = mask;
    }
    std::pair<uint32_t,uint32_t> GetSlot(uint32_t slow_idx){
        DASSERT(slow_idx<GetSlotNum());
        auto* dst = data+FIRST_SLOT_OFFSET+(slow_idx*SLOT_SIZE);
        return {*(uint32_t*)dst,*(uint32_t*)(dst+4)};
    }

    std::tuple<uint32_t,std::string>
    GetObj(uint32_t row_idx){
        DASSERT(row_idx <=GetEndRowId());
        auto slot_idx = row_idx - GetBeginRowId();
        auto [offset,flag] = GetSlot(slot_idx);
        
        auto length = *(uint32_t*)(GetData()+offset);
        return {flag,std::string((GetData()+offset+4),length)};
    }
    
    bool HasSpace(uint32_t bytes){
        if(GetFreeSpacePointerOffset()-GetSlotPointerOffset() >= 4+bytes+SLOT_SIZE)
            return true;
        return false;
    }
    void Append(String& value,uint32_t row_idx){
        auto free_slot_off = GetSlotPointerOffset();
        auto free_space_off = GetFreeSpacePointerOffset();
        D_assert(free_space_off - free_slot_off >=value.size() + 4+SLOT_SIZE);


        auto value_dst = free_space_off - value.size() - 4;
        auto slot_idx = GetSlotNum();

        *(uint32_t*)(data+value_dst) = value.size();
        memcpy(data+value_dst+4,value.c_str(),value.size());
        SetFreeSpacePointerOffset(value_dst);
        SetSlotNum(GetSlotNum()+1);
        SetSlotPointerOffset(GetSlotPointerOffset()+SLOT_SIZE);
        SetSlot(slot_idx,value_dst,0);

        if(GetBeginRowId()== NULL_PAGE_ID){
            SetBeginRowId(row_idx);
        }
        SetEndRowId(row_idx);
        if(GetLocalMaxValue() < value.size()){
            SetLocalMaxValue(value.size());
        }
        SetLocalSumValue(GetLocalSumValue()+value.size());

        if(GetLocalMinValue() > value.size()){
            SetLocalMinValue(value.size());
        }


    }




};



/*
----------------------------------------------------------------
LINKED_PAGE_HEADER(16byte)
----------------------------------------------------------------
COLUMN_TYPE(INT OR STRING(4Byte))|COLUMN_LENGTH(4byte)|
----------------------------------------------------------------
BEGIN_ROW_ID(4byte)|END_ROW_ID(4byte)|TOTAL_OBJ(4byte)|
----------------------------------------------------------------
LOCAL_MAX_VALUE(4byte)|LOCAL_SUM_VALUE(4byte)|LOCAL_MIN_VALUE(4byte)
----------------------------------------------------------------
----------------------------------------------------------------
........................
.......................................META_INFORMATION(516byte)
FREE_SPACE_OFFSET(4byte)|(1byte flag)VALUE1(4).....
----------------------------------------------------------------
...(1byte flag)VALUE_n(4)|
----------------------------------------------------------------

----------------------------------------------------------------

----------------------------------------------------------------
*/

class ColumnHeapNumPage:public ColumnHeapPage{
public:
    class ColumnNumPageIterator{
    public:
        ColumnNumPageIterator(){
            page_=nullptr;
            offset_=0;
        }
        ColumnNumPageIterator(ColumnHeapNumPage* page):page_(page){
            if(page){
                offset_=VALUE_BEGIN;
                operator++();
            }else {
                offset_=0;
            }
        }
        ColumnNumPageIterator(ColumnNumPageIterator& other)=default;
        void operator++(){
            if( page_==nullptr || offset_==0 ){
                throw Exception("You ++ a end ColumnNumPageiterator");
            }
            page_->ReadLock();
            DASSERT(page_->GetFreeSpaceOffset() <= DB_COLUMN_HEAP_PAGE_SIZE);
            if(offset_==page_->GetFreeSpaceOffset()){
                //to end
                page_=nullptr;
                offset_=0;
                return;
            }
            current_value_ = *(uint32_t*)(page_->data+offset_+1);                
            page_->ReadUnlock();
            offset_+=5;
        }
        
        bool operator==(const ColumnNumPageIterator& other){
            if(other.page_ == page_ && offset_ == other.offset_)
                return true;
            return false;
        }
        bool operator!=(const ColumnNumPageIterator& other){
            return ! (*this==other);
        }
        Value operator*(){
            return current_value_;
        }
        Value current_value_;
        ColumnHeapNumPage* page_;
        uint32_t offset_=0;
    };

    static constexpr const uint32_t FREE_SPACE_OFFSET = 516;
    static constexpr const uint32_t VALUE_BEGIN = 520;
    SETTER_UINT32(SetFreeSpaceOffset,FREE_SPACE_OFFSET)
    GETTER_UINT32(GetFreeSpaceOffset,FREE_SPACE_OFFSET)
    inline std::tuple<uint8_t,Value> GetObj(uint32_t row_id){
        if(!(row_id>=GetBeginRowId() && row_id<=GetEndRowId())){
            throw Exception("row_id not in this page");
        }
        auto offset = row_id-GetBeginRowId();
        offset = VALUE_BEGIN+(offset*(GetColumnLength()+1));
        uint8_t flag = *(uint8_t*)(GetData()+offset);
        auto* begin = GetData()+offset+1;
        auto v =*(int32_t*)begin;
        return {flag,v};
    }

    /*
        append this value to the page,return row_id.
    */

    ColumnNumPageIterator begin(){
        return ColumnNumPageIterator(this);
    }
    ColumnNumPageIterator end(){
        return ColumnNumPageIterator(nullptr);
    }
    void Append(Value val,uint32_t row_id){
        
        //only support int for now.
        D_assert(GetColumnLength()==4);
        //prepare 1 bit falg;
        char buf[5];
        memset(buf,0,sizeof(buf));
        memcpy(buf+1,(void*)&val,4);
        auto dest = GetData()+GetFreeSpaceOffset();
        memcpy(dest,buf,5);
        SetFreeSpaceOffset(GetFreeSpaceOffset()+5);
        SetTotalObj(GetTotalObj()+1);
        if(GetBeginRowId()==NULL_PAGE_ID)
            SetBeginRowId(row_id); // first insert.
        SetEndRowId(row_id);

        if(GetLocalMaxValue() < val){
            SetLocalMaxValue(val);
        }

        SetLocalSumValue(GetLocalSumValue()+val);
        
        if(GetLocalMinValue() > val){
            SetLocalMinValue(val);
        }
    }
};



template<uint32_t PAGE_SIZE>
class PageReader{
public:
    char* data_;
    uint32_t offset_{0};
    uint32_t length_{PAGE_SIZE};
    explicit PageReader(char* data):data_(data){}
    template<uint32_t N>
    inline void Read(char* buf){
        D_assert(offset_+N<=length_);
        if(buf){
            memcpy(buf,data_+offset_,N);
        }
        offset_+=N;
    }
    template<class T>
    inline T Read(){
        D_assert(offset_+sizeof(T) <=length_);
        auto* value =  (T*)(data_+offset_);
        offset_+=sizeof(T);
        return *value;
    }

    inline void Read(char* buf,uint32_t n){
        D_assert(offset_+n <=length_);
        if(buf){
            memcpy(buf,data_+offset_,n);
        }
        offset_+=n;
    }
};
template<uint32_t PAGE_SIZE>
class PageWrite{
public:
    char* data_;
    uint32_t offset_{0};
    uint32_t length_{PAGE_SIZE};
    
    explicit PageWrite(char* data):data_(data){}
    
    template<uint32_t N>
    inline void Write(const char* buf){
        D_assert(offset_+N <=length_);
        if(buf){
            memcpy(data_+offset_,buf,N);
        }
        offset_+=N;
    }
    inline void Write(const char* buf,uint32_t n){
        D_assert(offset_+n <=length_);
        if(buf){
            memcpy(data_+offset_,buf,n);
        }
        offset_+=n;
    }
    template<class T>
    inline void Write(const T& t){
        D_assert(offset_+sizeof(T) <=length_);
        memcpy(data_+offset_,(void*)&t,sizeof(T));
        offset_+=sizeof(T);
    }
};