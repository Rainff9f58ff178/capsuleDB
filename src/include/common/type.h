#pragma  once


#include "BufferManager.h"
#include <cstdint>
using table_oid_t = uint32_t;
using column_idx_t = uint32_t;
//column type only support int32_t.
using column_type = int32_t;
using   Value = int32_t;

#define down_cast dynamic_cast
extern bool show_info;
using   String = std::string;

static constexpr const char* const UNKNOWNED_NAME  
    = "unknowned_col";

enum ColumnType:uint32_t{
        INT=0,
        STRING,
        UNKOWN
};

enum InsertStatementType:uint8_t{
    INSERT_VALUES=0,
    INSERT_SELECT
};

enum class SourceResult:uint8_t{
    HAVE_MORE=0,
    FINISHED
};
enum class SinkResult:uint8_t{
    NEED_MORE=0,
    FINISHED

};
enum class OperatorResult:uint8_t{
    NEED_MORE,
    HAVE_MORE,
    FINISHED

};



