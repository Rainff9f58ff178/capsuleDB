#pragma once
#include"Column.h"
#include "common/Value.h"
class ColumnFactory{
public:
    template<class T>
    static ColumnRef CreateColumnVector();
    template<class T>
    static ColumnRef CreateColumnVector(const std::string& name);
    static ColumnRef CreateColumnString();
    static ColumnRef CreateColumnString(const std::string& name);

    static ColumnRef 
    CreateColumnFrom(ValueUnion& value,const std::string& name);
    static ColumnRef CreateColumn(ColumnType type,const std::string& name){
        auto ref = CreateColumn(type);
        ref->name_ = name;
        return ref;
    }
    static ColumnRef CreateColumn(ColumnType type){
        switch(type){
            case ColumnType::INT:{
                return CreateColumnVector<int32_t>();
            }
            case ColumnType::STRING:{
                return CreateColumnString();
            }
            default:
                UNREACHABLE;
        }
    }
};