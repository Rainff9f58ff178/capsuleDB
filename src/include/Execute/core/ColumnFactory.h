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
};