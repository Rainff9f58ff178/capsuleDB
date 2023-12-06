
#pragma once
#include <cstdint>
#include<string>
#include"common/type.h"

class Column{
public:

    explicit Column(std::string name,uint32_t index);
    Column(std::string name,ColumnType type_);
    Column(std::string name,ColumnType type_,column_idx_t idx);

    Column() = default;
    ~Column()=default;
    
    bool operator==(const Column& other){
        return name_==other.name_;
    }
    std::string name_;
    ColumnType type_;
    uint32_t fix_length_=4;
    uint32_t idx_=0;  // index in table.
};