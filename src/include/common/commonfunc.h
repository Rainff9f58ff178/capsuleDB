#pragma once
#include "CataLog/Schema.h"
#include <atomic>
#include <string>
#include <vector>
#include <memory>
#include<ranges>
static std::string 
join(const std::vector<std::string>& arr,std::string s){
    if(arr.size()==1)
        return arr[0];
    std::string result;
    for(uint32_t i=0;i<arr.size();++i){
        result.append(arr[i]);
        if(i!=arr.size()-1)
            result.append(s);
    }
    return result;
}

static std::vector<std::string>
split(const std::string& string, const std::string& key_word){
    std::vector<std::string> _result;
    for(auto const s:  string | std::views::split(key_word)){
        _result.emplace_back(s.begin(),s.end());
    }
    return _result;
}


static SchemaRef
InferSchemaFromCols(const std::vector<std::string> cols){
    std::vector<Column> columns;
    for(uint32_t i=0;i<cols.size();++i){
        columns.push_back(Column(cols[i],i*4));
    }
    return std::shared_ptr<Schema>(new Schema(columns));
}
static std::string StringLower(const std::string& string){
    std::string a = string;
    std::transform(a.begin(),a.end(),a.begin(),[](char& c){ return std::tolower(c) ;});
    return  a;
}
static std::string ColumnTypeToString(ColumnType type){
    switch(type){
        case  ColumnType::INT:{
            return "int";
        }
        case   ColumnType::STRING:{
            return "String";
        }
        default:
            return "Unkowned";
    }
}
static std::string getTableNameFromColName(const std::string& col_name){
    auto cols = split(col_name,".");
    CHEKC_THORW(cols.size() >= 2);
    return cols[0];
}
static ColumnType ValueTypeConvertToColumnType(ValueType type){
    switch(type){
        case ValueType::TypeInt:{
            return ColumnType::INT;
        }
        case ValueType::TypeString:{
            return ColumnType::STRING;
        }
        default:{
            return  ColumnType::UNKOWN;
        }
    }
    UNREACHABLE;
}
static std::string ChangeTableName(
    const std::string& name,std::string& table_name){
    
    auto cols = split(name,".") ;
    CHEKC_THORW(cols.size() >=2);
    cols[0] = table_name;
    return join(cols,".");
}
static ValueType ColumnTypeConverToValueType(ColumnType type){
    switch(type){
        case ColumnType::INT:{
            return ValueType::TypeInt;
        }
        case ColumnType::STRING:{
            return ValueType::TypeString;
        }
        default:
            NOT_IMP
    }
    UNREACHABLE
}


