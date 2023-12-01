#pragma once
#include "CataLog/Schema.h"
#include <atomic>
#include <string>
#include <vector>
#include <memory>

static std::string 
join(const std::vector<std::string> arr,std::string s){
    std::string result;
    for(uint32_t i=0;i<arr.size();++i){
        result.append(arr[i]);
        if(i!=arr.size()-1)
            result.append(s);
    }
    return result;
}
static SchemaRef
InferSchemaFromCols(const std::vector<std::string> cols){
    std::vector<Column> columns;
    for(uint32_t i=0;i<cols.size();++i){
        columns.push_back(Column(cols[i],i*4));
    }
    return std::shared_ptr<Schema>(new Schema(columns));
}