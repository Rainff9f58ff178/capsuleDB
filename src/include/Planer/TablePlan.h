#pragma once


#include<vector>
#include<string>
#include"common/type.h"
#include"CataLog/Schema.h"
#include<ranges>
class PlanContext;

class TablePlan{
public:
    TablePlan(std::string table_name,SchemaRef table_schema,SchemaRef input_schema):interested_columns_(std::move(input_schema)){
        alias_ =std::move(table_name);
        int idx = 0;
        column_names_ = table_schema;
        for(auto& col : column_names_->columns_){
            columns_map_[col.name_]= idx++;
        }
    }

    column_idx_t TryGetColumnidx(const std::string& col_name){
        auto it = columns_map_.find(col_name);
        if(it == columns_map_.end())
            throw Exception("Column Not In this table");

        return it->second;
    }
    void AddInterestedColumn(const std::string& col_name){
        auto it = columns_map_.find(col_name);
        if(it == columns_map_.end())
            throw Exception("Not found this Column in this table");
        
        for(auto& col : interested_columns_->columns_){
            if(col.name_ == col_name )
                return;
        }

        auto col = column_names_->getColumnByname(col_name);
        if(!col.has_value())
            throw Exception("Not found this column in this table");
        interested_columns_->AddColumn(std::move(col.value()));
    }


    std::string alias_;

    SchemaRef column_names_; // tables whole column schema.
    std::unordered_map<std::string,column_idx_t> columns_map_; // map column name to his idx .
    SchemaRef interested_columns_; // this table schema point to LogicalSeqScan InputSchema.
};