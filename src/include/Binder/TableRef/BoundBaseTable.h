#pragma once

#include "Binder/BoundTabRef.h"
#include "CataLog/Schema.h"
#include <optional>
#include "common/type.h"




class BoundBaseTableRef:public BoundTabRef{
public:

    explicit BoundBaseTableRef(std::string table_name,
        table_oid_t table_id,
        std::optional<std::string> alias,
        Schema schema
        ):BoundTabRef(TableReferenceType::BASE_TABLE),
        table_name_(std::move(table_name)),
        table_id_(table_id),
        alias_name_(std::move(alias)),
        schema_(std::move(schema)){}


    BoundBaseTableRef()=default;
    ~BoundBaseTableRef()=default;

    std::string getTableNameRef() const{
        if(alias_name_)
            return *alias_name_;
        return  table_name_;
    }

    std::string table_name_;
    table_oid_t table_id_;
    
    std::optional<std::string> alias_name_;

    Schema schema_;
};  