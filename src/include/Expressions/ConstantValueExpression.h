#pragma once

#include "Expressions/LogicalExpression.h"
#include "common/commonfunc.h"


class ConstantValueExpression:public LogicalExpression{
    static constexpr const LogicalExpressionType type_=
        LogicalExpressionType::ConstantExpr;
public:
    explicit ConstantValueExpression(ValueUnion&& val)
        :LogicalExpression({}),
        val_(std::move(val)){}

    ~ConstantValueExpression() = default;
  
    virtual ValueUnion Evalute(ChunkRef* chunk,uint32_t idx) override{
        return val_;
    }

    LogicalExpressionType GetType() override{
        return LogicalExpressionType::ConstantExpr;
    }

    virtual ColumnType GetReturnType() override{
        return ValueTypeConvertToColumnType(val_.type_);
    }
    std::string toString() override{
        if(val_.type_==ValueType::TypeString){
            return std::string(val_.data_,val_.value_len_);
        }else {
            return std::to_string(val_.num_);
        }
        return  "";
    }
private:
    ValueUnion val_;
};