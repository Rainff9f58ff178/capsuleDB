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
    ValueUnion EvaluteJoin(ChunkRef* l_chunk,ChunkRef* r_chunk,uint32_t l_idx,uint32_t r_idx) override{
        return val_;
    }
    LogicalExpressionType GetType() override{
        return LogicalExpressionType::ConstantExpr;
    }
    LogicalExpressionRef Copy() override{
        auto r = std::make_shared<ConstantValueExpression>(std::move(val_.clone()));
        r->alias_ = alias_;
        return r;
    }
    virtual ColumnType GetReturnType() override{
        return ValueTypeConvertToColumnType(val_.type_);
    }
    std::string toString() override{
        if(alias_){
            return *alias_;
        }

        if(val_.type_==ValueType::TypeString){
            return std::string(val_.val_.data_,val_.value_len_);
        }else {
            return std::to_string(val_.val_.num_);
        }
        return  "";
    }
private:
    ValueUnion val_;
};