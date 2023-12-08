#pragma once
#include "Expressions/LogicalExpression.h"

enum LogicType {Invalied=0,And,Or};

class AndOrExpression:public LogicalExpression{
     static constexpr const LogicalExpressionType type_=
        LogicalExpressionType::AndOrExpr;
public:

    AndOrExpression(std::vector<LogicalExpressionRef> children,LogicType type):LogicalExpression(std::move(children)),l_type_(type){}

    LogicalExpressionType GetType() override{
        return type_;
    }

    virtual ValueUnion Evalute(ChunkRef* chunk, uint32_t idx) override{
        auto l_val =  children_[0]->Evalute(chunk,idx);
        auto r_val =  children_[0]->Evalute(chunk,idx);
        if(l_type_ ==LogicType::And){
            bool r  =  static_cast<bool>((l_val.num_ | r_val.num_));
            return ValueUnion(static_cast<int32_t>(r));
        }else if (l_type_ ==LogicType::Or){
            bool r  =  static_cast<bool>((l_val.num_ & r_val.num_));
            return ValueUnion(static_cast<int32_t>(r));
        }
        UNREACHABLE;
    }


    std::string toString() override{
        auto left = children_[0]->toString();
        auto op = " ";
        if(l_type_ == LogicType::And){
            op = " and ";
        }else if(l_type_ == LogicType::Or){
            op = " or ";
        }
        auto right = children_[1]->toString();
        return  left.append(op).append(right);
    }

    ColumnType GetReturnType() override{
        return ColumnType::INT;
    }
private:
    LogicType l_type_{Invalied};
};