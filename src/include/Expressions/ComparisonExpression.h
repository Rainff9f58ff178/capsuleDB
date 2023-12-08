#pragma once
#include "Expressions/LogicalExpression.h"

enum ComparisonType {GreaterThan,LesserThan,GreaterEqualThan,LesserEquanThan,Equal};




class ComparisonExpression : public LogicalExpression {
    static constexpr const LogicalExpressionType type_= LogicalExpressionType::ComparsionExpr;
public:

    
    ComparisonExpression(std::vector<LogicalExpressionRef> child,ComparisonType type)
        :LogicalExpression(std::move(child)),cp_type_(type){

        }
    
    virtual ValueUnion Evalute(ChunkRef* chunk, uint32_t idx) override{
        auto l_val = children_[0]->Evalute(chunk,idx);
        auto r_val = children_[1]->Evalute(chunk,idx);
        if(cp_type_==ComparisonType::Equal){
            return ValueUnion(static_cast<int>( l_val==r_val) );
        }else if(cp_type_ == ComparisonType::GreaterEqualThan){
            return ValueUnion(static_cast<int> ( l_val>=r_val));
        }else if(cp_type_==ComparisonType::GreaterThan){
            return ValueUnion(static_cast<int>( l_val > r_val));
        }else if(cp_type_==ComparisonType::LesserEquanThan){
            return ValueUnion(static_cast<int>( l_val<=r_val));
        }else if(cp_type_== ComparisonType::LesserThan){
            return ValueUnion(static_cast<int>( l_val < r_val));
        }
        UNREACHABLE
    }

    std::string toString() override{
        auto left = children_[0]->toString();
        auto op = "";
        
        if(cp_type_==ComparisonType::Equal){
            op= " = ";
        }else if(cp_type_ == ComparisonType::GreaterEqualThan){
            op= " >= ";
        }else if(cp_type_==ComparisonType::GreaterThan){
            op= " > ";
        }else if(cp_type_==ComparisonType::LesserEquanThan){
            op= " <= ";
        }else if(cp_type_== ComparisonType::LesserThan){
            op= " < ";
        }
        auto right = children_[1]->toString();
        return left.append(op).append(right);
    }

 
private:
    ComparisonType cp_type_;

};