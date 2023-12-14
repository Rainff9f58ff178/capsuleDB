#pragma once
#include "Expressions/LogicalExpression.h"

enum ComparisonType {GreaterThan,LesserThan,GreaterEqualThan,LesserEquanThan,Equal};




class ComparisonExpression : public LogicalExpression {
    static constexpr const LogicalExpressionType type_= LogicalExpressionType::ComparsionExpr;
public:

    
    ComparisonExpression(std::vector<LogicalExpressionRef> child,ComparisonType type)
        :LogicalExpression(std::move(child)),cp_type_(type){

        }
    
    LogicalExpressionType GetType() override{
        return  LogicalExpressionType::ComparsionExpr;
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
    virtual ValueUnion EvaluteJoin(ChunkRef* l_chunk,ChunkRef* r_chunk,uint32_t l_idx,uint32_t r_idx) override{
        auto l_val = children_[0]->EvaluteJoin(l_chunk,r_chunk,l_idx,r_idx);
        auto r_val = children_[1]->EvaluteJoin(l_chunk,r_chunk,l_idx,r_idx);
        // actually ,here must be "=".
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
        UNREACHABLE;
    }
    ColumnType GetReturnType() override{
        return ColumnType::INT;
    }
    LogicalExpressionRef Copy() override{
        auto l = children_[0]->Copy();
        auto r = children_[1]->Copy();
        auto _r = std::make_shared<ComparisonExpression>(std::vector<LogicalExpressionRef>{l,r},cp_type_);
        _r->alias_= alias_;
        return _r;
    }

    std::string toString() override{
        if(alias_){
            return *alias_;
        }
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