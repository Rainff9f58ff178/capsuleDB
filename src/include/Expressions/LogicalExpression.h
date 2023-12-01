#pragma once
#include <memory>
#include <vector>
#include "CataLog/TableCataLog.h"
#include "common/Exception.h"
#include "common/type.h"
#include "common/Value.h"
#include "Execute/BitMap.h"
class LogicalExpression;
using LogicalExpressionRef = std::shared_ptr<LogicalExpression>;
enum  LogicalExpressionType{
    LogicalExpr,
    AndOrExpr,
    ArithmeticExpr,
    ColumnValueExpr,
    ComparsionExpr,
    ConstantExpr,
};
class LogicalExpression{
    static constexpr const LogicalExpressionType type_=
        LogicalExpr;
public:

    explicit LogicalExpression(std::vector<LogicalExpressionRef> children)
        :children_(std::move(children)){}
    virtual ~LogicalExpression()=default;
    inline std::vector<LogicalExpressionRef>& GetChildren(){return children_;}

    inline LogicalExpressionRef GetChild(uint32_t idx){return  children_[idx];}


    virtual ValueUnion Evalute(DataChunk* chunk,uint32_t data_idx_in_this_chunk){
        throw Exception("Unreachable call LogicalExpression 's Evalute");
    }

    // thsi function used in Evalute two value if equal.
    virtual bool EvaluteJoin(DataChunk* left_chunk,DataChunk* right_chunk,
    uint32_t data_idx_in_this_chun){
        throw Exception("Unreachable call LogicalExpression 's Evalutejoin");
    }
    
    virtual ValueUnion Evalute(std::vector<bool>& bitmap,TableCataLog* table){
        throw Exception("Unraechable call LogicalExpression's Evalute");
    }
    
    virtual void PrintDebug(){
        for(auto& r :children_){
            r->PrintDebug();
        }
    }

    //max size of children is 2;
    std::vector<LogicalExpressionRef> children_;
    
};