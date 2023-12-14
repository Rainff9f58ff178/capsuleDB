#pragma once
#include <memory>
#include <vector>
#include "CataLog/TableCataLog.h"
#include "common/Exception.h"
#include "common/type.h"
#include "common/Value.h"
#include "Execute/core/Chunk.h"
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

    virtual std::string toString(){
        UNREACHABLE
    }
    virtual LogicalExpressionRef Copy(){
        UNREACHABLE;
    }

    virtual void collect_column(std::vector<Column>& col){
        // collect all column appear in this exprsiion.
        for(auto& child : children_){
            child->collect_column(col);
        }
    }
    virtual ColumnType GetReturnType(){
        UNREACHABLE;
    }
    virtual LogicalExpressionType GetType(){
        UNREACHABLE
    }
    virtual ValueUnion Evalute(ChunkRef* chunk,uint32_t idx){
        throw Exception("Unreachable call LogicalExpression 's Evalute");
    }
    virtual ValueUnion EvaluteJoin(ChunkRef* left_chunk,
        ChunkRef* right_chunk,uint32_t l_idx,uint32_t r_idx){
        UNREACHABLE;
    }

    std::optional<std::string> alias_=std::nullopt;
    
 

    //max size of children is 2;
    std::vector<LogicalExpressionRef> children_;
    
};