#pragma once
#include "Planer/LogicalOperator.h"
#include "Expressions/LogicalExpression.h"
#include "Execute/ExecuteContext.h"
#include<cstdint>

enum class AggregationType: uint8_t {
    CountStarAggregate, 
    CountAggregate,
    SumAggregate,
    MinAggregate,
    MaxAggregate,
    AvgAggregate,
};

class AggregateEntry{
public:
    // select sum(colA ) as a ..... 
    // a is alias ,sum(colA is agg_result_name

    bool operator==(AggregateEntry& other){
        return agg_result_name == other.agg_result_name;
    }
    AggregateEntry(std::string re_name , AggregationType type,std::optional<std::string> alias,
        std::vector<LogicalExpressionRef> exprs):agg_result_name(std::move(re_name)),
        agg_type_(type),alias_(std::move(alias)),args_expr_(std::move(exprs)){

        }
    AggregateEntry(AggregateEntry&& other){
        agg_result_name = std::move(other.agg_result_name);
        agg_type_ = other.agg_type_;
        alias_ = std::move(other.alias_);
        args_expr_ = std::move(other.args_expr_);
    }
    AggregateEntry(const AggregateEntry& other){
        agg_result_name = other.agg_result_name;
        agg_type_ = other.agg_type_;
        alias_ = other.alias_;
        args_expr_ = other.args_expr_;
    }
    std::string agg_result_name;
    AggregationType agg_type_;
    std::optional<std::string> alias_ = std::nullopt;
    std::vector<LogicalExpressionRef> args_expr_;
};


class AggregateLogicalOperator:public LogicalOperator{
    static constexpr const OperatorType type_ = 
        OperatorType::AggOperatorNode;
public:
    AggregateLogicalOperator(std::vector<LogicalExpressionRef> group_bys,std::vector<AggregateEntry> aggs,
    std::vector<LogicalOperatorRef> child):LogicalOperator(std::move(child)),group_bys_(std::move(group_bys)),aggs_(std::move(aggs)){

    }

    

    COPY_PLAN_WITH_CHILDREN(AggregateLogicalOperator)

    void PrintDebug()override{
        LogicalOperator::PrintDebug();
    }
    OperatorType GetType() override{
        return  type_;
    }
    std::vector<LogicalExpressionRef> group_bys_;
    std::vector<Column> group_by_cols_;
    std::vector<AggregateEntry> aggs_;
    

    std::vector<LogicalExpressionRef> pridicators_{};

};