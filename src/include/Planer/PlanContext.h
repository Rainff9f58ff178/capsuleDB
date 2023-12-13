#pragma once
#include<unordered_map>
#include<string>
#include<memory>
#include"Planer/TablePlan.h"
#include"Expressions/LogicalExpression.h"
/*
    this class store some data use to create correct Operator,like 
    columns that should loaded.
    select colA from t where colB =1 order colC ;
    select list is singal  'colA', but actually TableScan need load ColA colB colC ,output colA and colC
    (colB as tablefilter not output).
*/
class PlanContext{
public:
    PlanContext(){}
    std::vector<std::string> table_name_queue_;
    std::unordered_map<std::string,std::unique_ptr<TablePlan>> planings_;


    std::unordered_map<std::string,LogicalExpressionRef> agg_map_;
};