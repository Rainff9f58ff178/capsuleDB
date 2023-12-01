#pragma  once
#include "Planer/LogicalOperator.h"

class LimitLogicalOperator : public LogicalOperator{
    static constexpr const OperatorType type_=
        OperatorType::LimitOperatorNode;
public:
    LimitLogicalOperator(std::vector<LogicalOperatorRef> child,
    SchemaRef schema,SchemaRef table_schame,uint32_t number):
    LogicalOperator(std::move(child),std::move(schema),std::move(table_schame)),number_(number){}



    virtual OperatorType GetType() override{
        return type_;
    }
    uint32_t number_;
};