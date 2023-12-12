#pragma  once
#include "Planer/LogicalOperator.h"

class LimitLogicalOperator : public LogicalOperator{
    static constexpr const OperatorType type_=
        OperatorType::LimitOperatorNode;
public:
    LimitLogicalOperator(std::vector<LogicalOperatorRef> child,
    uint32_t limit ,uint32_t offset):
    LogicalOperator(std::move(child)),limit_(limit),offset_(offset){

    }

    COPY_PLAN_WITH_CHILDREN(LimitLogicalOperator)
    virtual OperatorType GetType() override{
        return type_;
    }
    uint32_t limit_;
    uint32_t offset_;
};