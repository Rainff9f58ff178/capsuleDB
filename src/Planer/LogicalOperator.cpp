#include "Planer/LogicalOperator.h"



LogicalOperator::LogicalOperator(std::vector<LogicalOperatorRef> children):
    children_(std::move(children)){
    
}