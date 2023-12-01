#pragma once

#include "CataLog/CataLog.h"
#include "Execute/ExecutorNode/PhysicalOperator.h"
#include "Execute/Pipeline/Pipeline.h"
#include "Table/Tuple.h"
#include <memory>


using PipelineRef = std::shared_ptr<Pipeline>;

class ExecuteContext{
public:

    explicit ExecuteContext(CataLog* cata_log_);

    CataLog* cata_log_;

    std::vector<PipelineRef> pipelines_;

    void Build(PhysicalOperatorRef plan,PipelineRef father);
    void ShowPipelines();
    void PrintOperatorName(PhysicalOperator* node);
    // this is root node
    PhysicalOperatorRef physical_plan_;
    //use to identify pipeline
    uint32_t id_{0};
};