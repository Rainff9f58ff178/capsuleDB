#pragma once
#include "common/type.h"
#include <memory>
#include <atomic>
#include "static/RuntimeProfile.h"
class Pipeline;
class PhysicalOperator;
class ExecuteContext;

class Pipeline{
public:
    using PipelineRef = std::shared_ptr<Pipeline>;

    explicit Pipeline(ExecuteContext* context,uint32_t id);
    ~Pipeline()= default;
    PhysicalOperator* sink_{nullptr};
    PhysicalOperator* source_{nullptr};
    std::vector<PhysicalOperator*> operators_;

    
    // this pipeline must wait all children finished     
    std::vector<PipelineRef> children_;
    
    std::weak_ptr<Pipeline> parent_;

    PipelineRef Complete();

    //Does this pipeline runed ?
    std::atomic<bool> runed_{false};
    

    std::atomic<uint32_t> total_children_{0};
    std::atomic<uint32_t> finished_children_{0};

    
    std::atomic<bool> is_root_pipe_line_{false};

    uint32_t identify_{0};
    ExecuteContext* context_;

    RuntimeProfile* profile_;
    RuntimeProfile::Counter* pipeline_execute_time_ = nullptr;
};
