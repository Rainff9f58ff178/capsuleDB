#include "Execute/ExecuteContext.h"



ExecuteContext::ExecuteContext(CataLog* cata_log):cata_log_(cata_log){
    
}



void 
ExecuteContext::Build(PhysicalOperatorRef plan,
PipelineRef father){
    auto new_pipe_line = std::make_shared<Pipeline>(this,id_++);
    if(father!=nullptr){
        father->children_.push_back(new_pipe_line);
        father->total_children_++;
        new_pipe_line->parent_=
            std::move(std::weak_ptr<Pipeline>(father));
        
        new_pipe_line->sink_=father->source_;
    }else{
        new_pipe_line->is_root_pipe_line_=true;
    }
    pipelines_.push_back(std::move(new_pipe_line)); 
    plan->BuildPipeline(pipelines_.back(),this);
}
void 
ExecuteContext::ShowPipelines(){
    std::cout<<"==== Pipelines ===="<<std::endl;
    for(auto& pipeline : pipelines_){
        if(pipeline->is_root_pipe_line_){
            std::cout<<" root pipeline"<<pipeline->identify_<<":"
            <<"total child has :"<<pipeline->total_children_;
        }else{
            std::cout<<"pipline :"<<pipeline->identify_;
        }
        std::cout<<"[";
        PrintOperatorName(pipeline->source_);
        std::cout<<"]---->";
        std::cout<<"[";
        for(int32_t i=pipeline->operators_.size()-1;i>=0;--i){
            PrintOperatorName(pipeline->operators_[i]);
            if(i!= 0){
                std::cout<<",";
            }
        }
        std::cout<<"]----->";
        std::cout<<"[";
        PrintOperatorName(pipeline->sink_);
        std::cout<<"]";

        std::cout<<"child has ";
        for(auto& child : pipeline->children_){
            std::cout<<child->identify_;
        }
        std::cout<<std::endl;
    }
}
void 
ExecuteContext::PrintOperatorName(PhysicalOperator* node){
    if(node==nullptr){
        std::cout<<"nullptr";
        return;
    }
    switch(node->GetType()){
        case LogicalOperatorNode:{
            throw Exception("Impossiable run to here");
            break;
        }
        case PhysicalOperatorNode:{
            throw Exception("Impossiable run to here");
            break;
        }
        case MaterilizeOperatorNode:{
            std::cout<<"Materilzie Operator Node";
            break;
        }
        case HashJoinOperatorNode:{
            std::cout<<"hashJoin Operator Node";
            break;
        }
        case FilterOperatorNode:{
            std::cout<<"FilterOperator Operator Node";
            break;
        }
        case InsertOperatorNode:{
            std::cout<<"Insert Operator Node";
            break;
        }
        case ValuesOperatorNode:{
            std::cout<<"ValuesOperator Operator Node";
            break;
        }
        case AggOperatorNode:{
            std::cout<<"AggOperator Operator Node";
            break;
        }
        case LimitOperatorNode:{
            std::cout<<"limit Operator Node";
            break;
        }
        case SortOperatorNode:{
            std::cout<<"Sort Operator Node";
            break;
        }
        case SeqScanOperatorNode:{
            std::cout<<"SeqScan Operator Node";
            break;
        }
        case ResultOperatorNode:{
            std::cout<<"Result Operator Node";
        }
        default:
            break;
    }
}
