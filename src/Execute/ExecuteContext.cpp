#include "Execute/ExecuteContext.h"



ExecuteContext::ExecuteContext(CataLog* cata_log):cata_log_(cata_log){
    profile_ = std::make_shared<RuntimeProfile>("god_profile");
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
std::string
ExecuteContext::ShowPipelines(){
    std::stringstream ss;
    ss<<"==== Pipelines ===="<<std::endl;
    for(auto& pipeline : pipelines_){
        if(pipeline->is_root_pipe_line_){
            ss<<"root pipeline:"<<pipeline->identify_<<":"
            <<"total child has :"<<pipeline->total_children_;
        }else{
            ss<<"pipline :"<<pipeline->identify_;
        }
        ss<<"[";
        ss<<PrintOperatorName(pipeline->source_);
        ss<<"]---->";
        ss<<"[";
        for(int32_t i=0;i< pipeline->operators_.size();++i){
            ss<<PrintOperatorName(pipeline->operators_[i]);
            if(i!= 0){
                ss<<",";
            }
        }
        ss<<"]----->";
        ss<<"[";
        ss<<PrintOperatorName(pipeline->sink_);
        ss<<"]";

        ss<<"child has ";
        for(auto& child : pipeline->children_){
            ss<<child->identify_;
        }
        ss<<std::endl;
    }
    return ss.str();
}
std::string 
ExecuteContext::PrintOperatorName(PhysicalOperator* node){
    if(node==nullptr){
        return "";
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
            return "Materilzie Operator Node";
            break;
        }
        case SubqueryMaterializeOperatorNode:{
            return "Subquery Materialize Operator node";
            break;
        }
        case HashJoinOperatorNode:{
            return "hashJoin Operator Node";
            break;
        }
        case FilterOperatorNode:{
            return "FilterOperator Operator Node";
            break;
        }
        case InsertOperatorNode:{
            return "Insert Operator Node";
            break;
        }
        case ValuesOperatorNode:{
            return "ValuesOperator Operator Node";
            break;
        }
        case AggOperatorNode:{
            return "AggOperator Operator Node";
            break;
        }
        case LimitOperatorNode:{
            return "limit Operator Node";
            break;
        }
        case SortOperatorNode:{
            return "Sort Operator Node";
            break;
        }
        case SeqScanOperatorNode:{
            return "SeqScan Operator Node";
            break;
        }
        case ResultOperatorNode:{
            return "Result Operator Node";
        }
        default:
            break;
    }
    return "";
}
