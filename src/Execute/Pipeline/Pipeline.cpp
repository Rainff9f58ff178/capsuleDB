#include "Execute/Pipeline/Pipeline.h"
#include <cassert>
Pipeline::Pipeline(ExecuteContext* context,uint32_t id):context_(context),
identify_(id){
    
}
std::shared_ptr<Pipeline> 
Pipeline::Complete(){
    auto parent = parent_.lock();
    if(parent==nullptr)
        assert(0 && "Impossiable parent pipeline is null");

    this->runed_=true;
    parent->finished_children_++;
    assert(parent->finished_children_ <= parent->total_children_);
    if(parent->finished_children_ == parent->total_children_){
        return parent;
    }
    return nullptr;
}