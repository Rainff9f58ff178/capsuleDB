#pragma once



#include "Planer/MaterilizeLogicaloperator.h"




class SubqueryMaterializeLogicalOperator:public MaterilizeLogicaloperator {
    static constexpr const OperatorType type_ =
        OperatorType::SubqueryMaterializeOperatorNode;
public:

    SubqueryMaterializeLogicalOperator(std::vector<LogicalOperatorRef> child,
    SchemaRef selcet_list):MaterilizeLogicaloperator(std::move(child)),select_list_(std::move(selcet_list)){

    }




    COPY_PLAN_WITH_CHILDREN(SubqueryMaterializeLogicalOperator);

    OperatorType GetType() override{
        return  type_;
    }


    void PrintDebug() override{
        if(!show_info)
            return;;

        if(ouput_schema_){
            std::cout<<" | output_schema: ";
            for(auto& col :ouput_schema_->columns_){
                std::cout<<" "<<col.name_<<" ";
            }
        }
        std::cout<<" | intput_schema: ";
        for(auto& col:input_schema_->columns_){
            std::cout<<" "<<col.name_<<" ";
        }
    }
    SchemaRef select_list_{nullptr};
};