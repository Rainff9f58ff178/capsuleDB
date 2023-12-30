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


    std::string PrintDebug() override{
        std::stringstream ss;
        if(!show_info)
            return "";

        if(ouput_schema_){
            ss<<" | output_schema: ";
            for(auto& col :ouput_schema_->columns_){
                ss<<" "<<col.name_<<" ";
            }
        }
        ss<<" | intput_schema: ";
        for(auto& col:input_schema_->columns_){
            ss<<" "<<col.name_<<" ";
        }
        return ss.str();
    }
    SchemaRef select_list_{nullptr};
};