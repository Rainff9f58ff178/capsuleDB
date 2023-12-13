#pragma once

#include "Binder/BoundExpression.h"





class BoundAgg:public BoundExpression{
public:

    explicit BoundAgg(std::string agg_name,bool is_distinct,
        std::vector<std::unique_ptr<BoundExpression>> args):
        BoundExpression(ExpressionType::AGG_CALL),
        agg_name_(std::move(agg_name)),
        is_distinct_(is_distinct),
        args_(std::move(args)){}
    
    BoundAgg()=default;
    ~BoundAgg()=default;

    std::unique_ptr<BoundExpression> Copy()override{
        std::vector<std::unique_ptr<BoundExpression>> args;
        for(auto& arg : args_){
            args.push_back(std::move(arg->Copy()));
        }
        return std::make_unique<BoundAgg>(agg_name_,is_distinct_,std::move(args));
    }
    std::string ToString() const override{
        std::stringstream ss;
        ss<<agg_name_;
        ss<<"(";
        for(uint32_t i=0;i<args_.size();++i){
            ss<<args_[i]->ToString();
            if(i!= args_.size()-1)
                ss<<",";
        }
        ss<<")";
        return ss.str();
    }
    bool HasAgg() override{
        return true;
    }
    std::string agg_name_;
    bool is_distinct_;
    std::vector<std::unique_ptr<BoundExpression>> args_;
};