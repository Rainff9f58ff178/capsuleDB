#pragma once

#include "Execute/ExecutorNode/PhysicalOperator.h"
#include "common/Hash/HashUtils.h"


class AggregateKey{
public:
    std::vector<ValueUnion> group_by_val_;
    AggregateKey(std::vector<ValueUnion>&& vals):group_by_val_(std::move(vals)){

    }

    AggregateKey(const AggregateKey& other ){
        for(auto& val : other.group_by_val_)    {
            group_by_val_.push_back(std::move(val.clone()));
        }
    }

    bool operator==(const AggregateKey& other) const {
        if(group_by_val_.size() != other.group_by_val_.size())
            return false;

        for(uint32_t i=0;i<group_by_val_.size();++i){
            if(group_by_val_[i] != other.group_by_val_[i])
                return false;
        }
        return true;
    }
};
namespace std{
    template<>
    struct hash<AggregateKey>{
        auto operator()(const AggregateKey& agg) const ->std::size_t{
            hash_t current_hash=0;
            for(auto& val : agg.group_by_val_){
                if(val.type_==TypeInt)
                    current_hash = Hashutils::CombineHashes(current_hash,Hashutils::Hash<ValueUnion::__value>(&val.val_));
                else if(val.type_==TypeString){
                    current_hash = Hashutils::CombineHashes(current_hash, Hashutils::HashBytes(val.val_.data_,val.value_len_));
                }else {
                    UNREACHABLE;
                }
            }
            return current_hash;
        }   
    };
}

class AggregatePhysicalOperator : public PhysicalOperator{
    static constexpr const OperatorType type_ = 
            OperatorType::AggOperatorNode;
public:
    
    AggregatePhysicalOperator(LogicalOperatorRef plan,
        ExecuteContext* context,
        std::vector<PhysicalOperatorRef> children):PhysicalOperator(std::move(plan),context,std::move(children)){

    }

    bool IsSink() override{
        return true;
    }                        

    OperatorType GetType() override{
        return  type_;
    }
    void source_init() override;
    SourceResult Source(ChunkRef& chunk) override;
    SinkResult Sink(ChunkRef& chunk) override;
    OperatorResult Execute(ChunkRef& chunk) override;

private:
    std::unordered_map<AggregateKey,ChunkRef> agg_map_{};
    std::vector<ChunkRef> chunks_;
    uint32_t offset_{0};
    std::vector<ValueUnion> __CalculateBucket(ChunkRef& chunk);
};