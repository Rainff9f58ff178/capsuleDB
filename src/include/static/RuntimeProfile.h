#pragma once
#include<vector>
#include<memory>
#include<string>
#include<atomic>
#include<unordered_map>
#include<sstream>
class RuntimeProfile{
public:


    class Counter{
    public: 
        Counter(std::string name):name_(std::move(name)){

        }

        void update(uint64_t value){
            time_.fetch_add(value);
        }
        std::atomic<uint64_t> time_;
        std::string name_;
    };

    RuntimeProfile(std::string name):name_(std::move(name)){

    }

    RuntimeProfile::Counter* add_counter(std::string name){
        counters_[name] = std::make_shared<Counter>(name);
        return counters_[name].get();
    }
    RuntimeProfile* create_child(std::string name){
        children_.push_back(std::make_shared<RuntimeProfile>(std::move(name)));
        return children_.back().get();
    }


    std::string toString(uint32_t level){
        std::stringstream ss;
        ss<<std::string(level*4,' ');
        ss<<name_<<std::endl;

        for(auto& c : counters_){
            ss<<std::string((level+1)*4 ,' ');
            ss<<c.first<<":"
            << (c.second->time_ / 1000000000.000000000) <<"s "<< (void*)(c.second.get()) <<std::endl;
        }
        ss<<std::endl;
        for(auto& child : children_){
            ss<<child->toString(level+1);
        }
        ss<<std::endl;
        return ss.str();
    }
private:
    std::string name_;
    std::vector<std::shared_ptr<RuntimeProfile>> children_;
    std::unordered_map<std::string,std::shared_ptr<Counter>> counters_;

};