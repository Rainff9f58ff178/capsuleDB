
#pragma once
#include <cstdint>
#include<string>


class Column{
public:

    explicit Column(std::string name,uint32_t offset);
    Column(std::string name);
    Column() = default;
    ~Column()=default;
    
    bool operator==(const Column& other){
        return name_==other.name_;
    }
    std::string name_;
    uint32_t fix_length_=4;
    uint32_t offset_=0; // offset int tuple
};