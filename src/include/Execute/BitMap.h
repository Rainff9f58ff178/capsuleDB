#pragma once
#include <vector>
#include <stdint.h>
#include "Table/Tuple.h"
/*
    this is the Infomation transport between 
    Operaters,
*/
enum VectorType{
    FLAT_DATA
};

/*
    this class represent a column datachunk.
*/
class Vector{
public:
    VectorType type_;
    std::vector<Value> vals;
};
class  DataChunk{
public:
    //this is the actully data.
    //data.size() equal to the target_list.
    std::vector<Vector> data;    
};


