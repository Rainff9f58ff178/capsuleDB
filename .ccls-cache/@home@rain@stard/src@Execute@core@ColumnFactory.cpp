


#include "Execute/core/ColumnFactory.h"

#include "Execute/core/ColumnVector.h"
#include "Execute/core/ColumnString.h"



template<class T>
ColumnRef ColumnFactory::CreateColumnVector(){
    auto col = std::make_shared<ColumnVector<T>>();
    col->max_width_ = sizeof(T);
    return col;
}
template<class T>
ColumnRef
ColumnFactory::CreateColumnVector(const std::string& name){
    auto col = std::make_shared<ColumnVector<T>>();
    col->max_width_ = sizeof(T);
    col->name_ = name;
    return col;
}

ColumnRef ColumnFactory::CreateColumnString(){
    return std::make_shared<ColumnString>();
}
ColumnRef 
ColumnFactory::CreateColumnString(const std::string& name){
    auto col  = std::make_shared<ColumnString>();
    col->name_ = name;
    return col;
}

ColumnRef
ColumnFactory::CreateColumnFrom(ValueUnion& value,const std::string& name){
    if(value.type_ == ValueType::TypeString){
        return CreateColumnString(name);
    }
    if(value.value_len_ == 4){
        return CreateColumnVector<int32_t>(name);
    }


}
