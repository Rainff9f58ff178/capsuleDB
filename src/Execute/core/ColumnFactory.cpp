


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

    UNREACHABLE;
}

ColumnRef ColumnFactory::CreateColumn(const ValueUnionView& view){
    if(view.empty()){
        throw Exception(_error_msg("CreateEmpty"));
    }
    if(view[0].type_ == ValueType::TypeInt){
        auto col = CreateColumnVector<int32_t>();
        col->insertFrom(view);
        return col;
    }else if(view[0].type_== ValueType::TypeString){
        auto col = CreateColumnString();
        col->insertFrom(view);
        return col;
    }
    UNREACHABLE;
}