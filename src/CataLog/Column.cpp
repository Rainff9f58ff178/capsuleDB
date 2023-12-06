


#include "CataLog/Column.h"




Column::Column(std::string name,uint32_t offset):name_(std::move(name)),idx_(offset){

}
Column::Column(std::string name,ColumnType type):name_(std::move(name)),type_(type){
    
}

Column::Column(std::string name,ColumnType type,column_idx_t idx):name_(std::move(name)),type_(type),idx_(idx){
    
}