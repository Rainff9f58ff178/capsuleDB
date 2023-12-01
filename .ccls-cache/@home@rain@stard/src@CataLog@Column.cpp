


#include "CataLog/Column.h"




Column::Column(std::string name,uint32_t offset):name_(std::move(name)),offset_(offset){

}
Column::Column(std::string name):name_(std::move(name)){
    
}