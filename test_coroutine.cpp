

#include "./src/include/Utils/Generator.h"
#include <iostream>
Generator<int> genInt(){
    co_return 1;
}
int main(){
    auto gen=  genInt();
    auto i =gen();
    
    std::cout<<i<<std::endl;
    std::cout<<gen.finish()<<std::endl;

    return  0;
}