#include "Execute/ExecutorNode/ExprExecutor.h"
#include "Execute/core/ColumnFactory.h"





ColumnRef ExprExecutor::execute(const std::string& col){

    std::vector<ValueUnion> n_vals;
    for(uint32_t i=0;i<chunk_->rows();++i){
        n_vals.push_back(expr_->Evalute(&chunk_,i));
    }
    CHEKC_THORW(n_vals.size() ==chunk_->rows());
    auto ref = ColumnFactory::CreateColumn(n_vals);
    ref->name_ = col;

    return ref;
}
