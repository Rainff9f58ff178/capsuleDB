#include "Execute/ExecutorNode/ExprExecutor.h"
#include "Execute/core/ColumnFactory.h"
#include "Expressions/ColumnValueExpression.h"





ColumnRef ExprExecutor::execute(const std::string& col){
    if(expr_->GetType() == LogicalExpressionType::ColumnValueExpr){
        auto col_expr = down_cast<ColumnValueExpression&>(*expr_);
        auto ref  = chunk_->getColumnByname(col_expr.column_info().name_);
        ref->name_ = col;
        return ref;
    }

    std::vector<ValueUnion> n_vals;
    n_vals.reserve(chunk_->rows());
    for(uint32_t i=0;i<chunk_->rows();++i){
        n_vals.push_back(expr_->Evalute(&chunk_,i));
    }
    CHEKC_THORW(n_vals.size() ==chunk_->rows());
    auto ref = ColumnFactory::CreateColumn(n_vals);
    ref->name_ = col;

    return ref;
}
