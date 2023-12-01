#pragma  once

#include "Binder/BoundStatement.h"
#include "Binder/BoundTabRef.h"
#include "Binder/BoundExpression.h"
#include "nodes/primnodes.hpp"
#include <charconv>



class SelectStatement:public BoundStatement{
public:

    explicit SelectStatement(std::unique_ptr<BoundTabRef> from,
        std::vector<std::unique_ptr<BoundExpression>>&& select_list,
        std::unique_ptr<BoundExpression> where,
        std::vector<std::unique_ptr<BoundExpression>> group_by,
        std::unique_ptr<BoundExpression> having,
        std::unique_ptr<BoundExpression> limit,
        bool is_distinct):
            BoundStatement(StatementType::SELECT_STATEMENT),
            from_(std::move(from)),
            select_list_(std::move(select_list)),
            where_(std::move(where)),
            group_by_(std::move(group_by)),
            having_(std::move(having)),
            limit_(std::move(limit)),
            is_distinct_(is_distinct){}


        SelectStatement()=default;
        ~SelectStatement()=default;


    





    std::unique_ptr<BoundTabRef> from_;

    std::vector<std::unique_ptr<BoundExpression>> select_list_;

    std::unique_ptr<BoundExpression> where_;
    
    std::vector<std::unique_ptr<BoundExpression>> group_by_;

    std::unique_ptr<BoundExpression> having_;

    std::unique_ptr<BoundExpression> limit_;

    // TODO(wxy):sort
    // std::unique_ptr<BoundOrderBy> sort_

    bool is_distinct_;


};