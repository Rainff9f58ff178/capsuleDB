

#include <stdint.h>
#pragma once
#include <vector>
#include <string>
#include<memory>
#include<sstream>
#include "common/Exception.h"

enum class ExpressionType : uint8_t {
  INVALID = 0,    /**< Invalid expression type. */
  CONSTANT = 1,   /**< Constant expression type. */
  COLUMN_REF = 3, /**< A column in a table. */
  TYPE_CAST = 4,  /**< Type cast expression type. */
  FUNCTION = 5,   /**< Function expression type. */
  AGG_CALL = 6,   /**< Aggregation function expression type. */
  STAR = 7,       /**< Star expression type, will be rewritten by binder and won't appear in plan. */
  UNARY_OP = 8,   /**< Unary expression type. */
  BINARY_OP = 9,  /**< Binary expression type. */
  ALIAS = 10,     /**< Alias expression type. */
};


class BoundExpression{
public:
    explicit BoundExpression(ExpressionType type):type_(type){}
    BoundExpression()=default;
    virtual ~BoundExpression()=default;

    virtual inline bool isInvalid(){return type_ == ExpressionType::INVALID;}

    virtual inline ExpressionType GetType(){return type_;}
    virtual std::unique_ptr<BoundExpression> Copy(){
        UNREACHABLE
    }
    virtual std::string ToString() const{
        UNREACHABLE;
    }
    virtual bool HasAgg(){
        UNREACHABLE;
    }
    std::optional<std::string> alias_=std::nullopt;
    ExpressionType type_{ExpressionType::INVALID};
};