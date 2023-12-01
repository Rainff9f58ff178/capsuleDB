#pragma once

#include<memory>
#include<string>
#include<vector>






enum class TableReferenceType : uint8_t {
  INVALID = 0,         /**< Invalid table reference type. */
  BASE_TABLE = 1,      /**< Base table reference. */
  JOIN = 3,            /**< Output of join. */
  CROSS_PRODUCT = 4,   /**< Output of cartesian product. */
  EXPRESSION_LIST = 5, /**< Values clause. */
  SUBQUERY = 6,        /**< Subquery. */
  CTE = 7,             /**< CTE. */
  EMPTY = 8            /**< Placeholder for empty FROM. */
};


class BoundTabRef{
public:
    explicit BoundTabRef(TableReferenceType type):type_(type){}
    BoundTabRef()=default;
    virtual ~BoundTabRef()=default;




    TableReferenceType type_{TableReferenceType::INVALID};
};








