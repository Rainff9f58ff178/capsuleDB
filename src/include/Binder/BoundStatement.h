#pragma once

#include<string>
#include<memory.h>
#include<stdint.h>
#include <vector>

enum class StatementType : uint8_t {
  INVALID_STATEMENT,        // invalid statement type
  SELECT_STATEMENT,         // select statement type
  INSERT_STATEMENT,         // insert statement type
  UPDATE_STATEMENT,         // update statement type
  CREATE_STATEMENT,         // create statement type
  DELETE_STATEMENT,         // delete statement type
  EXPLAIN_STATEMENT,        // explain statement type
  DROP_STATEMENT,           // drop statement type
  INDEX_STATEMENT,          // index statement type
  VARIABLE_SET_STATEMENT,   // set variable statement type
  VARIABLE_SHOW_STATEMENT,  // show variable statement type
};
class BoundStatement{
public:
    explicit BoundStatement(StatementType type):type_(type){}
    virtual ~BoundStatement()=default;
    BoundStatement(){}
    StatementType type_{StatementType::INVALID_STATEMENT};
private:
};