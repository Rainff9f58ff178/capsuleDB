cmake_minimum_required(VERSION 3.20)


set(CXX_STANDARD 23)

add_subdirectory(Storage)
add_subdirectory(logger)
set(SRC Database.cpp
        Env.cpp
        Binder/Binder.cpp
        CataLog/Column.cpp
        CataLog/Schema.cpp
        CataLog/CataLog.cpp
        CataLog/TableHeap.cpp
        CataLog/TableCataLog.cpp
        CataLog/PageIterator/ColumnNumIterator.cpp
        CataLog/PageIterator/ColumnStringIterator.cpp 
        CataLog/PageIterator/ColumnIterator.cpp
        Table/Tuple.cpp
        Expressions/ColumnValueExpression.cpp
        Expressions/ConstantValueExpression.cpp
        Planer/LogicalOperator.cpp
        Planer/Planer.cpp 
        Planer/ValuesLogicalOperator.cpp 
        Optimizer/Optimizer.cpp
        Execute/ExecutorNode/ExprExecutor.cpp
        Execute/ExecutorNode/InsertPhysicalOperator.cpp 
        Execute/ExecutorNode/PhysicalOperator.cpp 
        Execute/ExecutorNode/ValuesPhysicalOperator.cpp 
        Execute/ExecutorNode/SeqScanPhysicalOperator.cpp
        Execute/ExecutorNode/MaterializePhysicalOperator.cpp
        Execute/ExecutorNode/ResultPhysicalOperator.cpp
        Execute/ExecutorNode/HashJoinPhysicalOperator.cpp
        Execute/ExecutorNode/LimitPhysicalOperator.cpp 
        Execute/ExecutorNode/SortPhysicalOperator.cpp 
        Execute/ExecutorNode/AggregatePhysicalOperator.cpp
        Execute/Pipeline/Pipeline.cpp 
        Execute/ExecuteContext.cpp 
        Execute/ExecuteEngine.cpp
        Execute/core/Chunk.cpp 
        Execute/core/Column.cpp 
        Execute/core/ColumnFactory.cpp 
        Execute/core/ColumnString.cpp 
        Execute/core/ColumnVector.cpp 
        Utils/Scheduler.cpp
        Utils/AsyncScheduler.cpp 
        Utils/ThreadPool.cpp 
        common/Value.cpp
        )

set(THIRD_PARTY_LIBS duckdb_pg_query)



add_library(stard_db ${SRC})

target_include_directories(stard_db
            PUBLIC 
            ${CMAKE_CURRENT_SOURCE_DIR}/include
            ${CMAKE_CURRENT_SOURCE_DIR}/Storage
            ${CMAKE_CURRENT_SOURCE_DIR}/logger
            )

target_link_libraries(stard_db ${THIRD_PARTY_LIBS} store log)
