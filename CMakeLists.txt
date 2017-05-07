    cmake_minimum_required(VERSION 3.7)
    project(sql4300)
    set(CMAKE_VERBOSE_MAKEFILE ON)
    set(CMAKE_CXX_STANDARD 11)

set(SOURCE_FILES
        heap_storage.cpp
        heap_storage.h
        sql4300.cpp
        storage_engine.h ParseTreeToString.cpp ParseTreeToString.h SQLExec.cpp SQLExec.h schema_tables.h schema_tables.cpp storage_engine.cpp EvalPlan.cpp EvalPlan.h btree.cpp btree.h BTreeNode.cpp BTreeNode.h)

    include_directories(/Users/uemurar51/sql-parser/src/)

    add_executable(sql4300 ${SOURCE_FILES})

    target_link_libraries (sql4300 db_cxx-6.2 sqlparser)
