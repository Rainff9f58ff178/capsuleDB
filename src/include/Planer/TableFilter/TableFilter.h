#pragma once
#include<cstdint>

enum class TableFilterType: uint8_t{
    CONSTANT_COMPARISON = 0, // constant comparison (e.g. =C, >C, >=C, <C, <=C)
	IS_NULL = 1,
	IS_NOT_NULL = 2,
	CONJUNCTION_OR = 3,
	CONJUNCTION_AND = 4
};

class TableFilter{
public:
    TableFilter();
};