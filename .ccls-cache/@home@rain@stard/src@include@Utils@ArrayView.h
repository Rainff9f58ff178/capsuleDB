#pragma once

#include<cstdint>
#include<cstddef>


template <typename T>
class array_view {
public:
    array_view(T* ptr, size_t len) noexcept : _ptr(ptr), _len(len) {}
    template <class Container>
    array_view(const Container& container) noexcept : _ptr(container.data()), _len(container.size()) {}

    T const& operator[](int i) const noexcept { return _ptr[i]; }
    size_t size() const noexcept { return _len; }
    bool empty() const noexcept { return size() == 0; }

    auto begin() const noexcept { return _ptr; }
    auto end() const noexcept { return _ptr + _len; }

private:
    const T* _ptr = nullptr;
    size_t _len;
};