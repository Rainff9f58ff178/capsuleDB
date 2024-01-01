//
// Copyright (c) 2015-2019 Vinnie Falco (vinnie dot falco at gmail dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/boostorg/beast
//

#ifndef BHO_BEAST_CORE_IMPL_FILE_STDIO_IPP
#define BHO_BEAST_CORE_IMPL_FILE_STDIO_IPP

#include <asio2/bho/beast/core/file_stdio.hpp>
#include <asio2/bho/config/workaround.hpp>
#include <asio2/bho/core/exchange.hpp>
#include <limits>

#if defined(BHO_MSVC)
#  pragma warning(push)
#  pragma warning(disable:4996) // warning C4996: 'fopen': This function or variable may be unsafe.
#endif

#if defined(BHO_GCC)
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Wunused-variable"
#  pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif

#if defined(BHO_CLANG)
#  pragma clang diagnostic push
#  pragma clang diagnostic ignored "-Wunused-variable"
#  pragma clang diagnostic ignored "-Wexceptions"
#  pragma clang diagnostic ignored "-Wdeprecated-declarations"
#  pragma clang diagnostic ignored "-Wunused-private-field"
#  pragma clang diagnostic ignored "-Wunused-local-typedef"
#  pragma clang diagnostic ignored "-Wunknown-warning-option"
#endif

namespace bho {
namespace beast {

file_stdio::
~file_stdio()
{
    if(f_)
        fclose(f_);
}

file_stdio::
file_stdio(file_stdio&& other)
    : f_(std::exchange(other.f_, nullptr))
{
}

file_stdio&
file_stdio::
operator=(file_stdio&& other)
{
    if(&other == this)
        return *this;
    if(f_)
        fclose(f_);
    f_ = other.f_;
    other.f_ = nullptr;
    return *this;
}

void
file_stdio::
native_handle(std::FILE* f)
{
    if(f_)
        fclose(f_);
    f_ = f;
}

void
file_stdio::
close(error_code& ec)
{
    if(f_)
    {
        int failed = fclose(f_);
        f_ = nullptr;
        if(failed)
        {
            ec.assign(errno, generic_category());
            return;
        }
    }
    ec = {};
}

void
file_stdio::
open(char const* path, file_mode mode, error_code& ec)
{
    if(f_)
    {
        fclose(f_);
        f_ = nullptr;
    }
    ec = {};

    char const* s;

    switch(mode)
    {
    default:
    case file_mode::read:
        s = "rb";
        break;

    case file_mode::scan:
        s = "rb";
        break;

    case file_mode::write:
        s = "wb+";
        break;

    case file_mode::write_new:
    {
        s = "wbx";
        break;
    }

    case file_mode::write_existing:
        s = "rb+";
        break;

    case file_mode::append:
        s = "ab";
        break;

    case file_mode::append_existing:
    {
        auto const f0 =
            std::fopen(path, "rb+");
        if(! f0)
        {
            ec.assign(errno, generic_category());
            return;
        }
        std::fclose(f0);
        s = "ab";
        break;
    }
    }

    f_ = std::fopen(path, s);
    if(! f_)
    {
        ec.assign(errno, generic_category());
        return;
    }
}

std::uint64_t
file_stdio::
size(error_code& ec) const
{
    if(! f_)
    {
        ec = make_error_code(errc::bad_file_descriptor);
        return 0;
    }
    long pos = std::ftell(f_);
    if(pos == -1L)
    {
        ec.assign(errno, generic_category());
        return 0;
    }
    int result = std::fseek(f_, 0, SEEK_END);
    if(result != 0)
    {
        ec.assign(errno, generic_category());
        return 0;
    }
    long size = std::ftell(f_);
    if(size == -1L)
    {
        ec.assign(errno, generic_category());
        std::fseek(f_, pos, SEEK_SET);
        return 0;
    }
    result = std::fseek(f_, pos, SEEK_SET);
    if(result != 0)
        ec.assign(errno, generic_category());
    else
        ec = {};
    return size;
}

std::uint64_t
file_stdio::
pos(error_code& ec) const
{
    if(! f_)
    {
        ec = make_error_code(errc::bad_file_descriptor);
        return 0;
    }
    long pos = std::ftell(f_);
    if(pos == -1L)
    {
        ec.assign(errno, generic_category());
        return 0;
    }
    ec = {};
    return pos;
}

void
file_stdio::
seek(std::uint64_t offset, error_code& ec)
{
    if(! f_)
    {
        ec = make_error_code(errc::bad_file_descriptor);
        return;
    }
    if(offset > static_cast<std::uint64_t>((std::numeric_limits<long>::max)()))
    {
        ec = make_error_code(errc::invalid_seek);
        return;
    }
    int result = std::fseek(f_,
        static_cast<long>(offset), SEEK_SET);
    if(result != 0)
        ec.assign(errno, generic_category());
    else
        ec = {};
}

std::size_t
file_stdio::
read(void* buffer, std::size_t n, error_code& ec) const
{
    if(! f_)
    {
        ec = make_error_code(errc::bad_file_descriptor);
        return 0;
    }
    auto nread = std::fread(buffer, 1, n, f_);
    if(std::ferror(f_))
    {
        ec.assign(errno, generic_category());
        return 0;
    }
    return nread;
}

std::size_t
file_stdio::
write(void const* buffer, std::size_t n, error_code& ec)
{
    if(! f_)
    {
        ec = make_error_code(errc::bad_file_descriptor);
        return 0;
    }
    auto nwritten = std::fwrite(buffer, 1, n, f_);
    if(std::ferror(f_))
    {
        ec.assign(errno, generic_category());
        return 0;
    }
    return nwritten;
}

} // beast
} // bho

#if defined(BHO_CLANG)
#  pragma clang diagnostic pop
#endif

#if defined(BHO_GCC)
#  pragma GCC diagnostic pop
#endif

#if defined(BHO_MSVC)
#  pragma warning(pop)
#endif

#endif
