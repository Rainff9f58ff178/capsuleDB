//
// Copyright (c) 2016-2019 Vinnie Falco (vinnie dot falco at gmail dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/boostorg/beast
//

#ifndef BHO_BEAST_DETAIL_VARIANT_HPP
#define BHO_BEAST_DETAIL_VARIANT_HPP

#include <asio2/bho/beast/core/detail/type_traits.hpp>
#include <asio2/bho/assert.hpp>

namespace bho {
namespace beast {
namespace detail {

// This simple variant gets the job done without
// causing too much trouble with template depth:
//
// * Always allows an empty state I==0
// * emplace() and get() support 1-based indexes only
// * Basic exception guarantee
// * Max 255 types
//

} // detail
} // beast
} // bho

#endif
