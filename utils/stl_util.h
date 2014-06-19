/*
 * stl_util.h
 *
 *  Created on: Jun 19, 2014
 *      Author: ylu
 */

#ifndef STL_UTIL_H
#define STL_UTIL_H

#include <set>
#include <map>
#include <vector>
#include <algorithm>
#include <iterator>
#include <sstream>
#include <iostream>
#include <iomanip>

template <typename T>
std::string tostr(const T& t)
{
    std::stringstream strm;
    strm << t;
    return strm.str();
}

#endif
