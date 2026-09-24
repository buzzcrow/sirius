/*
 * Copyright 2026, Sirius Contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <duckdb/common/value_operations/value_operations.hpp>
#include <duckdb/planner/filter/conjunction_filter.hpp>
#include <duckdb/planner/filter/constant_filter.hpp>
#include <duckdb/planner/filter/in_filter.hpp>
#include <op/scan/parquet_source_filter.hpp>

namespace sirius::op::scan {

source_filter_result evaluate_parquet_source_filter(duckdb::TableFilter const& filter,
                                                    duckdb::Value const& value)
{
  using result = source_filter_result;
  using type   = duckdb::TableFilterType;
  if (value.type() != duckdb::LogicalType::VARCHAR &&
      value.type() != duckdb::LogicalType::UBIGINT) {
    return result::UNKNOWN;
  }
  switch (filter.filter_type) {
    case type::IS_NULL: return value.IsNull() ? result::MATCH : result::NO_MATCH;
    case type::IS_NOT_NULL: return value.IsNull() ? result::NO_MATCH : result::MATCH;
    case type::CONSTANT_COMPARISON: {
      auto const& comparison = filter.Cast<duckdb::ConstantFilter>();
      switch (comparison.comparison_type) {
        case duckdb::ExpressionType::COMPARE_EQUAL:
        case duckdb::ExpressionType::COMPARE_NOTEQUAL:
        case duckdb::ExpressionType::COMPARE_LESSTHAN:
        case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
        case duckdb::ExpressionType::COMPARE_GREATERTHAN:
        case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO: break;
        default: return result::UNKNOWN;
      }
      if (comparison.constant.IsNull() || value.IsNull()) { return result::SQL_NULL; }
      if (comparison.constant.type() != value.type()) { return result::UNKNOWN; }
      return comparison.Compare(value) ? result::MATCH : result::NO_MATCH;
    }
    case type::IN_FILTER: {
      if (value.IsNull()) { return result::SQL_NULL; }
      bool has_null = false;
      bool unknown  = false;
      for (auto const& candidate : filter.Cast<duckdb::InFilter>().values) {
        if (candidate.IsNull()) {
          has_null = true;
          continue;
        }
        if (candidate.type() != value.type()) {
          unknown = true;
          continue;
        }
        if (duckdb::ValueOperations::Equals(candidate, value)) { return result::MATCH; }
      }
      return unknown ? result::UNKNOWN : has_null ? result::SQL_NULL : result::NO_MATCH;
    }
    case type::CONJUNCTION_AND:
    case type::CONJUNCTION_OR: {
      bool const is_and    = filter.filter_type == type::CONJUNCTION_AND;
      auto const& children = is_and ? filter.Cast<duckdb::ConjunctionAndFilter>().child_filters
                                    : filter.Cast<duckdb::ConjunctionOrFilter>().child_filters;
      bool unknown         = false;
      bool has_null        = false;
      for (auto const& child : children) {
        auto const r = evaluate_parquet_source_filter(*child, value);
        if (is_and && r == result::NO_MATCH) { return result::NO_MATCH; }
        if (!is_and && r == result::MATCH) { return result::MATCH; }
        unknown |= r == result::UNKNOWN;
        has_null |= r == result::SQL_NULL;
      }
      if (unknown) { return result::UNKNOWN; }
      if (has_null) { return result::SQL_NULL; }
      return is_and ? result::MATCH : result::NO_MATCH;
    }
    default: return result::UNKNOWN;
  }
}

}  // namespace sirius::op::scan
