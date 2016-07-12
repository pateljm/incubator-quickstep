/**
 *   Copyright 2011-2015 Quickstep Technologies LLC.
 *   Copyright 2015 Pivotal Software, Inc.
 *   Copyright 2016, Quickstep Research Group, Computer Sciences Department,
 *     University of Wisconsin—Madison.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 **/

#ifndef QUICKSTEP_EXPRESSIONS_WINDOW_AGGREGATION_WINDOW_AGGREGATION_HANDLE_AVG_HPP_
#define QUICKSTEP_EXPRESSIONS_WINDOW_AGGREGATION_WINDOW_AGGREGATION_HANDLE_AVG_HPP_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <queue>
#include <vector>

#include "catalog/CatalogTypedefs.hpp"
#include "expressions/window_aggregation/WindowAggregationHandle.hpp"
#include "types/Type.hpp"
#include "types/TypedValue.hpp"
#include "types/operations/binary_operations/BinaryOperation.hpp"
#include "types/operations/comparisons/Comparison.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace quickstep {

class ColumnVector;
class ColumnVectorsValueAccessor;
class InsertDestinationInterface;
class StorageManager;
class ValueAccessor;

/** \addtogroup Expressions
 *  @{
 */

/**
 * @brief A WindowAggregationHandle for average.
 **/
class WindowAggregationHandleAvg : public WindowAggregationHandle {
 public:
  ~WindowAggregationHandleAvg() override {}

  void calculate(const std::vector<std::unique_ptr<const Scalar>> &arguments,
                 const std::vector<attribute_id> &partition_by_ids,
                 const bool is_row,
                 const std::int64_t num_preceding,
                 const std::int64_t num_following,
                 StorageManager *storage_manager);

  std::vector<ValueAccessor*> finalize(StorageManager *storage_manager);

 private:
  friend class WindowAggregateFunctionAvg;

  /**
   * @brief Constructor.
   *
   * @param partition_by_ids The attribute_id of partition keys in
   *                         attribute_accessor.
   * @param is_row True if the frame mode is ROWS, false if it is RANGE.
   * @param num_preceding The number of rows/range that precedes the current row.
   * @param num_following The number of rows/range that follows the current row.
   * @param storage_manager A pointer to the storage manager.
   * @param type Type of the avg value.
   **/
  explicit WindowAggregationHandleAvg(const CatalogRelationSchema &relation,
                                      const std::vector<block_id> &block_ids,
                                      const Type &type,
                                      std::vector<const Type*> &&partition_key_types);

  TypedValue calculateOneWindow(
      std::vector<ValueAccessor*> &tuple_accessors,
      std::vector<ColumnVectorsValueAccessor*> &argument_accessors,
      const std::vector<attribute_id> &partition_by_ids,
      const std::uint32_t current_block_index,
      const bool is_row,
      const std::int64_t num_preceding,
      const std::int64_t num_following) const;

  bool samePartition(const std::vector<TypedValue> &current_row_partition_key,
                     ValueAccessor *tuple_accessor,
                     const tuple_id boundary_tuple_id,
                     const std::vector<attribute_id> &partition_by_ids) const;

  const Type &argument_type_;
  const Type *sum_type_;
  const Type *result_type_;
  std::unique_ptr<UncheckedBinaryOperator> fast_add_operator_;
  std::unique_ptr<UncheckedBinaryOperator> divide_operator_;
  std::vector<std::unique_ptr<UncheckedComparator>> equal_comparators_;

  DISALLOW_COPY_AND_ASSIGN(WindowAggregationHandleAvg);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_EXPRESSIONS_WINDOW_AGGREGATION_WINDOW_AGGREGATION_HANDLE_AVG_HPP_
