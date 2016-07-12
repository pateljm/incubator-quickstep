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

#include "expressions/window_aggregation/WindowAggregationHandleAvg.hpp"

#include <cstddef>
#include <memory>
#include <vector>

#include "catalog/CatalogTypedefs.hpp"
#include "expressions/scalar/Scalar.hpp"
#include "expressions/scalar/ScalarAttribute.hpp"
#include "storage/InsertDestinationInterface.hpp"
#include "storage/StorageBlock.hpp"
#include "storage/StorageManager.hpp"
#include "storage/SubBlocksReference.hpp"
#include "storage/TupleStorageSubBlock.hpp"
#include "storage/ValueAccessor.hpp"
#include "storage/ValueAccessorUtil.hpp"
#include "types/Type.hpp"
#include "types/TypeFactory.hpp"
#include "types/TypeID.hpp"
#include "types/TypedValue.hpp"
#include "types/containers/ColumnVectorsValueAccessor.hpp"
#include "types/operations/binary_operations/BinaryOperation.hpp"
#include "types/operations/binary_operations/BinaryOperationFactory.hpp"
#include "types/operations/binary_operations/BinaryOperationID.hpp"
#include "types/operations/comparisons/Comparison.hpp"
#include "types/operations/comparisons/ComparisonFactory.hpp"
#include "types/operations/comparisons/ComparisonID.hpp"

#include "glog/logging.h"

namespace quickstep {

class StorageManager;

WindowAggregationHandleAvg::WindowAggregationHandleAvg(
    const CatalogRelationSchema &relation,
    const std::vector<block_id> &block_ids,
    const Type &type,
    std::vector<const Type*> &&partition_key_types)
    : WindowAggregationHandle(relation, block_ids),
      argument_type_(type) {
  // We sum Int as Long and Float as Double so that we have more headroom when
  // adding many values.
  TypeID type_id;
  switch (type.getTypeID()) {
    case kInt:
    case kLong:
      type_id = kLong;
      break;
    case kFloat:
    case kDouble:
      type_id = kDouble;
      break;
    default:
      type_id = type.getTypeID();
      break;
  }

  sum_type_ = &(TypeFactory::GetType(type_id));

  // Result is nullable, because AVG() over 0 values (or all NULL values) is
  // NULL.
  result_type_
      = &(BinaryOperationFactory::GetBinaryOperation(BinaryOperationID::kDivide)
              .resultTypeForArgumentTypes(*sum_type_, TypeFactory::GetType(kDouble))
                  ->getNullableVersion());

  // Make operators to do arithmetic:
  // Add operator for summing argument values.
  fast_add_operator_.reset(
      BinaryOperationFactory::GetBinaryOperation(BinaryOperationID::kAdd)
          .makeUncheckedBinaryOperatorForTypes(*sum_type_, argument_type_));
  // Divide operator for dividing sum by count to get final average.
  divide_operator_.reset(
      BinaryOperationFactory::GetBinaryOperation(BinaryOperationID::kDivide)
          .makeUncheckedBinaryOperatorForTypes(*sum_type_, TypeFactory::GetType(kDouble)));
  // Comparison operators for checking if two tuples belong to the same partition.
  for (const Type *partition_key_type : partition_key_types) {
    equal_comparators_.emplace_back(
        ComparisonFactory::GetComparison(ComparisonID::kEqual)
            .makeUncheckedComparatorForTypes(*partition_key_type, *partition_key_type));
  }
}

void WindowAggregationHandleAvg::calculate(const std::vector<std::unique_ptr<const Scalar>> &arguments,
                                           const std::vector<attribute_id> &partition_by_ids,
                                           const bool is_row,
                                           const std::int64_t num_preceding,
                                           const std::int64_t num_following,
                                           StorageManager *storage_manager) {
  DCHECK(arguments.size() == 1);
  
  // Initialize the tuple accessors and argument accessors.
  // Index of each value accessor indicates the block it belongs to.
  std::vector<ValueAccessor*> tuple_accessors;
  std::vector<ColumnVectorsValueAccessor*> argument_accessors;
  for (block_id bid : block_ids_) {
    // Get tuple accessor.
    BlockReference block = storage_manager->getBlock(bid, relation_);
    const TupleStorageSubBlock &tuple_block = block->getTupleStorageSubBlock();
    ValueAccessor *tuple_accessor = tuple_block.createValueAccessor();
    tuple_accessors.push_back(tuple_accessor);

    // Get argument accessor.
    ColumnVectorsValueAccessor *argument_accessor = new ColumnVectorsValueAccessor();
    SubBlocksReference sub_block_ref(tuple_block,
                                     block->getIndices(),
                                     block->getIndicesConsistent());
    argument_accessor->addColumn(
        arguments.front()->getAllValues(tuple_accessor, &sub_block_ref));
    argument_accessors.push_back(argument_accessor);
  }

  // Create a window for each tuple and calculate the window aggregate.
  for (std::uint32_t current_block_index = 0;
       current_block_index < block_ids_.size();
       ++current_block_index) {
    ValueAccessor *tuple_accessor = tuple_accessors[current_block_index];
    ColumnVectorsValueAccessor* argument_accessor =
        argument_accessors[current_block_index];
    NativeColumnVector* window_aggregates_for_block =
        new NativeColumnVector(*result_type_, argument_accessor->getNumTuples());

    InvokeOnAnyValueAccessor (
        tuple_accessor,
        [&] (auto *tuple_accessor) -> void {
      tuple_accessor->beginIteration();
      argument_accessor->beginIteration();
      
      while (tuple_accessor->next() && argument_accessor->next()) {
        const TypedValue window_aggregate = this->calculateOneWindow(tuple_accessors,
                                                                     argument_accessors,
                                                                     partition_by_ids,
                                                                     current_block_index,
                                                                     is_row,
                                                                     num_preceding,
                                                                     num_following);
        window_aggregates_for_block->appendTypedValue(window_aggregate);
      }
    });

    window_aggregates_.push_back(window_aggregates_for_block);
  }
}

std::vector<ValueAccessor*> WindowAggregationHandleAvg::finalize(
    StorageManager *storage_manager) {
  std::vector<ValueAccessor*> accessors;
  
  // Create a ValueAccessor for each block, including the new window aggregate
  // attribute.
  for (std::size_t block_idx = 0; block_idx < block_ids_.size(); ++block_idx) {
    // Get the block information.
    BlockReference block = storage_manager->getBlock(block_ids_[block_idx],
                                                     relation_);
    const TupleStorageSubBlock &tuple_block = block->getTupleStorageSubBlock();
    ValueAccessor *block_accessor = tuple_block.createValueAccessor();
    SubBlocksReference sub_block_ref(tuple_block,
                                     block->getIndices(),
                                     block->getIndicesConsistent());
    ColumnVectorsValueAccessor* accessor = new ColumnVectorsValueAccessor();

    // Add all attributes in the original relation.
    for (CatalogRelationSchema::const_iterator attr_it = relation_.begin();
         attr_it != relation_.end();
         ++attr_it) {
      ScalarAttribute scalar_attr(*attr_it);
      accessor->addColumn(scalar_attr.getAllValues(block_accessor, &sub_block_ref));
    }

    // Add the window aggregate attribute
    accessor->addColumn(window_aggregates_[block_idx]);

    accessors.push_back(accessor);
  }

  return accessors;
}

TypedValue WindowAggregationHandleAvg::calculateOneWindow(
    std::vector<ValueAccessor*> &tuple_accessors,
    std::vector<ColumnVectorsValueAccessor*> &argument_accessors,
    const std::vector<attribute_id> &partition_by_ids,
    const std::uint32_t current_block_index,
    const bool is_row,
    const std::int64_t num_preceding,
    const std::int64_t num_following) const {
  // Initialize.
  ValueAccessor *tuple_accessor = tuple_accessors[current_block_index];
  ColumnVectorsValueAccessor *argument_accessor = argument_accessors[current_block_index];
  TypedValue sum = sum_type_->makeZeroValue();
  TypedValue current_value = argument_accessor->getTypedValue(0);
  // If current value is null, return null.
  if (current_value.isNull()) {
    return TypedValue(result_type_->getTypeID());
  }
  
  sum = fast_add_operator_->
      applyToTypedValues(sum, current_value);
  std::uint64_t count = 1;
  
  // Get the partition key for the current row.
  std::vector<TypedValue> current_row_partition_key;
  for (attribute_id partition_by_id : partition_by_ids) {
    current_row_partition_key.push_back(
        tuple_accessor->getTypedValueVirtual(partition_by_id));
  }

  // Get current position.
  tuple_id current_tuple_id = tuple_accessor->getCurrentPositionVirtual();
  
  // Find preceding tuples.
  int count_preceding = 0;
  tuple_id preceding_tuple_id = current_tuple_id;
  block_id preceding_block_index = current_block_index;
  while (num_preceding == -1 || count_preceding < num_preceding) {
    preceding_tuple_id--;

    // If the preceding tuple locates in the previous block, move to the
    // previous block and continue searching.
    // TODO(Shixuan): If it is possible to have empty blocks, "if" has to be
    // changed to "while".
    if (preceding_tuple_id < 0) {
      // First tuple of the first block, no more preceding blocks.
      if (preceding_block_index == 0) {
        break;
      }
      preceding_block_index--;

      tuple_accessor = tuple_accessors[preceding_block_index];
      argument_accessor = argument_accessors[preceding_block_index];
      preceding_tuple_id = argument_accessor->getNumTuples() - 1;
    }

    // Get the partition keys and compare. If not the same partition as the
    // current row, end searching preceding tuples.
    if (!samePartition(current_row_partition_key,
                       tuple_accessor,
                       preceding_tuple_id,
                       partition_by_ids)) {
      break;
    }


    // Actually count the element and do the calculation.
    count_preceding++;
    TypedValue preceding_value =
        argument_accessor->getTypedValueAtAbsolutePosition(0, preceding_tuple_id);
        
    // If a null value is in the window, return a null value.
    if (preceding_value.isNull()) {
      return TypedValue(result_type_->getTypeID());
    }
    
    sum = fast_add_operator_->applyToTypedValues(sum, preceding_value);
  }

  count += count_preceding;

  // Find following tuples.
  int count_following = 0;
  tuple_id following_tuple_id = current_tuple_id;
  block_id following_block_index = current_block_index;
  while (num_following == -1 || count_following < num_following) {
    following_tuple_id++;

    // If the following tuple locates in the next block, move to the next block
    // and continue searching.
    // TODO(Shixuan): If it is possible to have empty blocks, "if" has to be
    // changed to "while".
    if (following_tuple_id >= argument_accessor->getNumTuples()) {
      following_block_index++;
      // Last tuple of the last block, no more following blocks.
      if (following_block_index == tuple_accessors.size()) {
        break;
      }

      tuple_accessor = tuple_accessors[following_block_index];
      argument_accessor = argument_accessors[following_block_index];
      following_tuple_id = 0;
    }

    // Get the partition keys and compare. If not the same partition as the
    // current row, end searching preceding tuples.
    if (!samePartition(current_row_partition_key,
                       tuple_accessor,
                       following_tuple_id,
                       partition_by_ids)) {
      break;
    }


    // Actually count the element and do the calculation.
    count_following++;
    TypedValue following_value =
        argument_accessor->getTypedValueAtAbsolutePosition(0, following_tuple_id);
        
    // If a null value is in the window, return a null value.
    if (following_value.isNull()) {
      return TypedValue(result_type_->getTypeID());
    }
    
    sum = fast_add_operator_->applyToTypedValues(sum, following_value);
  }

  count += count_following;


  return divide_operator_->applyToTypedValues(sum,
                                              TypedValue(static_cast<double>(count)));
}

bool WindowAggregationHandleAvg::samePartition(
    const std::vector<TypedValue> &current_row_partition_key,
    ValueAccessor *tuple_accessor,
    const tuple_id boundary_tuple_id,
    const std::vector<attribute_id> &partition_by_ids) const {
  return InvokeOnAnyValueAccessor (tuple_accessor,
                                   [&] (auto *tuple_accessor) -> bool {
    for (std::size_t partition_by_index = 0;
         partition_by_index < partition_by_ids.size();
         ++partition_by_index) {
      if (!equal_comparators_[partition_by_index]->compareTypedValues(
              current_row_partition_key[partition_by_index],
              tuple_accessor->getTypedValueAtAbsolutePosition(
                  partition_by_ids[partition_by_index], boundary_tuple_id))) {
        return false;
      }
    }

    return true;
  });
}

}  // namespace quickstep