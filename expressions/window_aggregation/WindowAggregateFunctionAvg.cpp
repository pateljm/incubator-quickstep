/**
 *   Copyright 2011-2015 Quickstep Technologies LLC.
 *   Copyright 2015-2016 Pivotal Software, Inc.
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

#include "expressions/window_aggregation/WindowAggregateFunctionAvg.hpp"

#include <vector>

#include "expressions/window_aggregation/WindowAggregationHandleAvg.hpp"
#include "types/Type.hpp"
#include "types/TypeFactory.hpp"
#include "types/TypeID.hpp"
#include "types/operations/binary_operations/BinaryOperation.hpp"
#include "types/operations/binary_operations/BinaryOperationFactory.hpp"
#include "types/operations/binary_operations/BinaryOperationID.hpp"

#include "glog/logging.h"

namespace quickstep {

bool WindowAggregateFunctionAvg::canApplyToTypes(
    const std::vector<const Type*> &argument_types) const {
  // AVG is unary.
  if (argument_types.size() != 1) {
    return false;
  }

  // Argument must be addable and divisible.
  return BinaryOperationFactory::GetBinaryOperation(BinaryOperationID::kAdd)
             .canApplyToTypes(*argument_types.front(), *argument_types.front())
         && BinaryOperationFactory::GetBinaryOperation(BinaryOperationID::kDivide)
             .canApplyToTypes(*argument_types.front(), TypeFactory::GetType(kDouble));
}

const Type* WindowAggregateFunctionAvg::resultTypeForArgumentTypes(
    const std::vector<const Type*> &argument_types) const {
  if (!canApplyToTypes(argument_types)) {
    return nullptr;
  }

  // The type used to sum values is nullable, and we automatically widen int to
  // long and float to double to have more headroom when adding up many values.
  const Type *sum_type = &(argument_types.front()->getNullableVersion());
  switch (sum_type->getTypeID()) {
    case kInt:
      sum_type = &TypeFactory::GetType(kLong, true);
      break;
    case kFloat:
      sum_type = &TypeFactory::GetType(kDouble, true);
      break;
    default:
      break;
  }

  return BinaryOperationFactory::GetBinaryOperation(BinaryOperationID::kDivide)
             .resultTypeForArgumentTypes(*sum_type, TypeFactory::GetType(kDouble));
}

WindowAggregationHandle* WindowAggregateFunctionAvg::createHandle(
    const CatalogRelationSchema &relation,
    std::vector<const Type*> &&argument_types,
    std::vector<const Type*> &&partition_key_types) const {
  DCHECK(canApplyToTypes(argument_types))
      << "Attempted to create an WindowAggregationHandleAvg for argument Type(s)"
      << " that AVG can not be applied to.";

  return new WindowAggregationHandleAvg(relation,
                                        *argument_types.front(),
                                        std::move(partition_key_types));
}

}  // namespace quickstep
