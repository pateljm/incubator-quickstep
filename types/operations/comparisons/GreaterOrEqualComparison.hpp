/**
 *   Copyright 2011-2015 Quickstep Technologies LLC.
 *   Copyright 2015 Pivotal Software, Inc.
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

#ifndef QUICKSTEP_TYPES_OPERATIONS_COMPARISONS_GREATER_OR_EQUAL_COMPARISON_HPP_
#define QUICKSTEP_TYPES_OPERATIONS_COMPARISONS_GREATER_OR_EQUAL_COMPARISON_HPP_

#include "types/operations/comparisons/BasicComparison.hpp"
#include "types/operations/comparisons/ComparisonID.hpp"
#include "utility/Macros.hpp"

namespace quickstep {

class Type;
class TypedValue;
class UncheckedComparator;

/** \addtogroup Types
 *  @{
 */

/**
 * @brief The greater-than-or-equal Comparison.
 **/
class GreaterOrEqualComparison : public BasicComparison {
 public:
  /**
   * @brief Get a reference to the singleton instance of this Operation.
   *
   * @return A reference to the singleton instance of this Operation.
   **/
  static const GreaterOrEqualComparison& Instance() {
    static GreaterOrEqualComparison instance;
    return instance;
  }

  bool compareTypedValuesChecked(const TypedValue &left,
                                 const Type &left_type,
                                 const TypedValue &right,
                                 const Type &right_type) const override;

  UncheckedComparator* makeUncheckedComparatorForTypes(const Type &left,
                                                       const Type &right) const override;

 private:
  GreaterOrEqualComparison() : BasicComparison(ComparisonID::kGreaterOrEqual) {}

  DISALLOW_COPY_AND_ASSIGN(GreaterOrEqualComparison);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_TYPES_OPERATIONS_COMPARISONS_GREATER_OR_EQUAL_COMPARISON_HPP_
