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

#ifndef QUICKSTEP_EXPRESSIONS_WINDOW_AGGREGATION_WINDOW_AGGREGATE_FUNCTION_AVG_HPP_
#define QUICKSTEP_EXPRESSIONS_WINDOW_AGGREGATION_WINDOW_AGGREGATE_FUNCTION_AVG_HPP_

#include <string>
#include <vector>

#include "expressions/window_aggregation/WindowAggregateFunction.hpp"
#include "expressions/window_aggregation/WindowAggregationID.hpp"
#include "utility/Macros.hpp"

namespace quickstep {

class WindowAggregationHandle;
class Type;

/** \addtogroup Expressions
 *  @{
 */

/**
 * @brief WindowAggregateFunction representing SQL AVG() OVER term.
 **/
class WindowAggregateFunctionAvg : public WindowAggregateFunction {
 public:
  static const WindowAggregateFunctionAvg& Instance() {
    static WindowAggregateFunctionAvg instance;
    return instance;
  }

  std::string getName() const override {
    return "AVG";
  }

  bool canApplyToTypes(
      const std::vector<const Type*> &argument_types) const override;

  const Type* resultTypeForArgumentTypes(
      const std::vector<const Type*> &argument_types) const override;

  WindowAggregationHandle* createHandle(
      const CatalogRelationSchema &relation,
      std::vector<const Type*> &&argument_types,
      std::vector<const Type*> &&partition_key_types) const override;

 private:
  WindowAggregateFunctionAvg()
      : WindowAggregateFunction(WindowAggregationID::kAvg) {
  }

  DISALLOW_COPY_AND_ASSIGN(WindowAggregateFunctionAvg);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_EXPRESSIONS_WINDOW_AGGREGATION_WINDOW_AGGREGATE_FUNCTION_AVG_HPP_
