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

#ifndef QUICKSTEP_EXPRESSIONS_WINDOW_AGGREGATION_WINDOW_AGGREGATE_FUNCTION_FACTORY_HPP_
#define QUICKSTEP_EXPRESSIONS_WINDOW_AGGREGATION_WINDOW_AGGREGATE_FUNCTION_FACTORY_HPP_

#include <string>

#include "expressions/window_aggregation/WindowAggregationID.hpp"
#include "utility/Macros.hpp"

namespace quickstep {

class WindowAggregateFunction;
namespace serialization { class WindowAggregateFunction; }

/** \addtogroup Expressions
 *  @{
 */

/**
 * @brief All-static factory class that provides access to the various concrete
 *        implementations of WindowAggregateFunction.
 *
 * WindowAggregateFunctionFactory allows client code to use any
 * WindowAggregateFunction in Quickstep in a generic way without having to know
 * about all the specific subclasses of WindowAggregateFunction. In particular,
 * it is used to deserialize WindowAggregateFunctions used in
 * WindowAggregationOperationState from their protobuf representations
 * (originally created by the optimizer) when deserializing a QueryContext.
 **/
class WindowAggregateFunctionFactory {
 public:
  /**
   * @brief Get a particular WindowAggregateFunction by its ID.
   *
   * @param agg_id The ID of the desired WindowAggregateFunction.
   * @return A reference to the singleton instance of the
   *         WindowAggregateFunction specified by agg_id.
   **/
  static const WindowAggregateFunction& Get(const WindowAggregationID agg_id);

  /**
   * @brief Get a particular WindowAggregateFunction by its name in SQL syntax.
   *
   * @param name The name of the desired WindowAggregateFunction in lower case.
   * @return A pointer to the WindowAggregateFunction specified by name, or NULL
   *         if name does not match any known WindowAggregateFunction.
   **/
  static const WindowAggregateFunction* GetByName(const std::string &name);

  /**
   * @brief Determine if a serialized protobuf representation of a
   *        WindowAggregateFunction is fully-formed and valid.
   *
   * @param proto A serialized protobuf representation of a
   *              WindowAggregateFunction to check for validity.
   * @return Whether proto is fully-formed and valid.
   **/
  static bool ProtoIsValid(const serialization::WindowAggregateFunction &proto);

  /**
   * @brief Get the WindowAggregateFunction represented by a proto.
   *
   * @warning It is an error to call this method with an invalid proto.
   *          ProtoIsValid() should be called first to check.
   *
   * @param proto A serialized protobuf representation of a
   *              WindowAggregateFunction.
   * @return The WindowAggregateFunction represented by proto.
   **/
  static const WindowAggregateFunction& ReconstructFromProto(
      const serialization::WindowAggregateFunction &proto);

 private:
  // Class is all-static and can not be instantiated.
  WindowAggregateFunctionFactory();

  DISALLOW_COPY_AND_ASSIGN(WindowAggregateFunctionFactory);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_EXPRESSIONS_WINDOW_AGGREGATION_WINDOW_AGGREGATE_FUNCTION_FACTORY_HPP_
