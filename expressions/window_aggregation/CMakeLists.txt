#   Copyright 2011-2015 Quickstep Technologies LLC.
#   Copyright 2015 Pivotal Software, Inc.
#   Copyright 2016, Quickstep Research Group, Computer Sciences Department,
#   University of Wisconsin—Madison.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

QS_PROTOBUF_GENERATE_CPP(expressions_windowaggregation_WindowAggregateFunction_proto_srcs
                         expressions_windowaggregation_WindowAggregateFunction_proto_hdrs
                         WindowAggregateFunction.proto)

# Declare micro-libs:
add_library(quickstep_expressions_windowaggregation_WindowAggregateFunction
            WindowAggregateFunction.cpp
            WindowAggregateFunction.hpp)
add_library(quickstep_expressions_windowaggregation_WindowAggregateFunction_proto
            ${expressions_windowaggregation_WindowAggregateFunction_proto_srcs})
add_library(quickstep_expressions_windowaggregation_WindowAggregateFunctionAvg
            WindowAggregateFunctionAvg.cpp
            WindowAggregateFunctionAvg.hpp)
add_library(quickstep_expressions_windowaggregation_WindowAggregateFunctionFactory
            WindowAggregateFunctionFactory.cpp
            WindowAggregateFunctionFactory.hpp)
add_library(quickstep_expressions_windowaggregation_WindowAggregationHandle
            ../../empty_src.cpp
            WindowAggregationHandle.hpp)
add_library(quickstep_expressions_windowaggregation_WindowAggregationHandleAvg
            WindowAggregationHandleAvg.cpp
            WindowAggregationHandleAvg.hpp)
add_library(quickstep_expressions_windowaggregation_WindowAggregationID
            ../../empty_src.cpp
            WindowAggregationID.hpp)

# Link dependencies:
target_link_libraries(quickstep_expressions_windowaggregation_WindowAggregateFunction
                      glog
                      quickstep_expressions_windowaggregation_WindowAggregateFunction_proto
                      quickstep_expressions_windowaggregation_WindowAggregationID
                      quickstep_utility_Macros)
target_link_libraries(quickstep_expressions_windowaggregation_WindowAggregationHandle
                      glog
                      quickstep_catalog_CatalogTypedefs
                      quickstep_storage_HashTableBase
                      quickstep_types_TypedValue
                      quickstep_utility_Macros)
target_link_libraries(quickstep_expressions_windowaggregation_WindowAggregateFunction_proto
                      ${PROTOBUF_LIBRARY})
target_link_libraries(quickstep_expressions_windowaggregation_WindowAggregateFunctionFactory
                      quickstep_expressions_windowaggregation_WindowAggregateFunctionAvg
                      quickstep_expressions_windowaggregation_WindowAggregateFunction_proto
                      quickstep_expressions_windowaggregation_WindowAggregationID
                      quickstep_utility_Macros)
target_link_libraries(quickstep_expressions_windowaggregation_WindowAggregateFunctionAvg
                      quickstep_expressions_windowaggregation_WindowAggregateFunction
                      quickstep_expressions_windowaggregation_WindowAggregationHandleAvg
                      quickstep_expressions_windowaggregation_WindowAggregationID
                      quickstep_types_Type
                      quickstep_types_TypeFactory
                      quickstep_types_TypeID
                      quickstep_types_operations_binaryoperations_BinaryOperation
                      quickstep_types_operations_binaryoperations_BinaryOperationFactory
                      quickstep_types_operations_binaryoperations_BinaryOperationID
                      quickstep_utility_Macros)
target_link_libraries(quickstep_expressions_windowaggregation_WindowAggregationHandleAvg
                      quickstep_catalog_CatalogTypedefs
                      quickstep_expressions_windowaggregation_WindowAggregationHandle
                      quickstep_types_Type
                      quickstep_types_TypeFactory
                      quickstep_types_TypeID
                      quickstep_types_TypedValue
                      quickstep_types_containers_ColumnVector
                      quickstep_types_operations_binaryoperations_BinaryOperation
                      quickstep_types_operations_binaryoperations_BinaryOperationFactory
                      quickstep_types_operations_binaryoperations_BinaryOperationID
                      quickstep_utility_Macros)
