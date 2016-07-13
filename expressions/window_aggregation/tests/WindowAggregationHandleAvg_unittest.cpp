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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "catalog/CatalogTypedefs.hpp"
#include "expressions/window_aggregation/WindowAggregateFunction.hpp"
#include "expressions/window_aggregation/WindowAggregateFunctionFactory.hpp"
#include "expressions/window_aggregation/WindowAggregationHandle.hpp"
#include "expressions/window_aggregation/WindowAggregationHandleAvg.hpp"
#include "expressions/window_aggregation/WindowAggregationID.hpp"
#include "types/CharType.hpp"
#include "types/DateOperatorOverloads.hpp"
#include "types/DatetimeIntervalType.hpp"
#include "types/DoubleType.hpp"
#include "types/FloatType.hpp"
#include "types/IntType.hpp"
#include "types/IntervalLit.hpp"
#include "types/LongType.hpp"
#include "types/Type.hpp"
#include "types/TypeFactory.hpp"
#include "types/TypeID.hpp"
#include "types/TypedValue.hpp"
#include "types/VarCharType.hpp"
#include "types/YearMonthIntervalType.hpp"
#include "types/containers/ColumnVector.hpp"

#include "gtest/gtest.h"

namespace quickstep {

namespace {

  constexpr int kNumTuples = 100;
  constexpr int kNumTuplesPerPartition = 8;
  constexpr int kNullInterval = 25;

}  // namespace

// Attribute value could be null if set true.
class WindowAggregationHandleAvgTest : public::testing::TestWithParam<bool> {
 protected: 
  // Handle initialization.
  void initializeHandle(const Type &argument_type,
                        const std::vector<const Type*> &partition_key_types) {
    WindowAggregateFunction *function =
        WindowAggregateFactory::Get(WindowAggregationID::kAvg);
    handle_avg_.reset(function->createHandle(relation_,
                                             std::vector<const Type*>(1, &argument_type),
                                             partition_key_types));
  }

  // Test canApplyToTypes().
  static bool CanApplyToTypesTest(TypeID typeID) {
    const Type &type = (typeID == kChar || typeID == kVarChar) ?
        TypeFactory::GetType(typeID, static_cast<std::size_t>(10)) :
        TypeFactory::GetType(typeID);

    return WindowAggregateFunctionFactory::Get(WindowAggregationID::kAvg).canApplyToTypes(
        std::vector<const Type*>(1, &type));
  }

  // Test resultTypeForArgumentTypes().
  static bool ResultTypeForArgumentTypesTest(TypeID input_type_id,
                                            TypeID output_type_id) {
    const Type *result_type
        = WindowAggregateFunctionFactory::Get(WindowAggregationID::kAvg).resultTypeForArgumentTypes(
            std::vector<const Type*>(1, &TypeFactory::GetType(input_type_id)));
    return (result_type->getTypeID() == output_type_id);
  }

  template <typename CppType>
  static void CheckAvgValues(
      std::vector<CppType*> expected,
      const ColumnVector *actual) {
    EXPECT_TRUE(actual->isNative());
    NativeColumnVector *native = static_cast<const NativeColumnVector*>(actual);

    EXPECT_EQ(expected.size(), actual->size());
    for (std::size_t i = 0; i < expected.size(); ++i) {
      if (expected[i] == nullptr) {
        EXPECT_TRUE(actual->getTypedValue(i).isNull());
      } else {
        EXPECT_EQ(expected[i], actual->getTypedValue(i).getLiteral<CppType>());
      }
    }
  }

  // Static templated method for set a meaningful value to data types.
  template <typename CppType>
  static void SetDataType(int value, CppType *data) {
    *data = value;
  }

  template <typename GenericType, typename OutputType = DoubleType>
  void checkAggregationAvgGeneric() {
    const GenericType &type = GenericType::Instance(true);
    initializeHandle(type);
    EXPECT_EQ(0, aggregation_handle_avg_->finalize()->getNumTuplesVirtual());

    // Create argument, partition key and cpptype vectors.
    std::vector<GenericType::cpptype*> argument_cpp_vector;
    argument_cpp_vector.reserve(kNumTuples);
    ColumnVector *argument_type_vector =
        createArgumentGeneric<GenericType>(&argument_cpp_vector);
    const IntType &int_type = ;
    NativeColumnVector *partition_key_vector =
      new NativeColumnVector(IntType::InstanceNonNullable(), kNumTuples + 2);
        
    for (int i = 0; i < kNumTuples; ++i) {
      partition_key_vector->appendTypedValue(TypedValue(i / kNumTuplesPerPartition));
    }

    // Create tuple ValueAccessor
    ColumnVectorsValueAccessor *tuple_accessor = new ColumnVectorsValueAccessor();
    tuple_accessor->addColumn(partition_key_vector);
    tuple_accessor->addColumn(argument_type_vector);

    // Test UNBOUNDED PRECEDING AND CURRENT ROW.
    calculateAccumulative<GenericType, OutputType>(tuple_accessor,
                                                   argument_type_vector,
                                                   argument_cpp_vector);
  }

  template <typename GenericType>
  ColumnVector *createArgumentGeneric(
      std::vector<GenericType::cpptype*> *argument_cpp_vector) {
    const GenericType &type = GenericType::Instance(true);
    NativeColumnVector *column = new NativeColumnVector(type, kNumSamples);

    column->appendTypedValue(type.makeNullValue());
    for (int i = 0; i < kNumSamples; ++i) {
      // Insert a NULL every kNullInterval tuples.
      if (i % kNullInterval == 0) {
        argument_cpp_vector->push_back(nullptr);
        column->appendTypedValue(type.makeNullValue());
        continue;
      }

      typename GenericType::cpptype val = new GenericType::cpptype;
      
      if (type.getTypeID() == kInt || type.getTypeID() == kLong) {
        SetDataType(i - 10, val);
      } else {
        SetDataType(static_cast<float>(i - 10) / 10, val);
      }
      
      column->appendTypedValue(type.makeValue(val));
      argument_cpp_vector->push_back(val);
    }

    return column;
  }

  template <typename GenericType, typename OutputType>
  void calculateAccumulate(ValueAccessor *tuple_accessor,
                           ColumnVector *argument_type_vector,
                           const std::vector<GenericType::cpptype> &argument_cpp_vector) {
    std::vector<ColumnVector*> arguments;
    arguments.push_back(argument_type_vector);
    // The partition key index is 0.
    std::vector<attribute_id> partition_key(1, 0);
    
    ColumnVector *result =
        handle_avg_->calculate(tuple_accessor,
                               std::move(arguments),
                               partition_key,
                               true  /* is_row */,
                               -1  /* num_preceding: UNBOUNDED PRECEDING */,
                               0  /* num_following: CURRENT ROW */);
                           
    // Get the cpptype result.
    std::vector<OutputType::cpptype*> result_cpp_vector;
    bool is_null;
    typename GenericType::cpptype sum;
    int count;
    for (std::size_t i = 0; i < argument_cpp_vector.size(); ++i) {
      // Start of new partition
      if (i % kNumTuplesPerPartition == 0) {
        is_null = false;
        SetDataType(0, &sum);
        count = 0;
      }

      typename GenericType::cpptype *value = argument_cpp_vector[i];
      if (value == nullptr) {
        is_null = true;
      }

      if (is_null) {
        result_cpp_vector.push_back(nullptr);
      } else {
        sum += *value;
        count++;
        result_cpp_vector.push_back(static_cast<typename OutputType>(sum) / count);
      }
    }

    CheckAvgValues(result_cpp_vector, result);
  }

  std::unique_ptr<WindowAggregationHandle> handle_avg_;
};

}  // namespace quickstep
