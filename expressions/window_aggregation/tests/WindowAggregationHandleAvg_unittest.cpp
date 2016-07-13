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
#include "storage/StorageManager.hpp"
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

  constexpr int kNumTuplesPerBlock = 100;
  constexpr int kNumBlocks = 5;
  constexpr int kNumTuplesPerPartition = 8;

}  // namespace

// Attribute value could be null if set true.
class WindowAggregationHandleAvgTest : public::testing::TestWithParam<bool> {
 protected:
  virtual void SetUp() {
    // Initialize relation and storage manager.
    relation_.reset(new CatalogRelation(NULL, "TestRelation", kRelationId));
    storage_manager_.reset(new StorageManager("TestAvg"));

    // Add All kinds of TypedValues.
    CatalogAttribute *int_attr = new CatalogAttribute(relation_.get(),
                                                      "int_attr",
                                                      TypeFactory::GetType(kInt, GetParam()));

    relation_->addAttribute(int_attr);

    CatalogAttribute *float_attr = new CatalogAttribute(relation_.get(),
                                                        "float_attr",
                                                        TypeFactory::GetType(kFloat, GetParam()));
    relation_->addAttribute(float_attr);

    CatalogAttribute *long_attr = new CatalogAttribute(relation_.get(),
                                                       "long_attr",
                                                       TypeFactory::GetType(kLong, GetParam()));
    relation_->addAttribute(long_attr);

    CatalogAttribute *double_attr = new CatalogAttribute(relation_.get(),
                                                         "double_attr",
                                                         TypeFactory::GetType(kDouble, GetParam()));
    relation_->addAttribute(double_attr);

    CatalogAttribute *char_attr = new CatalogAttribute(relation_.get(),
                                                       "char_attr",
                                                       TypeFactory::GetType(kChar, 4, GetParam()));
    relation_->addAttribute(char_attr);

    CatalogAttribute *varchar_attr = new CatalogAttribute(relation_.get(),
                                                          "varchar_attr",
                                                          TypeFactory::GetType(kVarChar, 32, GetParam()));
    relation_->addAttribute(varchar_attr);
    
    // Records the 'base_value' of a tuple used in createSampleTuple.
    CatalogAttribute *partition_value = new CatalogAttribute(relation_.get(),
                                                             "partition_value",
                                                             TypeFactory::GetType(kInt, false));
    relation_->addAttribute(partition_value);

    StorageBlockLayout *layout = StorageBlockLayout::GenerateDefaultLayout(*relation_, true);

    // Initialize blocks.
    for (int i = 0; i < kNumBlocks; ++i) {
      block_id bid = storage_manager_->createBlock(relation_, layout);
      relation_->addBlock(bid);
      insertTuples(bid);
    }
  }

  // Insert kNumTuplesPerBlock tuples into the block.
  void insertTuples(block_id bid) {
    MutableBlockReference block = storage_manager_->getBlockMutable(bid, relation_);
    for (int i = 0; i < kNumTuplesPerBlock; ++i) {
      Tuple *tuple = createTuple(bid * kNumTuplesPerBlock + i);
      block->insertTuple(*tuple);
    }
  }

  Tuple* createTuple(int base_value) {
    std::vector<TypedValue> attrs;

    // int_attr.
    if (GetParam() && base_value % 10 == 0) {
      // Throw in a NULL integer for every ten values.
      attrs.emplace_back(kInt);
    } else {
      attrs.emplace_back(base_value);
    }

    // float_attr.
    if (GetParam() && base_value % 10 == 1) {
      attrs.emplace_back(kFloat);
    } else {
      attrs.emplace_back(static_cast<float>(0.4 * base_value));
    }

    // long_attr.
    if (GetParam() && base_value % 10 == 2) {
      attrs.emplace_back(kLong);
    } else {
      attrs.emplace_back(static_cast<std::int64_t>(base_value));
    }

    // double_attr.
    if (GetParam() && base_value % 10 == 3) {
      attrs.emplace_back(kDouble);
    } else {
      attrs.emplace_back(static_cast<double>(0.25 * base_value));
    }

    // char_attr
    if (GetParam() && base_value % 10 == 4) {
      attrs.emplace_back(CharType::InstanceNullable(4).makeNullValue());
    } else {
      std::ostringstream char_buffer;
      char_buffer << base_value;
      std::string string_literal(char_buffer.str());
      attrs.emplace_back(CharType::InstanceNonNullable(4).makeValue(
          string_literal.c_str(),
          string_literal.size() > 3 ? 4
                                    : string_literal.size() + 1));
      attrs.back().ensureNotReference();
    }

    // varchar_attr
    if (GetParam() && base_value % 10 == 5) {
      attrs.emplace_back(VarCharType::InstanceNullable(32).makeNullValue());
    } else {
      std::ostringstream char_buffer;
      char_buffer << "Here are some numbers: " << base_value;
      std::string string_literal(char_buffer.str());
      attrs.emplace_back(VarCharType::InstanceNonNullable(32).makeValue(
          string_literal.c_str(),
          string_literal.size() + 1));
      attrs.back().ensureNotReference();
    }

    // base_value
    attrs.emplace_back(base_value / kNumTuplesPerPartition);
    return new Tuple(std::move(attrs));
  }
  
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
      std::vector<CppType> expected,
      const ColumnVector *actual) {
    EXPECT_TRUE(actual->isNative());
    NativeColumnVector *native = static_cast<const NativeColumnVector*>(actual);

    EXPECT_EQ(expected.size(), actual->size());
    for (std::size_t i = 0; i < expected.size(); ++i) {
      EXPECT_EQ(expected[i], actual->getTypedValue(i).getLiteral<CppType>());
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
    EXPECT_TRUE(aggregation_handle_avg_->finalize(relation_, storage_manager_).empty());

    aggregation_handle_avg_->calculate(relation_.getBlocksSnapshot(),
                                       std::vector<GenericType
                                       

    std::vector<OutputType> result_vector;
    typename GenericType::cpptype val;
    typename GenericType::cpptype sum;
    SetDataType(0, &sum);

    for (int i = 0; i < kNumSamples; ++i) {
      if (type.getTypeID() == kInt || type.getTypeID() == kLong) {
        SetDataType(i - 10, &val);
      } else {
        SetDataType(static_cast<float>(i - 10)/10, &val);
      }
      iterateHandle(aggregation_handle_avg_state_.get(), type.makeValue(&val));
      sum += val;
    }
    iterateHandle(aggregation_handle_avg_state_.get(), type.makeNullValue());
    CheckAvgValue<typename OutputType::cpptype>(static_cast<typename OutputType::cpptype>(sum) / kNumSamples,
                                                *aggregation_handle_avg_,
                                                *aggregation_handle_avg_state_);
  }

  template <typename GenericType>
  ColumnVector *createColumnVectorGeneric(const Type &type, typename GenericType::cpptype *sum) {
    NativeColumnVector *column = new NativeColumnVector(type, kNumSamples + 3);

    typename GenericType::cpptype val;
    SetDataType(0, sum);

    column->appendTypedValue(type.makeNullValue());
    for (int i = 0; i < kNumSamples; ++i) {
      if (type.getTypeID() == kInt || type.getTypeID() == kLong) {
        SetDataType(i - 10, &val);
      } else {
        SetDataType(static_cast<float>(i - 10)/10, &val);
      }
      column->appendTypedValue(type.makeValue(&val));
      *sum += val;
      // One NULL in the middle.
      if (i == kNumSamples/2) {
        column->appendTypedValue(type.makeNullValue());
      }
    }
    column->appendTypedValue(type.makeNullValue());

    return column;
  }

  template <typename GenericType, typename OutputType = DoubleType>
  void checkAggregationAvgGenericColumnVector() {
    const GenericType &type = GenericType::Instance(true);
    initializeHandle(type);
    EXPECT_TRUE(aggregation_handle_avg_->finalize(*aggregation_handle_avg_state_).isNull());

    typename GenericType::cpptype sum;
    SetDataType(0, &sum);
    std::vector<std::unique_ptr<ColumnVector>> column_vectors;
    column_vectors.emplace_back(createColumnVectorGeneric<GenericType>(type, &sum));

    std::unique_ptr<AggregationState> cv_state(
        aggregation_handle_avg_->accumulateColumnVectors(column_vectors));

    // Test the state generated directly by accumulateColumnVectors(), and also
    // test after merging back.
    CheckAvgValue<typename OutputType::cpptype>(
        static_cast<typename OutputType::cpptype>(sum) / kNumSamples,
        *aggregation_handle_avg_,
        *cv_state);

    aggregation_handle_avg_->mergeStates(*cv_state, aggregation_handle_avg_state_.get());
    CheckAvgValue<typename OutputType::cpptype>(
        static_cast<typename OutputType::cpptype>(sum) / kNumSamples,
        *aggregation_handle_avg_,
        *aggregation_handle_avg_state_);
  }

#ifdef QUICKSTEP_ENABLE_VECTOR_COPY_ELISION_SELECTION
  template <typename GenericType, typename OutputType = DoubleType>
  void checkAggregationAvgGenericValueAccessor() {
    const GenericType &type = GenericType::Instance(true);
    initializeHandle(type);
    EXPECT_TRUE(aggregation_handle_avg_->finalize(*aggregation_handle_avg_state_).isNull());

    typename GenericType::cpptype sum;
    SetDataType(0, &sum);
    std::unique_ptr<ColumnVectorsValueAccessor> accessor(new ColumnVectorsValueAccessor());
    accessor->addColumn(createColumnVectorGeneric<GenericType>(type, &sum));

    std::unique_ptr<AggregationState> va_state(
        aggregation_handle_avg_->accumulateValueAccessor(accessor.get(),
                                                         std::vector<attribute_id>(1, 0)));

    // Test the state generated directly by accumulateValueAccessor(), and also
    // test after merging back.
    CheckAvgValue<typename OutputType::cpptype>(
        static_cast<typename OutputType::cpptype>(sum) / kNumSamples,
        *aggregation_handle_avg_,
        *va_state);

    aggregation_handle_avg_->mergeStates(*va_state, aggregation_handle_avg_state_.get());
    CheckAvgValue<typename OutputType::cpptype>(
        static_cast<typename OutputType::cpptype>(sum) / kNumSamples,
        *aggregation_handle_avg_,
        *aggregation_handle_avg_state_);
  }
#endif  // QUICKSTEP_ENABLE_VECTOR_COPY_ELISION_SELECTION

  std::unique_ptr<AggregationHandle> aggregation_handle_avg_;
  std::unique_ptr<AggregationState> aggregation_handle_avg_state_;
  std::unique_ptr<StorageManager> storage_manager_;
  std::unique_ptr<CatalogRelation> relation_;
};

const int AggregationHandleAvgTest::kNumSamples;

template <>
void AggregationHandleAvgTest::CheckAvgValue<double>(
    double expected,
    const AggregationHandle &handle,
    const AggregationState &state) {
  EXPECT_DOUBLE_EQ(expected, handle.finalize(state).getLiteral<double>());
}

template <>
void AggregationHandleAvgTest::SetDataType<DatetimeIntervalLit>(int value, DatetimeIntervalLit *data) {
  data->interval_ticks = value;
}

template <>
void AggregationHandleAvgTest::SetDataType<YearMonthIntervalLit>(int value, YearMonthIntervalLit *data) {
  data->months = value;
}

typedef AggregationHandleAvgTest AggregationHandleAvgDeathTest;

TEST_F(AggregationHandleAvgTest, IntTypeTest) {
  checkAggregationAvgGeneric<IntType>();
}

TEST_F(AggregationHandleAvgTest, LongTypeTest) {
  checkAggregationAvgGeneric<LongType>();
}

TEST_F(AggregationHandleAvgTest, FloatTypeTest) {
  checkAggregationAvgGeneric<FloatType>();
}

TEST_F(AggregationHandleAvgTest, DoubleTypeTest) {
  checkAggregationAvgGeneric<DoubleType>();
}

TEST_F(AggregationHandleAvgTest, DatetimeIntervalTypeTest) {
  checkAggregationAvgGeneric<DatetimeIntervalType, DatetimeIntervalType>();
}

TEST_F(AggregationHandleAvgTest, YearMonthIntervalTypeTest) {
  checkAggregationAvgGeneric<YearMonthIntervalType, YearMonthIntervalType>();
}

TEST_F(AggregationHandleAvgTest, IntTypeColumnVectorTest) {
  checkAggregationAvgGenericColumnVector<IntType>();
}

TEST_F(AggregationHandleAvgTest, LongTypeColumnVectorTest) {
  checkAggregationAvgGenericColumnVector<LongType>();
}

TEST_F(AggregationHandleAvgTest, FloatTypeColumnVectorTest) {
  checkAggregationAvgGenericColumnVector<FloatType>();
}

TEST_F(AggregationHandleAvgTest, DoubleTypeColumnVectorTest) {
  checkAggregationAvgGenericColumnVector<DoubleType>();
}

TEST_F(AggregationHandleAvgTest, DatetimeIntervalTypeColumnVectorTest) {
  checkAggregationAvgGenericColumnVector<DatetimeIntervalType, DatetimeIntervalType>();
}

TEST_F(AggregationHandleAvgTest, YearMonthIntervalTypeColumnVectorTest) {
  checkAggregationAvgGenericColumnVector<YearMonthIntervalType, YearMonthIntervalType>();
}

#ifdef QUICKSTEP_ENABLE_VECTOR_COPY_ELISION_SELECTION
TEST_F(AggregationHandleAvgTest, IntTypeValueAccessorTest) {
  checkAggregationAvgGenericValueAccessor<IntType>();
}

TEST_F(AggregationHandleAvgTest, LongTypeValueAccessorTest) {
  checkAggregationAvgGenericValueAccessor<LongType>();
}

TEST_F(AggregationHandleAvgTest, FloatTypeValueAccessorTest) {
  checkAggregationAvgGenericValueAccessor<FloatType>();
}

TEST_F(AggregationHandleAvgTest, DoubleTypeValueAccessorTest) {
  checkAggregationAvgGenericValueAccessor<DoubleType>();
}

TEST_F(AggregationHandleAvgTest, DatetimeIntervalTypeValueAccessorTest) {
  checkAggregationAvgGenericValueAccessor<DatetimeIntervalType, DatetimeIntervalType>();
}

TEST_F(AggregationHandleAvgTest, YearMonthIntervalTypeValueAccessorTest) {
  checkAggregationAvgGenericValueAccessor<YearMonthIntervalType, YearMonthIntervalType>();
}
#endif  // QUICKSTEP_ENABLE_VECTOR_COPY_ELISION_SELECTION

#ifdef QUICKSTEP_DEBUG
TEST_F(AggregationHandleAvgDeathTest, CharTypeTest) {
  const Type &type = CharType::Instance(true, 10);
  EXPECT_DEATH(initializeHandle(type), "");
}

TEST_F(AggregationHandleAvgDeathTest, VarTypeTest) {
  const Type &type = VarCharType::Instance(true, 10);
  EXPECT_DEATH(initializeHandle(type), "");
}

TEST_F(AggregationHandleAvgDeathTest, WrongTypeTest) {
  const Type &int_non_null_type = IntType::Instance(false);
  const Type &long_type = LongType::Instance(true);
  const Type &double_type = DoubleType::Instance(true);
  const Type &float_type = FloatType::Instance(true);
  const Type &char_type = CharType::Instance(true, 10);
  const Type &varchar_type = VarCharType::Instance(true, 10);

  initializeHandle(IntType::Instance(true));
  int int_val = 0;
  std::int64_t long_val = 0;
  double double_val = 0;
  float float_val = 0;

  iterateHandle(aggregation_handle_avg_state_.get(), int_non_null_type.makeValue(&int_val));

  EXPECT_DEATH(iterateHandle(aggregation_handle_avg_state_.get(), long_type.makeValue(&long_val)), "");
  EXPECT_DEATH(iterateHandle(aggregation_handle_avg_state_.get(), double_type.makeValue(&double_val)), "");
  EXPECT_DEATH(iterateHandle(aggregation_handle_avg_state_.get(), float_type.makeValue(&float_val)), "");
  EXPECT_DEATH(iterateHandle(aggregation_handle_avg_state_.get(), char_type.makeValue("asdf", 5)), "");
  EXPECT_DEATH(iterateHandle(aggregation_handle_avg_state_.get(), varchar_type.makeValue("asdf", 5)), "");

  // Test mergeStates() with incorrectly typed handles.
  std::unique_ptr<AggregationHandle> aggregation_handle_avg_double(
      AggregateFunctionFactory::Get(AggregationID::kAvg).createHandle(
          std::vector<const Type*>(1, &double_type)));
  std::unique_ptr<AggregationState> aggregation_state_avg_merge_double(
      aggregation_handle_avg_double->createInitialState());
  static_cast<const AggregationHandleAvg&>(*aggregation_handle_avg_double).iterateUnaryInl(
      static_cast<AggregationStateAvg*>(aggregation_state_avg_merge_double.get()),
      double_type.makeValue(&double_val));
  EXPECT_DEATH(aggregation_handle_avg_->mergeStates(*aggregation_state_avg_merge_double,
                                                    aggregation_handle_avg_state_.get()),
               "");

  std::unique_ptr<AggregationHandle> aggregation_handle_avg_float(
      AggregateFunctionFactory::Get(AggregationID::kAvg).createHandle(
          std::vector<const Type*>(1, &float_type)));
  std::unique_ptr<AggregationState> aggregation_state_avg_merge_float(
      aggregation_handle_avg_float->createInitialState());
  static_cast<const AggregationHandleAvg&>(*aggregation_handle_avg_float).iterateUnaryInl(
      static_cast<AggregationStateAvg*>(aggregation_state_avg_merge_float.get()),
      float_type.makeValue(&float_val));
  EXPECT_DEATH(aggregation_handle_avg_->mergeStates(*aggregation_state_avg_merge_float,
                                                    aggregation_handle_avg_state_.get()),
               "");
}
#endif

TEST_F(AggregationHandleAvgTest, canApplyToTypeTest) {
  EXPECT_TRUE(ApplyToTypesTest(kInt));
  EXPECT_TRUE(ApplyToTypesTest(kLong));
  EXPECT_TRUE(ApplyToTypesTest(kFloat));
  EXPECT_TRUE(ApplyToTypesTest(kDouble));
  EXPECT_FALSE(ApplyToTypesTest(kChar));
  EXPECT_FALSE(ApplyToTypesTest(kVarChar));
  EXPECT_FALSE(ApplyToTypesTest(kDatetime));
  EXPECT_TRUE(ApplyToTypesTest(kDatetimeInterval));
  EXPECT_TRUE(ApplyToTypesTest(kYearMonthInterval));
}

TEST_F(AggregationHandleAvgTest, ResultTypeForArgumentTypeTest) {
  EXPECT_TRUE(ResultTypeForArgumentTypeTest(kInt, kDouble));
  EXPECT_TRUE(ResultTypeForArgumentTypeTest(kLong, kDouble));
  EXPECT_TRUE(ResultTypeForArgumentTypeTest(kFloat, kDouble));
  EXPECT_TRUE(ResultTypeForArgumentTypeTest(kDouble, kDouble));
  EXPECT_TRUE(ResultTypeForArgumentTypeTest(kDatetimeInterval, kDatetimeInterval));
  EXPECT_TRUE(ResultTypeForArgumentTypeTest(kYearMonthInterval, kYearMonthInterval));
}

TEST_F(AggregationHandleAvgTest, GroupByTableMergeTestAvg) {
  const Type &long_non_null_type = LongType::Instance(false);
  initializeHandle(long_non_null_type);
  storage_manager_.reset(new StorageManager("./test_avg_data"));
  std::unique_ptr<AggregationStateHashTableBase> source_hash_table(
      aggregation_handle_avg_->createGroupByHashTable(
          HashTableImplType::kSimpleScalarSeparateChaining,
          std::vector<const Type *>(1, &long_non_null_type),
          10,
          storage_manager_.get()));
  std::unique_ptr<AggregationStateHashTableBase> destination_hash_table(
      aggregation_handle_avg_->createGroupByHashTable(
          HashTableImplType::kSimpleScalarSeparateChaining,
          std::vector<const Type *>(1, &long_non_null_type),
          10,
          storage_manager_.get()));

  AggregationStateHashTable<AggregationStateAvg> *destination_hash_table_derived =
      static_cast<AggregationStateHashTable<AggregationStateAvg> *>(
          destination_hash_table.get());

  AggregationStateHashTable<AggregationStateAvg> *source_hash_table_derived =
      static_cast<AggregationStateHashTable<AggregationStateAvg> *>(
          source_hash_table.get());

  AggregationHandleAvg *aggregation_handle_avg_derived =
      static_cast<AggregationHandleAvg *>(aggregation_handle_avg_.get());
  // We create three keys: first is present in both the hash tables, second key
  // is present only in the source hash table while the third key is present
  // the destination hash table only.
  std::vector<TypedValue> common_key;
  common_key.emplace_back(static_cast<std::int64_t>(0));
  std::vector<TypedValue> exclusive_source_key, exclusive_destination_key;
  exclusive_source_key.emplace_back(static_cast<std::int64_t>(1));
  exclusive_destination_key.emplace_back(static_cast<std::int64_t>(2));

  const std::int64_t common_key_source_avg = 355;
  TypedValue common_key_source_avg_val(common_key_source_avg);

  const std::int64_t common_key_destination_avg = 295;
  TypedValue common_key_destination_avg_val(common_key_destination_avg);

  const std::int64_t exclusive_key_source_avg = 1;
  TypedValue exclusive_key_source_avg_val(exclusive_key_source_avg);

  const std::int64_t exclusive_key_destination_avg = 1;
  TypedValue exclusive_key_destination_avg_val(exclusive_key_destination_avg);

  std::unique_ptr<AggregationStateAvg> common_key_source_state(
      static_cast<AggregationStateAvg *>(
          aggregation_handle_avg_->createInitialState()));
  std::unique_ptr<AggregationStateAvg> common_key_destination_state(
      static_cast<AggregationStateAvg *>(
          aggregation_handle_avg_->createInitialState()));
  std::unique_ptr<AggregationStateAvg> exclusive_key_source_state(
      static_cast<AggregationStateAvg *>(
          aggregation_handle_avg_->createInitialState()));
  std::unique_ptr<AggregationStateAvg> exclusive_key_destination_state(
      static_cast<AggregationStateAvg *>(
          aggregation_handle_avg_->createInitialState()));

  // Create avg value states for keys.
  aggregation_handle_avg_derived->iterateUnaryInl(common_key_source_state.get(),
                                                  common_key_source_avg_val);

  aggregation_handle_avg_derived->iterateUnaryInl(
      common_key_destination_state.get(), common_key_destination_avg_val);

  aggregation_handle_avg_derived->iterateUnaryInl(
      exclusive_key_destination_state.get(), exclusive_key_destination_avg_val);

  aggregation_handle_avg_derived->iterateUnaryInl(
      exclusive_key_source_state.get(), exclusive_key_source_avg_val);

  // Add the key-state pairs to the hash tables.
  source_hash_table_derived->putCompositeKey(common_key,
                                             *common_key_source_state);
  destination_hash_table_derived->putCompositeKey(
      common_key, *common_key_destination_state);
  source_hash_table_derived->putCompositeKey(exclusive_source_key,
                                             *exclusive_key_source_state);
  destination_hash_table_derived->putCompositeKey(
      exclusive_destination_key, *exclusive_key_destination_state);

  EXPECT_EQ(2u, destination_hash_table_derived->numEntries());
  EXPECT_EQ(2u, source_hash_table_derived->numEntries());

  aggregation_handle_avg_->mergeGroupByHashTables(*source_hash_table,
                                                  destination_hash_table.get());

  EXPECT_EQ(3u, destination_hash_table_derived->numEntries());

  CheckAvgValue<double>(
      (common_key_destination_avg_val.getLiteral<std::int64_t>() +
          common_key_source_avg_val.getLiteral<std::int64_t>()) / static_cast<double>(2),
      *aggregation_handle_avg_derived,
      *(destination_hash_table_derived->getSingleCompositeKey(common_key)));
  CheckAvgValue<double>(exclusive_key_destination_avg_val.getLiteral<std::int64_t>(),
                  *aggregation_handle_avg_derived,
                  *(destination_hash_table_derived->getSingleCompositeKey(
                      exclusive_destination_key)));
  CheckAvgValue<double>(exclusive_key_source_avg_val.getLiteral<std::int64_t>(),
                  *aggregation_handle_avg_derived,
                  *(source_hash_table_derived->getSingleCompositeKey(
                      exclusive_source_key)));
}

}  // namespace quickstep
