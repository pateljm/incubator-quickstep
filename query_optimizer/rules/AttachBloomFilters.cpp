/**
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

#include "query_optimizer/rules/AttachBloomFilters.hpp"

#include <memory>
#include <set>
#include <unordered_set>
#include <unordered_map>
#include <vector>

#include "query_optimizer/cost_model/StarSchemaSimpleCostModel.hpp"
#include "query_optimizer/expressions/AttributeReference.hpp"
#include "query_optimizer/expressions/NamedExpression.hpp"
#include "query_optimizer/expressions/PatternMatcher.hpp"
#include "query_optimizer/physical/HashJoin.hpp"
#include "query_optimizer/physical/PatternMatcher.hpp"
#include "query_optimizer/physical/Physical.hpp"
#include "query_optimizer/physical/PhysicalType.hpp"
#include "query_optimizer/physical/TopLevelPlan.hpp"

#include "glog/logging.h"

namespace quickstep {
namespace optimizer {

namespace E = ::quickstep::optimizer::expressions;
namespace P = ::quickstep::optimizer::physical;

P::PhysicalPtr AttachBloomFilters::apply(const P::PhysicalPtr &input) {
  DCHECK(input->getPhysicalType() == P::PhysicalType::kTopLevelPlan);
  cost_model_.reset(
      new cost::StarSchemaSimpleCostModel(
          std::static_pointer_cast<const P::TopLevelPlan>(input)->shared_subplans()));

  visitProducer(input);
  visitConsumer(input);

  for (const auto &info_vec_pair : producers_) {
    std::cerr << "--------\n"
              << "Node " << info_vec_pair.first->getName()
              << info_vec_pair.first << "\n";
    for (const auto &info : info_vec_pair.second) {
      std::cerr << info.attribute->attribute_alias() << "@"
                << info.source
                << ": " << info.selectivity << "\n";
    }
    std::cerr << "********\n";
  }

  return input;
}

void AttachBloomFilters::visitProducer(const P::PhysicalPtr &node) {
  std::vector<const BloomFilterInfo> bloom_filters;

  // First check inherited bloom filters
  std::vector<const BloomFilterInfo*> candidates;
  switch (node->getPhysicalType()) {
    case P::PhysicalType::kAggregate:
    case P::PhysicalType::kSelection:
    case P::PhysicalType::kHashJoin: {
      for (const P::PhysicalPtr &child : node->children()) {
        const auto bloom_filters_it = producers_.find(child);
        if (bloom_filters_it != producers_.end()) {
          for (const BloomFilterInfo info : bloom_filters_it->second) {
            candidates.emplace_back(&info);
          }
        }
      }
    }
    default:
      break;
  }

  const std::vector<E::AttributeReferencePtr> output_attributes(
      node->getOutputAttributes());

  std::unordered_set<E::ExprId> output_attribute_ids;
  for (const auto &attr : output_attributes) {
    output_attribute_ids.emplace(attr->id());
  }
  for (const BloomFilterInfo *info : candidates) {
    if (output_attribute_ids.find(info->attribute->id()) != output_attribute_ids.end()) {
      bloom_filters.emplace_back(*info);
    }
  }

  // Self-produced bloom filters
  double selectivity = cost_model_->estimateSelectivity(node);
  if (selectivity < 1.0) {
    for (const auto &attr : output_attributes) {
      bloom_filters.emplace_back(node, attr, selectivity, false);
    }
  }

  producers_.emplace(node, std::move(bloom_filters));
}

void AttachBloomFilters::visitConsumer(const P::PhysicalPtr &node) {
}

}  // namespace optimizer
}  // namespace quickstep
