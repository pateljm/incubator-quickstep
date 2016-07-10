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
#include <utility>
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

  visitProducer(input, 0);
  visitConsumer(input);

  for (const auto &info_vec_pair : consumers_) {
    std::cerr << "--------\n"
              << "Node " << info_vec_pair.first->getName()
              << " " << info_vec_pair.first << "\n";

    for (const auto &info : info_vec_pair.second) {
      std::cerr << info.attribute->attribute_alias();
      if (info.attribute->id() != info.source_attribute->id()) {
        std::cerr << "{FROM " << info.source_attribute->attribute_alias() << "}";
      }
      if (info.from_sibling) {
        std::cerr << " sibling";
      }
      std::cerr << " @" << info.source << "[" << info.depth << "]"
                << ": " << info.selectivity << "\n";
    }
    std::cerr << "********\n";
  }

  return input;
}

void AttachBloomFilters::visitProducer(const P::PhysicalPtr &node, int depth) {
  for (const P::PhysicalPtr &child : node->children()) {
    visitProducer(child, depth+1);
  }

  std::vector<BloomFilterInfo> bloom_filters;

  const std::vector<E::AttributeReferencePtr> output_attributes(
      node->getOutputAttributes());
  std::unordered_set<E::ExprId> output_attribute_ids;
  for (const auto &attr : output_attributes) {
    output_attribute_ids.emplace(attr->id());
  }

  // First check inherited bloom filters
  std::vector<const BloomFilterInfo*> candidates;
  switch (node->getPhysicalType()) {
    case P::PhysicalType::kAggregate:
    case P::PhysicalType::kSelection:
    case P::PhysicalType::kHashJoin: {
      for (const P::PhysicalPtr &child : node->children()) {
        for (const BloomFilterInfo &info : producers_[child]) {
          candidates.emplace_back(&info);
        }
      }
    }
    default:
      break;
  }

  for (const BloomFilterInfo *info : candidates) {
    if (output_attribute_ids.find(info->attribute->id()) != output_attribute_ids.end()) {
      bloom_filters.emplace_back(
          info->source, info->attribute, info->depth, info->selectivity, false);
    }
  }

  // Self-produced bloom filters
  double selectivity = cost_model_->estimateSelectivity(node);
  if (selectivity < 1.0) {
    for (const auto &attr : output_attributes) {
      bloom_filters.emplace_back(node, attr, depth, selectivity, false);
    }
  }

  producers_.emplace(node, std::move(bloom_filters));
}

void AttachBloomFilters::visitConsumer(const P::PhysicalPtr &node) {
  std::vector<BloomFilterInfo> bloom_filters;

  // Bloom filters from parent
  const auto &parent_bloom_filters = consumers_[node];
  if (!parent_bloom_filters.empty()) {
    for (const auto &child : node->children()) {
      std::unordered_set<E::ExprId> child_output_attribute_ids;
      for (const auto &attr : child->getOutputAttributes()) {
        child_output_attribute_ids.emplace(attr->id());
      }

      std::vector<BloomFilterInfo> bloom_filters;
      for (const auto &info : parent_bloom_filters) {
        if (child_output_attribute_ids.find(info.attribute->id())
                != child_output_attribute_ids.end()) {
          bloom_filters.emplace_back(info.source,
                                     info.attribute,
                                     info.depth,
                                     info.selectivity,
                                     false,
                                     info.source_attribute);
        }
      }
      consumers_.emplace(child, std::move(bloom_filters));
    }
  }

  // Bloom filters from siblings via HashJoin
  if (node->getPhysicalType() == P::PhysicalType::kHashJoin) {
    const P::HashJoinPtr hash_join =
        std::static_pointer_cast<const P::HashJoin>(node);
    std::vector<P::PhysicalPtr> children =
        { hash_join->left(), hash_join->right() };

    std::unordered_map<E::ExprId, E::AttributeReferencePtr> join_attribute_pairs;
    for (std::size_t i = 0; i < hash_join->left_join_attributes().size(); ++i) {
      const E::AttributeReferencePtr left_join_attribute =
          hash_join->left_join_attributes()[i];
      const E::AttributeReferencePtr right_join_attribute =
          hash_join->right_join_attributes()[i];
      join_attribute_pairs.emplace(left_join_attribute->id(),
                                   right_join_attribute);
      join_attribute_pairs.emplace(right_join_attribute->id(),
                                   left_join_attribute);
    }

    for (int side = 0; side < 2; ++side) {
      const P::PhysicalPtr &producer_child = children[side];
      const P::PhysicalPtr &consumer_child = children[1-side];

      auto &consumer_bloom_filters = consumers_[consumer_child];
      for (const auto &info : producers_[producer_child]) {
        const auto pair_it = join_attribute_pairs.find(info.attribute->id());
        if (pair_it != join_attribute_pairs.end()) {
          consumer_bloom_filters.emplace_back(info.source,
                                              pair_it->second,
                                              info.depth,
                                              info.selectivity,
                                              true,
                                              info.attribute);
        }
      }
    }
  }

  for (const auto &child : node->children()) {
    visitConsumer(child);
  }
}

}  // namespace optimizer
}  // namespace quickstep
