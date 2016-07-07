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

#ifndef QUICKSTEP_QUERY_OPTIMIZER_RULES_ATTACH_BLOOM_FILTERS_HPP_
#define QUICKSTEP_QUERY_OPTIMIZER_RULES_ATTACH_BLOOM_FILTERS_HPP_

#include <algorithm>
#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "query_optimizer/cost_model/StarSchemaSimpleCostModel.hpp"
#include "query_optimizer/expressions/ExprId.hpp"
#include "query_optimizer/expressions/NamedExpression.hpp"
#include "query_optimizer/expressions/Predicate.hpp"
#include "query_optimizer/physical/Physical.hpp"
#include "query_optimizer/rules/Rule.hpp"
#include "utility/Macros.hpp"

namespace quickstep {
namespace optimizer {

/** \addtogroup OptimizerRules
 *  @{
 */

/**
 * @brief TODO
 */
class AttachBloomFilters : public Rule<physical::Physical> {
 public:
  AttachBloomFilters() {}

  ~AttachBloomFilters() override {}

  std::string getName() const override {
    return "AttachBloomFilters";
  }

  physical::PhysicalPtr apply(const physical::PhysicalPtr &input) override;

 private:
  struct BloomFilterInfo {
    BloomFilterInfo(const physical::PhysicalPtr &source_in,
                    const expressions::AttributeReferencePtr &attribute_in,
                    const double selectivity_in,
                    const bool from_sibling_in)
        : source(source_in),
          attribute(attribute_in),
          selectivity(selectivity_in),
          from_sibling(from_sibling_in) {
    }
    BloomFilterInfo(const BloomFilterInfo &info)
        : source(info.source),
          attribute(info.attribute),
          selectivity(info.selectivity),
          from_sibling(info.from_sibling) {
    }
    physical::PhysicalPtr source;
    expressions::AttributeReferencePtr attribute;
    double selectivity;
    bool from_sibling;
  };

  void visitProducer(const physical::PhysicalPtr &node);

  void visitConsumer(const physical::PhysicalPtr &node);

  std::unique_ptr<cost::StarSchemaSimpleCostModel> cost_model_;

  std::map<physical::PhysicalPtr, std::vector<const BloomFilterInfo>> producers_;
  std::map<physical::PhysicalPtr, std::vector<const BloomFilterInfo>> consumers_;

  DISALLOW_COPY_AND_ASSIGN(AttachBloomFilters);
};

/** @} */

}  // namespace optimizer
}  // namespace quickstep

#endif /* QUICKSTEP_QUERY_OPTIMIZER_RULES_ATTACH_BLOOM_FILTERS_HPP_ */
