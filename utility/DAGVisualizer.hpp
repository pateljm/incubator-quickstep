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

#ifndef QUICKSTEP_UTILITY_DAG_VISUALIZER_HPP_
#define QUICKSTEP_UTILITY_DAG_VISUALIZER_HPP_

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "utility/Macros.hpp"

namespace quickstep {

class QueryPlan;

/** \addtogroup Utility
 *  @{
 */

/**
 * @brief A visualizer that converts an execution plan DAG into a graph in
 *        DOT format. Note that DOT is a plain text graph description language.
 *
 * @note This utility tool can be further extended to be more generic.
 */
class DAGVisualizer {
 public:
  DAGVisualizer(const QueryPlan &plan)
      : plan_(plan) {}

  ~DAGVisualizer() {}

  std::string toDOT();

 private:
  static std::string PercentageToString(double percentage);

  /**
   * @brief Information of a graph node.
   */
  struct NodeInfo {
    std::size_t id;
    std::vector<std::string> labels;
    std::string color;
  };

  /**
   * @brief Information of a graph edge.
   */
  struct EdgeInfo {
    std::size_t src_node_id;
    std::size_t dst_node_id;
    std::vector<std::string> labels;
    bool is_pipeline_breaker;
  };

  const QueryPlan &plan_;

  std::vector<NodeInfo> nodes_;
  std::vector<EdgeInfo> edges_;

  DISALLOW_COPY_AND_ASSIGN(DAGVisualizer);
};

/** @} */

}  // namespace quickstep

#endif /* QUICKSTEP_UTILITY_DAG_VISUALIZER_HPP_ */
