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

#include "utility/DAGVisualizer.hpp"
#include "utility/EventProfiler.hpp"

#include <cmath>
#include <cstddef>
#include <iomanip>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "query_optimizer/QueryPlan.hpp"
#include "utility/EventProfiler.hpp"
#include "utility/StringUtil.hpp"

#include "glog/logging.h"

namespace quickstep {

std::string DAGVisualizer::toDOT() {
  std::set<std::string> no_display_op_names =
      { "DestroyHashOperator", "DropTableOperator" };

  const auto &dag = plan_.getQueryPlanDAG();
  const std::size_t num_nodes = dag.size();

  std::vector<double> time_elapsed(num_nodes, 0);
  std::vector<double> time_percentage(num_nodes, 0);
  std::vector<double> time_start(num_nodes, std::numeric_limits<double>::max());
  std::vector<double> time_end(num_nodes, 0);
  const auto &zero_time = relop_profiler.zero_time();
  for (const auto &container : relop_profiler.containers()) {
    for (const auto &line : container.second.events) {
      const std::size_t relop_index = line.first;
      for (const auto &event : line.second) {
        time_elapsed[relop_index] +=
            std::chrono::duration<double>(event.end_time - event.start_time).count();
        time_start[relop_index] =
            std::min(time_start[relop_index],
                     std::chrono::duration<double>(event.start_time - zero_time).count());
        time_end[relop_index] =
            std::max(time_end[relop_index],
                     std::chrono::duration<double>(event.end_time - zero_time).count());
      }
    }
  }
  const std::size_t num_threads = relop_profiler.containers().size();
  double total_time_elapsed = 0;
  double max_percentage = 0;
  for (std::size_t i = 0; i < time_elapsed.size(); ++i) {
    time_elapsed[i] /= num_threads;
    total_time_elapsed += time_elapsed[i];
  }
  for (std::size_t i = 0; i < time_elapsed.size(); ++i) {
    time_percentage[i] = time_elapsed[i] / total_time_elapsed;
    max_percentage = std::max(max_percentage, time_percentage[i]);
  }

  std::vector<bool> display_ops(num_nodes, false);
  for (std::size_t node_index = 0; node_index < num_nodes; ++node_index) {
    const auto &node = dag.getNodePayload(node_index);
    const std::string relop_name = node.getName();
    if (no_display_op_names.find(relop_name) == no_display_op_names.end()) {
      display_ops[node_index] = true;

      nodes_.emplace_back();
      NodeInfo &node_info = nodes_.back();
      node_info.id = node_index;

      std::string hue =
          std::to_string(std::sqrt(time_percentage[node_index] / max_percentage));
      node_info.color = hue + " " + hue + " 1.0";

      node_info.labels.emplace_back(
          "[" + std::to_string(node.getOperatorIndex()) + "] " + relop_name);
      node_info.labels.emplace_back(
          std::to_string(std::lround(time_elapsed[node_index] * 1000)) +
          "ms (" + PercentageToString(time_percentage[node_index] * 100) + "%)");
      node_info.labels.emplace_back(
          "span: [" +
          std::to_string(std::lround(time_start[node_index] * 1000)) + "ms, " +
          std::to_string(std::lround(time_end[node_index] * 1000)) + "ms]");
    }
  }
  for (std::size_t node_index = 0; node_index < num_nodes; ++node_index) {
    if (display_ops[node_index]) {
      for (const auto &link : dag.getDependents(node_index)) {
        if (display_ops[link.first]) {
          edges_.emplace_back();
          EdgeInfo &edge_info = edges_.back();
          edge_info.src_node_id = node_index;
          edge_info.dst_node_id = link.first;
          edge_info.is_pipeline_breaker = link.second;
        }
      }
    }
  }

  // Format output graph
  std::ostringstream graph_oss;
  graph_oss << "digraph g {\n";
  graph_oss << "  rankdir=BT\n";
  graph_oss << "  node [penwidth=2]\n";
  graph_oss << "  edge [fontsize=16 fontcolor=gray penwidth=2]\n\n";

  // Format nodes
  for (const NodeInfo &node_info : nodes_) {
    graph_oss << "  " << node_info.id << " [ ";
    if (!node_info.labels.empty()) {
      graph_oss << "label=\""
                << EscapeSpecialChars(JoinToString(node_info.labels, "&#10;"))
                << "\" ";
    }
    if (!node_info.color.empty()) {
      graph_oss << "style=filled fillcolor=\"" << node_info.color << "\" ";
    }
    graph_oss << "]\n";
  }
  graph_oss << "\n";

  // Format edges
  for (const EdgeInfo &edge_info : edges_) {
    graph_oss << "  " << edge_info.src_node_id << " -> "
              << edge_info.dst_node_id << " [ ";
    if (edge_info.is_pipeline_breaker) {
      graph_oss << "style=dashed ";
    }
    if (!edge_info.labels.empty()) {
      graph_oss << "label=\""
                << EscapeSpecialChars(JoinToString(edge_info.labels, "&#10;"))
                << "\" ";
    }
    graph_oss << "]\n";
  }

  graph_oss << "}\n";

  return graph_oss.str();
}

std::string DAGVisualizer::PercentageToString(double percentage) {
  std::ostringstream oss;
  oss << static_cast<std::uint32_t>(percentage) << ".";
  int digits = std::lround(percentage * 10000) % 100;
  oss << digits / 10 << digits % 10;
  return oss.str();
}

}  // namespace quickstep
