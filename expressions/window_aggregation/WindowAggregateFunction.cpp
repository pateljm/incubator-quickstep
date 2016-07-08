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

#include "expressions/window_aggregation/WindowAggregateFunction.hpp"

#include <type_traits>

#include "expressions/window_aggregation/WindowAggregateFunction.pb.h"
#include "expressions/window_aggregation/WindowAggregationID.hpp"

#include "glog/logging.h"

namespace quickstep {

serialization::WindowAggregateFunction WindowAggregateFunction::getProto() const {
  serialization::WindowAggregateFunction proto;
  switch (win_agg_id_) {
    case WindowAggregationID::kAvg:
      proto.set_window_aggregation_id(serialization::WindowAggregateFunction::AVG);
      break;
    default: {
      LOG(FATAL) << "Unrecognized WindowAggregationID: "
                 << static_cast<std::underlying_type<WindowAggregationID>::type>(win_agg_id_);
    }
  }

  return proto;
}

}  // namespace quickstep
