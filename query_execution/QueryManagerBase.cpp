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

#include "query_execution/QueryManagerBase.hpp"

#include <memory>
#include <utility>
#include <vector>

#include "catalog/CatalogDatabase.hpp"
#include "catalog/CatalogRelation.hpp"
#include "catalog/CatalogTypedefs.hpp"
#include "catalog/PartitionScheme.hpp"
#include "query_execution/QueryContext.hpp"
#include "query_execution/QueryExecutionMessages.pb.h"
#include "query_execution/QueryExecutionTypedefs.hpp"
#include "query_optimizer/QueryHandle.hpp"
#include "query_optimizer/QueryPlan.hpp"
#include "relational_operators/WorkOrder.hpp"
#include "storage/StorageBlockInfo.hpp"

#include "glog/logging.h"

using std::pair;

namespace quickstep {

QueryManagerBase::QueryManagerBase(QueryHandle *query_handle,
                                   CatalogDatabaseLite *catalog_database)
    : query_id_(DCHECK_NOTNULL(query_handle)->query_id()),
      catalog_database_(DCHECK_NOTNULL(catalog_database)),
      query_dag_(DCHECK_NOTNULL(
          DCHECK_NOTNULL(query_handle->getQueryPlanMutable())->getQueryPlanDAGMutable())),
      num_operators_in_dag_(query_dag_->size()),
      output_consumers_(num_operators_in_dag_),
      blocking_dependencies_(num_operators_in_dag_),
      query_exec_state_(new QueryExecutionState(num_operators_in_dag_)) {
  for (dag_node_index node_index = 0;
       node_index < num_operators_in_dag_;
       ++node_index) {
    const QueryContext::insert_destination_id insert_destination_index =
        query_dag_->getNodePayload(node_index).getInsertDestinationID();
    if (insert_destination_index != QueryContext::kInvalidInsertDestinationId) {
      // Rebuild is necessary whenever InsertDestination is present.
      query_exec_state_->setRebuildRequired(node_index);
      query_exec_state_->setRebuildStatus(node_index, 0, false);
    }

    for (const pair<dag_node_index, bool> &dependent_link :
         query_dag_->getDependents(node_index)) {
      const dag_node_index dependent_op_index = dependent_link.first;
      if (!query_dag_->getLinkMetadata(node_index, dependent_op_index)) {
        // The link is not a pipeline-breaker. Streaming of blocks is possible
        // between these two operators.
        output_consumers_[node_index].push_back(dependent_op_index);
      } else {
        // The link is a pipeline-breaker. Streaming of blocks is not possible
        // between these two operators.
        blocking_dependencies_[dependent_op_index].push_back(node_index);
      }
    }
  }
}

QueryManagerBase::QueryStatusCode QueryManagerBase::processMessage(
    const TaggedMessage &tagged_message) {
  dag_node_index op_index;
  switch (tagged_message.message_type()) {
    case kWorkOrderCompleteMessage: {
      serialization::NormalWorkOrderCompletionMessage proto;
      CHECK(proto.ParseFromArray(tagged_message.message(),
                                 tagged_message.message_bytes()));

      op_index = proto.operator_index();
      processWorkOrderCompleteMessage(proto.operator_index());
      break;
    }
    case kRebuildWorkOrderCompleteMessage: {
      serialization::RebuildWorkOrderCompletionMessage proto;
      CHECK(proto.ParseFromArray(tagged_message.message(),
                                 tagged_message.message_bytes()));

      op_index = proto.operator_index();
      processRebuildWorkOrderCompleteMessage(proto.operator_index());
      break;
    }
    case kCatalogRelationNewBlockMessage: {
      serialization::CatalogRelationNewBlockMessage proto;
      CHECK(proto.ParseFromArray(tagged_message.message(),
                                 tagged_message.message_bytes()));

      const block_id block = proto.block_id();

      CatalogRelation *relation =
          static_cast<CatalogDatabase*>(catalog_database_)->getRelationByIdMutable(proto.relation_id());
      relation->addBlock(block);

      if (proto.has_partition_id()) {
        relation->getPartitionSchemeMutable()->addBlockToPartition(
            proto.partition_id(), block);
      }
      return QueryStatusCode::kNone;
    }
    case kDataPipelineMessage: {
      // Possible message senders include InsertDestinations and some
      // operators which modify existing blocks.
      serialization::DataPipelineMessage proto;
      CHECK(proto.ParseFromArray(tagged_message.message(),
                                 tagged_message.message_bytes()));

      op_index = proto.operator_index();
      processDataPipelineMessage(proto.operator_index(),
                                 proto.block_id(),
                                 proto.relation_id());
      break;
    }
    case kWorkOrdersAvailableMessage: {
      serialization::WorkOrdersAvailableMessage proto;
      CHECK(proto.ParseFromArray(tagged_message.message(),
                                 tagged_message.message_bytes()));

      op_index = proto.operator_index();

      // Check if new work orders are available.
      fetchNormalWorkOrders(op_index);
      break;
    }
    case kWorkOrderFeedbackMessage: {
      WorkOrder::FeedbackMessage msg(
          const_cast<void *>(tagged_message.message()),
          tagged_message.message_bytes());

      op_index = msg.header().rel_op_index;
      processFeedbackMessage(msg);
      break;
    }
    default:
      LOG(FATAL) << "Unknown message type found in QueryManager";
  }

  if (query_exec_state_->hasExecutionFinished(op_index)) {
    return QueryStatusCode::kOperatorExecuted;
  }

  // As kQueryExecuted takes precedence over kOperatorExecuted, we check again.
  if (query_exec_state_->hasQueryExecutionFinished()) {
    return QueryStatusCode::kQueryExecuted;
  }

  return QueryStatusCode::kNone;
}

void QueryManagerBase::processFeedbackMessage(
    const WorkOrder::FeedbackMessage &msg) {
  RelationalOperator *op =
      query_dag_->getNodePayloadMutable(msg.header().rel_op_index);
  op->receiveFeedbackMessage(msg);
}

void QueryManagerBase::processWorkOrderCompleteMessage(
    const dag_node_index op_index) {
  query_exec_state_->decrementNumQueuedWorkOrders(op_index);

  // Check if new work orders are available and fetch them if so.
  fetchNormalWorkOrders(op_index);

  if (checkRebuildRequired(op_index)) {
    if (checkNormalExecutionOver(op_index)) {
      if (!checkRebuildInitiated(op_index)) {
        if (initiateRebuild(op_index)) {
          // Rebuild initiated and completed right away.
          markOperatorFinished(op_index);
        } else {
          // Rebuild under progress.
        }
      } else if (checkRebuildOver(op_index)) {
        // Rebuild was under progress and now it is over.
        markOperatorFinished(op_index);
      }
    } else {
      // Normal execution under progress for this operator.
    }
  } else if (checkOperatorExecutionOver(op_index)) {
    // Rebuild not required for this operator and its normal execution is
    // complete.
    markOperatorFinished(op_index);
  }

  for (const pair<dag_node_index, bool> &dependent_link :
       query_dag_->getDependents(op_index)) {
    const dag_node_index dependent_op_index = dependent_link.first;
    if (checkAllBlockingDependenciesMet(dependent_op_index)) {
      // Process the dependent operator (of the operator whose WorkOrder
      // was just executed) for which all the dependencies have been met.
      processOperator(dependent_op_index, true);
    }
  }
}

void QueryManagerBase::processRebuildWorkOrderCompleteMessage(const dag_node_index op_index) {
  query_exec_state_->decrementNumRebuildWorkOrders(op_index);

  if (checkRebuildOver(op_index)) {
    markOperatorFinished(op_index);

    for (const pair<dag_node_index, bool> &dependent_link :
         query_dag_->getDependents(op_index)) {
      const dag_node_index dependent_op_index = dependent_link.first;
      if (checkAllBlockingDependenciesMet(dependent_op_index)) {
        processOperator(dependent_op_index, true);
      }
    }
  }
}

void QueryManagerBase::processOperator(const dag_node_index index,
                                       const bool recursively_check_dependents) {
  if (fetchNormalWorkOrders(index)) {
    // Fetched work orders. Return to wait for the generated work orders to
    // execute, and skip the execution-finished checks.
    return;
  }

  if (checkNormalExecutionOver(index)) {
    if (checkRebuildRequired(index)) {
      if (!checkRebuildInitiated(index)) {
        // Rebuild hasn't started, initiate it.
        if (initiateRebuild(index)) {
          // Rebuild initiated and completed right away.
          markOperatorFinished(index);
        } else {
          // Rebuild WorkOrders have been generated.
          return;
        }
      } else if (checkRebuildOver(index)) {
        // Rebuild had been initiated and it is over.
        markOperatorFinished(index);
      }
    } else {
      // Rebuild is not required and normal execution over, mark finished.
      markOperatorFinished(index);
    }
    // If we reach here, that means the operator has been marked as finished.
    if (recursively_check_dependents) {
      for (const pair<dag_node_index, bool> &dependent_link :
           query_dag_->getDependents(index)) {
        const dag_node_index dependent_op_index = dependent_link.first;
        if (checkAllBlockingDependenciesMet(dependent_op_index)) {
          processOperator(dependent_op_index, true);
        }
      }
    }
  }
}

void QueryManagerBase::processDataPipelineMessage(const dag_node_index op_index,
                                                  const block_id block,
                                                  const relation_id rel_id) {
  for (const dag_node_index consumer_index :
       output_consumers_[op_index]) {
    // Feed the streamed block to the consumer. Note that 'output_consumers_'
    // only contain those dependents of operator with index = op_index which are
    // eligible to receive streamed input.
    query_dag_->getNodePayloadMutable(consumer_index)->feedInputBlock(block, rel_id);
    // Because of the streamed input just fed, check if there are any new
    // WorkOrders available and if so, fetch them.
    fetchNormalWorkOrders(consumer_index);
  }
}

void QueryManagerBase::markOperatorFinished(const dag_node_index index) {
  query_exec_state_->setExecutionFinished(index);

  RelationalOperator *op = query_dag_->getNodePayloadMutable(index);
  op->updateCatalogOnCompletion();

  const relation_id output_rel = op->getOutputRelationID();
  for (const pair<dag_node_index, bool> &dependent_link : query_dag_->getDependents(index)) {
    const dag_node_index dependent_op_index = dependent_link.first;
    RelationalOperator *dependent_op = query_dag_->getNodePayloadMutable(dependent_op_index);
    // Signal dependent operator that current operator is done feeding input blocks.
    if (output_rel >= 0) {
      dependent_op->doneFeedingInputBlocks(output_rel);
    }
    if (checkAllBlockingDependenciesMet(dependent_op_index)) {
      dependent_op->informAllBlockingDependenciesMet();
    }
  }
}

}  // namespace quickstep
