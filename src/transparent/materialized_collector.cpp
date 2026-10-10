/*
 * Copyright 2026, Sirius Contributors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
 * OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
#include "transparent/materialized_collector.hpp"

#include "transparent/physical_sirius_execution.hpp"

#include <duckdb/common/types/column/column_data_collection.hpp>
#include <duckdb/common/types/column/column_data_scan_states.hpp>
#include <duckdb/execution/operator/helper/physical_result_collector.hpp>
#include <duckdb/main/client_config.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/materialized_query_result.hpp>
#include <duckdb/main/prepared_statement_data.hpp>

namespace sirius::transparent {
namespace {
// PhysicalSiriusExecution is a single-threaded source. Its materialized result
// needs a collection and a value snapshot of client properties, not ownership of
// ClientContext. A strong context reference here would cycle through its executor
// and keep an abandoned PendingQuery's validated plan and reservations alive.
class materialized_collector final : public duckdb::PhysicalResultCollector {
  struct global_state final : duckdb::GlobalSinkState {
    global_state(duckdb::unique_ptr<duckdb::ColumnDataCollection> data,
                 duckdb::ClientProperties properties)
      : collection(std::move(data)), client_properties(std::move(properties))
    {
    }
    duckdb::unique_ptr<duckdb::ColumnDataCollection> collection;
    duckdb::ClientProperties client_properties;
  };
  struct local_state final : duckdb::LocalSinkState {
    duckdb::ColumnDataAppendState append;
    bool initialized{false};
  };

 public:
  explicit materialized_collector(duckdb::PreparedStatementData& data)
    : PhysicalResultCollector(*data.physical_plan, data)
  {
  }

  duckdb::unique_ptr<duckdb::GlobalSinkState> GetGlobalSinkState(
    duckdb::ClientContext& context) const override
  {
    return duckdb::make_uniq<global_state>(CreateCollection(context),
                                           context.GetClientProperties());
  }
  duckdb::unique_ptr<duckdb::LocalSinkState> GetLocalSinkState(
    duckdb::ExecutionContext&) const override
  {
    return duckdb::make_uniq<local_state>();
  }
  duckdb::SinkResultType Sink(duckdb::ExecutionContext&,
                              duckdb::DataChunk& chunk,
                              duckdb::OperatorSinkInput& input) const override
  {
    auto& global = input.global_state.Cast<global_state>();
    auto& local  = input.local_state.Cast<local_state>();
    if (!local.initialized) {
      global.collection->InitializeAppend(local.append);
      local.initialized = true;
    }
    global.collection->Append(local.append, chunk);
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }
  duckdb::SinkCombineResultType Combine(duckdb::ExecutionContext&,
                                        duckdb::OperatorSinkCombineInput&) const override
  {
    return duckdb::SinkCombineResultType::FINISHED;
  }
  duckdb::unique_ptr<duckdb::QueryResult> GetResult(duckdb::GlobalSinkState& state) const override
  {
    auto& global = state.Cast<global_state>();
    return duckdb::make_uniq<duckdb::MaterializedQueryResult>(
      statement_type, properties, names, std::move(global.collection), global.client_properties);
  }
  bool ParallelSink() const override { return false; }
  bool SinkOrderDependent() const override { return true; }
};
}  // namespace

void install_materialized_collector(duckdb::ClientContext& context)
{
  auto& config = duckdb::ClientConfig::GetConfig(context);
  if (config.get_result_collector) { return; }
  config.get_result_collector =
    [](duckdb::ClientContext& client,
       duckdb::PreparedStatementData& data) -> duckdb::unique_ptr<duckdb::PhysicalOperator> {
    if (dynamic_cast<PhysicalSiriusExecution*>(&data.physical_plan->Root())) {
      return duckdb::make_uniq<materialized_collector>(data);
    }
    return duckdb::PhysicalResultCollector::GetResultCollector(client, data);
  };
}
}  // namespace sirius::transparent
