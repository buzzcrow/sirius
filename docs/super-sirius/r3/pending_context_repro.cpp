/*
 * Copyright 2026, Sirius Contributors.
 * SPDX-License-Identifier: Apache-2.0
 */
// Standalone diagnostic; run with SIRIUS_DISABLE=1. The pauses expose the
// pending executor's asynchronous initialization, not a cleanup workaround.
#include <duckdb.hpp>
#include <duckdb/main/client_context.hpp>

#include <chrono>
#include <cstdio>
#include <thread>
int main()
{
  duckdb::DuckDB db(nullptr);
  for (int i = 0; i < 5; ++i) {
    auto con      = std::make_unique<duckdb::Connection>(db);
    auto prepared = con->Prepare("SELECT 42");
    if (prepared->HasError()) return 2;
    duckdb::vector<duckdb::Value> parameters;
    auto pending = prepared->PendingQuery(parameters, false);
    if (pending->HasError()) return 3;
    duckdb::weak_ptr<duckdb::ClientContext> weak = con->context;
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    con.reset();
    pending.reset();
    prepared.reset();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::printf("CPU only SELECT 42: retained context after all handles dropped: %d\n",
                !weak.expired());
    if (auto context = weak.lock()) context->Destroy();
    std::printf("after explicit DuckDB Destroy: expired=%d\n", weak.expired());
  }
}
