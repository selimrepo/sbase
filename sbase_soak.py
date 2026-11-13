#!/usr/bin/env python3
"""Standalone SBase multi-tenant soak test. Not wired into pytest, Gradle, CI or Compose.

Python 3.11+: pip install aiohttp
Example: python sbase_soak.py --tenants tenants.json --hours 168
Expensive workloads are opt-in and independently rate-limited.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import random
import signal
import time
import uuid
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlparse

import aiohttp

LOG = logging.getLogger("sbase-soak")

READS = [
    ("/api/pipelines", 3),
    ("/api/bi/catalog/tables", 2),
    ("/api/bi/reports", 2),
    ("/api/bi/dashboards", 2),
    ("/api/bi/schedules", 1),
    ("/api/ml/experiments", 2),
    ("/api/ml/models", 1),
    ("/api/platform/ingestion/connectors", 1),
    ("/api/platform/ingestion/dead-letters?limit=25", 1),
    ("/api/platform/audit-events?limit=50", 1),
]


class Stats:
    def __init__(self):
        self.count = 0
        self.errors = 0
        self.latencies = []
        self.statuses = defaultdict(int)
        self.sent = 0
        self.received = 0

    def add(self, status, elapsed, sent, received):
        self.count += 1
        self.errors += int(status < 200 or status >= 400)
        self.statuses[str(status)] += 1
        self.latencies.append(elapsed)
        self.sent += sent
        self.received += received

    def report(self):
        values = sorted(self.latencies)
        def percentile(p):
            return round(values[min(len(values) - 1, int((len(values) - 1) * p))], 2) if values else 0
        return {
            "requests": self.count, "errors": self.errors,
            "error_rate": round(self.errors / self.count, 5) if self.count else 0,
            "status_codes": dict(self.statuses),
            "latency_ms": {"p50": percentile(.50), "p95": percentile(.95),
                           "p99": percentile(.99), "max": round(max(values), 2) if values else 0},
            "bytes_sent": self.sent, "bytes_received": self.received,
        }


class Runner:
    def __init__(self, args, tenants):
        self.args, self.tenants = args, tenants
        self.base = args.base_url.rstrip("/")
        self.stop = asyncio.Event()
        self.stats = defaultdict(Stats)
        self.by_tenant = defaultdict(Stats)
        self.resources = defaultdict(dict)
        self.semaphores = {
            "pipeline": asyncio.Semaphore(args.pipeline_concurrency),
            "ingestion": asyncio.Semaphore(args.ingestion_concurrency),
            "ml": asyncio.Semaphore(args.ml_concurrency),
        }
        self.started = time.time()
        self.session = None

    async def request(self, tenant, method, path, payload=None, metric=None):
        body = json.dumps(payload, separators=(",", ":")).encode() if payload is not None else None
        headers = {"Authorization": "Bearer " + tenant["token"], "Accept": "application/json"}
        if body is not None:
            headers["Content-Type"] = "application/json"
        start = time.perf_counter()
        status, received, result = 0, 0, None
        try:
            async with self.session.request(method, self.base + path, data=body, headers=headers) as response:
                raw = await response.read()
                status, received = response.status, len(raw)
                try:
                    result = json.loads(raw) if raw else None
                except (ValueError, UnicodeDecodeError):
                    result = raw.decode("utf-8", "replace")[:500]
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            result = {"error": str(exc)}
        elapsed = (time.perf_counter() - start) * 1000
        key = metric or method + " " + path.split("?")[0]
        self.stats[key].add(status, elapsed, len(body or b""), received)
        self.by_tenant[tenant["name"]].add(status, elapsed, len(body or b""), received)
        return status, result

    async def prepare(self, tenant):
        """Create one ML experiment. Pipelines are created from tenant-provided validated definitions."""
        resources = self.resources[tenant["name"]]
        if self.args.trigger_ml and not resources.get("experiment_id"):
            status, data = await self.request(tenant, "POST", "/api/ml/experiments", {
                "name": "soak-" + tenant["name"] + "-" + uuid.uuid4().hex[:8],
                "description": "Standalone soak-test experiment"
            }, "ml_experiment_create")
            if status in (200, 201) and isinstance(data, dict):
                resources["experiment_id"] = str(data.get("id", ""))
            resources["experiment_id"] = resources["experiment_id"] or tenant.get("experiment_id", "")

        if self.args.trigger_pipelines and not resources.get("pipeline_id"):
            pipeline_id = tenant.get("pipeline_id")
            definition = tenant.get("pipeline_definition")
            if not pipeline_id and definition:
                payload = {
                    "name": "soak-" + tenant["name"] + "-" + uuid.uuid4().hex[:8],
                    "description": "Standalone soak-test pipeline",
                    "definitionJson": json.dumps(definition),
                    "enabled": True,
                }
                status, data = await self.request(tenant, "POST", "/api/pipelines", payload, "pipeline_create")
                if status in (200, 201) and isinstance(data, dict):
                    pipeline_id = data.get("id")
            resources["pipeline_id"] = str(pipeline_id or "")

    async def read_worker(self, tenant):
        paths, weights = zip(*READS)
        while not self.stop.is_set():
            await self.request(tenant, "GET", random.choices(paths, weights=weights, k=1)[0])
            await asyncio.sleep(random.uniform(self.args.min_pause, self.args.max_pause))

    async def job_worker(self, tenant, kind):
        await self.prepare(tenant)
        interval = getattr(self.args, kind + "_interval")
        sem = self.semaphores[kind]
        while not self.stop.is_set():
            try:
                async with sem:
                    if kind == "pipeline":
                        pid = self.resources[tenant["name"]].get("pipeline_id") or tenant.get("pipeline_id")
                        if pid:
                            await self.request(tenant, "POST", f"/api/pipelines/{pid}/runs", metric="pipeline_submit")
                        else:
                            LOG.warning("%s: pipeline_id or pipeline_definition missing; no pipeline run submitted", tenant["name"])
                    elif kind == "ingestion":
                        required = ("connector_id", "dataset_id")
                        if all(tenant.get(k) for k in required):
                            payload = {
                                "connectorId": tenant["connector_id"], "datasetId": tenant["dataset_id"],
                                "source": tenant.get("ingestion_source", "soak-source"),
                                "target": tenant.get("ingestion_target", "soak-target"),
                                "options": tenant.get("ingestion_options", {"format": "json"}),
                                "idempotencyKey": "soak-" + tenant["name"] + "-" + uuid.uuid4().hex,
                            }
                            kind_name = tenant.get("ingestion_type", "file")
                            await self.request(tenant, "POST",
                                f"/api/platform/ingestion/{kind_name}/runs", payload, "ingestion_submit")
                        else:
                            LOG.warning("%s: ingestion requires connector_id and dataset_id", tenant["name"])
                    else:
                        experiment = self.resources[tenant["name"]].get("experiment_id") or tenant.get("experiment_id")
                        if experiment:
                            payload = {
                                "experimentId": experiment,
                                "runName": "soak-" + uuid.uuid4().hex[:12],
                                "algorithm": tenant.get("ml_algorithm", "RANDOM_FOREST_CLASSIFIER"),
                                "taskType": tenant.get("ml_task_type", "CLASSIFICATION"),
                                "datasetTable": tenant.get("ml_dataset_table", "soak_training"),
                                "featureColumns": tenant.get("ml_feature_columns", ["feature_a", "feature_b"]),
                                "labelColumn": tenant.get("ml_label_column", "label"),
                                "crossValidationFolds": int(tenant.get("ml_folds", 3)),
                                "parameters": tenant.get("ml_parameters", {"numTrees": 25, "maxDepth": 5}),
                                "searchSpace": tenant.get("ml_search_space", {}),
                            }
                            await self.request(tenant, "POST", "/api/ml/runs", payload, "ml_train_submit")
                        else:
                            LOG.warning("%s: ML experiment unavailable; no training submitted", tenant["name"])
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                raise
            except Exception:
                LOG.exception("%s %s worker error", tenant["name"], kind)
                await asyncio.sleep(2)

    async def run(self):
        timeout = aiohttp.ClientTimeout(total=self.args.timeout)
        connector = aiohttp.TCPConnector(limit=self.args.connection_limit)
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            self.session = session
            if self.args.health_check:
                status, data = await self.request(self.tenants[0], "GET", "/actuator/health/readiness")
                if status >= 400:
                    raise RuntimeError(f"Readiness check failed ({status}): {data}")
            tasks = []
            for tenant in self.tenants:
                for _ in range(int(tenant.get("workers", 2))):
                    tasks.append(asyncio.create_task(self.read_worker(tenant)))
                for kind in ("pipeline", "ingestion", "ml"):
                    if getattr(self.args, "trigger_" + kind):
                        tasks.append(asyncio.create_task(self.job_worker(tenant, kind)))
            LOG.info("Running %d workers across %d tenants for %.2f hours",
                     len(tasks), len(self.tenants), self.args.hours)
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=self.args.hours * 3600)
            except asyncio.TimeoutError:
                pass
            finally:
                self.stop.set()
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                self.session = None

    def report(self):
        return {
            "target": self.base, "started_epoch": self.started,
            "duration_seconds": round(time.time() - self.started, 2),
            "tenants": {k: v.report() for k, v in self.by_tenant.items()},
            "operations": {k: v.report() for k, v in self.stats.items()},
            "created_resources": dict(self.resources),
        }


def arguments():
    p = argparse.ArgumentParser(description="SBase standalone multi-tenant soak test")
    p.add_argument("--base-url", default="http://localhost:8080")
    p.add_argument("--allow-remote", action="store_true")
    p.add_argument("--tenants", type=Path, default=Path("tenants.json"))
    p.add_argument("--hours", type=float, default=168)
    p.add_argument("--timeout", type=float, default=30)
    p.add_argument("--connection-limit", type=int, default=100)
    p.add_argument("--min-pause", type=float, default=.2)
    p.add_argument("--max-pause", type=float, default=1.2)
    p.add_argument("--health-check", action="store_true")
    for kind, default in (("pipeline", 60), ("ingestion", 120), ("ml", 300)):
        p.add_argument("--trigger-" + kind, action="store_true")
        p.add_argument("--" + kind + "-interval", type=float, default=default)
        p.add_argument("--" + kind + "-concurrency", type=int, default=1)
    p.add_argument("--output-dir", type=Path, default=Path("sbase-soak-results"))
    p.add_argument("--log-level", default="INFO")
    a = p.parse_args()
    if a.hours <= 0 or a.timeout <= 0 or a.min_pause < 0 or a.max_pause < a.min_pause:
        p.error("Invalid duration, timeout or pause range")
    for kind in ("pipeline", "ingestion", "ml"):
        if getattr(a, kind + "_interval") <= 0 or getattr(a, kind + "_concurrency") < 1:
            p.error(f"Invalid {kind} interval/concurrency")
    parsed = urlparse(a.base_url)
    local = parsed.hostname in {"localhost", "127.0.0.1", "::1"}
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        p.error("--base-url must be an HTTP(S) URL")
    if not local and not a.allow_remote:
        p.error("Remote target refused; pass --allow-remote explicitly")
    if not local and parsed.scheme != "https":
        p.error("Remote targets must use HTTPS")
    return a


async def main():
    args = arguments()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(message)s")
    tenants = json.loads(args.tenants.read_text(encoding="utf-8"))
    if not isinstance(tenants, list) or not tenants:
        raise SystemExit("tenants.json must be a non-empty JSON array")
    for tenant in tenants:
        if not tenant.get("name") or not tenant.get("token"):
            raise SystemExit("Every tenant requires name and token")
    runner = Runner(args, tenants)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, runner.stop.set)
        except NotImplementedError:
            pass
    try:
        await runner.run()
    finally:
        args.output_dir.mkdir(parents=True, exist_ok=True)
        report = runner.report()
        stamp = time.strftime("%Y%m%d-%H%M%S")
        json_file = args.output_dir / f"report-{stamp}.json"
        csv_file = args.output_dir / f"report-{stamp}.csv"
        json_file.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        with csv_file.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.writer(stream)
            writer.writerow(["scope", "name", "requests", "errors", "error_rate", "p50_ms", "p95_ms", "p99_ms"])
            for scope in ("tenants", "operations"):
                for name, item in report[scope].items():
                    latency = item["latency_ms"]
                    writer.writerow([scope, name, item["requests"], item["errors"],
                                     item["error_rate"], latency["p50"], latency["p95"], latency["p99"]])
        LOG.info("Reports: %s ; %s", json_file, csv_file)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
