#!/usr/bin/env python3
"""SBase platform contract, integration, concurrency and soak-test runner.

Uses only the Python standard library. Read-only checks and load tests are safe by
default. Persistent workflow/AutoML checks require the explicit --write-tests flag.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import math
import os
import random
import statistics
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


@dataclass
class HttpResult:
    status: int
    elapsed_ms: float
    body: Any = None
    raw: str = ""
    error: str | None = None
    bytes_received: int = 0


@dataclass
class Check:
    name: str
    status: str
    required: bool
    http_status: int | None = None
    elapsed_ms: float | None = None
    detail: str = ""


@dataclass(frozen=True)
class Operation:
    method: str
    path: str
    payload: dict[str, Any] | None = None


def load_env_file(path: str) -> None:
    """Load simple KEY=VALUE entries without overriding exported environment."""
    file_path = Path(path)
    if not file_path.is_file():
        return
    for line in file_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key, value = key.strip(), value.strip().strip("\"'")
        if key and key not in os.environ:
            os.environ[key] = value


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


class ApiClient:
    def __init__(self, base_url: str, timeout: float, user_agent: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.user_agent = user_agent

    def request(
        self, method: str, path: str, payload: dict[str, Any] | None = None
    ) -> HttpResult:
        url = self.base_url + (path if path.startswith("/") else "/" + path)
        data = None if payload is None else json.dumps(payload, separators=(",", ":")).encode()
        headers = {"Accept": "application/json", "User-Agent": self.user_agent}
        if data is not None:
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(url, data=data, headers=headers, method=method)
        started = time.perf_counter()
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                raw = response.read().decode("utf-8", errors="replace")
                elapsed = (time.perf_counter() - started) * 1000
                try:
                    body = json.loads(raw) if raw else None
                except json.JSONDecodeError:
                    body = None
                return HttpResult(
                    response.status, elapsed, body, raw, None,
                    len(raw.encode("utf-8")),
                )
        except urllib.error.HTTPError as error:
            raw = error.read().decode("utf-8", errors="replace")
            elapsed = (time.perf_counter() - started) * 1000
            try:
                body = json.loads(raw) if raw else None
            except json.JSONDecodeError:
                body = None
            return HttpResult(
                error.code, elapsed, body, raw,
                f"HTTP {error.code}: {raw[:500]}",
                len(raw.encode("utf-8")),
            )
        except (urllib.error.URLError, TimeoutError, OSError) as error:
            elapsed = (time.perf_counter() - started) * 1000
            reason = getattr(error, "reason", error)
            return HttpResult(0, elapsed, error=f"{type(reason).__name__}: {reason}")


class PlatformRunner:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.client = ApiClient(
            args.base_url, args.timeout,
            f"SBase-Platform-Test/{args.version}",
        )
        self.checks: list[Check] = []
        self.failures = 0
        self.warnings = 0
        self.started_at = datetime.now(timezone.utc)
        self.run_id = self.started_at.strftime("%Y%m%dT%H%M%SZ")

    def record(
        self, name: str, passed: bool, required: bool = True,
        result: HttpResult | None = None, detail: str = "",
    ) -> bool:
        if passed:
            status = "PASS"
        elif required:
            status = "FAIL"
            self.failures += 1
        else:
            status = "WARN"
            self.warnings += 1
        check = Check(
            name=name, status=status, required=required,
            http_status=result.status if result else None,
            elapsed_ms=round(result.elapsed_ms, 3) if result else None,
            detail=detail or (result.error if result else ""),
        )
        self.checks.append(check)
        marker = {"PASS": "[PASS]", "WARN": "[WARN]", "FAIL": "[FAIL]"}[status]
        suffix = f" ({result.status}, {result.elapsed_ms:.1f} ms)" if result else ""
        print(f"{marker} {name}{suffix}" + (f" — {check.detail}" if check.detail else ""))
        return passed

    def call(
        self, name: str, method: str, path: str,
        payload: dict[str, Any] | None = None,
        required: bool = True,
        expected: tuple[int, ...] = (200,),
        validator: Callable[[Any], bool] | None = None,
    ) -> HttpResult:
        result = self.client.request(method, path, payload)
        valid = result.status in expected
        if valid and validator is not None:
            try:
                valid = bool(validator(result.body))
            except Exception as error:  # A malformed response is a contract failure.
                valid = False
                result.error = f"response validation: {error}"
        detail = ""
        if not valid:
            detail = result.error or f"expected HTTP {expected}, received {result.status}"
        self.record(name, valid, required, result, detail)
        return result

    def preflight(self) -> bool:
        print("\n== API and control-plane readiness ==")
        health = self.call("API liveness", "GET", "/health")
        ready = self.call("PostgreSQL/control-plane readiness", "GET", "/api/v1/ready")
        return health.status == 200 and ready.status == 200

    def contract_suite(self) -> None:
        print("\n== API contracts and component discovery ==")
        self.call("API identity", "GET", "/api/v1")
        self.call("Capabilities", "GET", "/api/v1/capabilities")
        components = self.call(
            "Component inventory", "GET", "/api/v1/components",
            validator=lambda body: isinstance(body, list) and len(body) >= 5,
        )
        self.call(
            "Operator IDs", "GET", "/api/v1/operators",
            validator=lambda body: isinstance(body, list) and len(body) > 0,
        )
        catalog = self.call(
            "Operator descriptor catalog", "GET", "/api/v1/operators/catalog",
            validator=lambda body: isinstance(body, list) and len(body) > 0,
        )
        if catalog.status == 200 and isinstance(catalog.body, list):
            for descriptor in catalog.body:
                name = descriptor.get("name") if isinstance(descriptor, dict) else None
                if not name:
                    self.record("Operator descriptor has a name", False)
                    continue
                self.call(
                    f"Operator descriptor: {name}", "GET",
                    "/api/v1/operators/" + urllib.parse.quote(str(name), safe=""),
                    validator=lambda body, expected=name: isinstance(body, dict)
                    and body.get("name") == expected,
                )
        if components.status == 200 and isinstance(components.body, list):
            found = {
                str(item.get("id"))
                for item in components.body if isinstance(item, dict)
            }
            expected_components = {
                "workflow-engine", "catalog", "query", "automl",
                "mining", "stream-processing", "model-registry",
            }
            missing = sorted(expected_components - found)
            self.record(
                "All advertised platform components are present",
                not missing, detail=f"missing={missing}" if missing else "",
            )

        self.call(
            "Engine discovery / Spark runtime", "GET", "/api/v1/engines",
            validator=lambda body: isinstance(body, dict)
            and isinstance(body.get("spark"), dict)
            and body["spark"].get("status") == "UP",
        )
        self.call("AutoML metric discovery", "GET", "/api/v1/automl/metrics")
        self.call("Workflow listing", "GET", "/api/v1/workflows?limit=20")
        self.call("Job listing", "GET", "/api/v1/jobs?limit=20")
        self.call("Schedule listing", "GET", "/api/v1/schedules")
        self.call("AutoML experiment listing", "GET", "/api/v1/automl/experiments")
        self.call("Model registry listing", "GET", "/api/v1/models")
        self.call("Model aliases", "GET", "/api/v1/models/aliases")
        self.call(
            "Statistical drift history", "GET",
            "/api/v1/automl/monitoring/statistical-runs?limit=10",
        )

        self.call(
            "PostgreSQL read-only query", "POST",
            "/api/v1/catalogs/postgresql/query",
            {"sql": "SELECT 1 AS sbase_probe", "parameters": [], "limit": 1},
        )
        self.call(
            "Spark SQL execution", "POST", "/api/v1/engines/spark/sql",
            {"sql": "SELECT 1 AS sbase_probe", "limit": 1},
            validator=lambda body: isinstance(body, dict)
            and body.get("engine") == "spark-sql"
            and body.get("rowCount", 0) >= 1,
        )
        self.call(
            "Federated query planning", "POST", "/api/v1/query/plan",
            {
                "sql": "SELECT customer_id FROM customers",
                "catalog": {
                    "customers": {
                        "source": "postgresql", "rows": 100,
                        "bytes": 4096, "partitions": 1,
                    }
                },
            },
        )
        definition = self.workflow_definition()
        self.call(
            "Workflow graph validation", "POST", "/api/v1/workflows/validate",
            {"definition": definition},
            validator=lambda body: isinstance(body, dict)
            and body.get("valid") is True,
        )

    @staticmethod
    def workflow_definition() -> dict[str, Any]:
        return {
            "nodes": [
                {
                    "id": "source",
                    "operator": "dataset-values",
                    "config": {"values": [1, 2, 3, 4, 5]},
                },
                {
                    "id": "sum",
                    "operator": "mahout-vector-sum",
                    "config": {},
                },
            ],
            "edges": [{"source": "source", "target": "sum"}],
        }

    def optional_connectors(self) -> None:
        if not self.args.include_optional:
            return
        print("\n== Optional connector and engine checks ==")
        self.call("Catalog connector discovery", "GET", "/api/v1/catalogs", required=False)
        self.call(
            "Cassandra system query", "POST", "/api/v1/catalogs/cassandra/query",
            {"cql": "SELECT release_version FROM system.local", "limit": 1},
            required=False,
        )
        self.call(
            "Flink overview", "GET", "/api/v1/engines/flink/overview",
            required=False,
        )
        self.call(
            "Flink jobs overview", "GET", "/api/v1/engines/flink/jobs",
            required=False,
        )
        collection = os.getenv("SBASE_SOLR_TEST_COLLECTION", "").strip()
        if collection:
            self.call(
                "Solr test collection query", "POST",
                "/api/v1/catalogs/solr/query",
                {"collection": collection, "q": "*:*", "limit": 1},
                required=False,
            )
        else:
            self.record(
                "Solr query", True, required=False,
                detail="skipped; set SBASE_SOLR_TEST_COLLECTION to enable",
            )

    def write_tests(self) -> None:
        if not self.args.write_tests:
            return
        print("\n== Persistent integration tests (explicitly enabled) ==")
        self.workflow_lifecycle_test()
        self.dataset_profile_test()
        self.statistical_drift_test()
        if self.args.automl_training:
            self.automl_training_test()

    def workflow_lifecycle_test(self) -> None:
        definition = self.workflow_definition()
        name = f"platform-test-{self.run_id}"
        created = self.call(
            "Create disposable workflow", "POST", "/api/v1/workflows",
            {"name": name, "definition": definition}, expected=(201,),
        )
        if created.status != 201 or not isinstance(created.body, dict):
            return
        workflow_id = created.body.get("workflowId")
        if not workflow_id:
            self.record("Created workflow ID", False)
            return
        encoded = urllib.parse.quote(str(workflow_id), safe="")
        self.call("Read created workflow", "GET", f"/api/v1/workflows/{encoded}")
        self.call("List immutable workflow versions", "GET", f"/api/v1/workflows/{encoded}/versions")
        self.call("Read workflow version 1", "GET", f"/api/v1/workflows/{encoded}/versions/1")

        key = f"platform-test-{self.run_id}"
        submitted = self.call(
            "Submit workflow run", "POST", f"/api/v1/workflows/{encoded}/runs",
            {"idempotencyKey": key, "maxAttempts": 2}, expected=(202,),
        )
        if submitted.status != 202 or not isinstance(submitted.body, dict):
            return
        job_id = submitted.body.get("jobId")
        if not job_id:
            self.record("Submitted job ID", False)
            return
        repeated = self.call(
            "Idempotent duplicate submission", "POST",
            f"/api/v1/workflows/{encoded}/runs",
            {"idempotencyKey": key, "maxAttempts": 2}, expected=(202,),
        )
        if repeated.status == 202:
            same_job = isinstance(repeated.body, dict) and repeated.body.get("jobId") == job_id
            self.record(
                "Idempotency key returns the same job",
                same_job, detail="" if same_job else "duplicate submission created a different job",
            )
        self.call("Read workflow runs", "GET", f"/api/v1/workflows/{encoded}/runs?limit=10")

        deadline = time.monotonic() + self.args.job_timeout
        terminal: dict[str, Any] | None = None
        while time.monotonic() < deadline:
            status = self.client.request("GET", f"/api/v1/jobs/{job_id}")
            if status.status == 200 and isinstance(status.body, dict):
                value = str(status.body.get("status", "")).upper()
                if value in {"SUCCEEDED", "SUCCESS", "FAILED", "CANCELLED", "DEAD"}:
                    terminal = status.body
                    self.record("Poll workflow job to terminal state", True, result=status)
                    break
            elif status.status not in (0, 200):
                self.record("Poll workflow job", False, result=status)
                break
            time.sleep(self.args.poll_interval)
        if terminal is None:
            self.record(
                "Workflow job reaches terminal state", False,
                detail=f"timed out after {self.args.job_timeout}s",
            )
            return
        state = str(terminal.get("status", "")).upper()
        self.record(
            "Workflow job execution succeeds", state in {"SUCCEEDED", "SUCCESS"},
            detail=f"terminal status={state}",
        )
        self.call("Read job execution events", "GET", f"/api/v1/jobs/{job_id}/events")

    def sample_rows(self, count: int = 80, shift: float = 0.0) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for index in range(count):
            value = float(index) + shift
            rows.append({
                "value": value,
                "segment": "north" if index % 2 == 0 else "south",
                "target": 1 if index % 2 == 0 else 0,
            })
        return rows

    def dataset_profile_test(self) -> None:
        rows = self.sample_rows(60)
        result = self.call(
            "Spark dataset profiling", "POST", "/api/v1/automl/data/profile",
            {"data": rows, "features": ["value", "segment"], "label": "target"},
            validator=lambda body: isinstance(body, dict)
            and body.get("rows") == 60
            and bool(body.get("datasetFingerprint")),
        )
        if result.status == 200 and isinstance(result.body, dict):
            self.record(
                "Dataset profile contains feature diagnostics",
                isinstance(result.body.get("features"), list)
                and len(result.body["features"]) == 2,
            )

    def statistical_drift_test(self) -> None:
        reference = self.sample_rows(80)
        current = self.sample_rows(80, shift=250.0)
        result = self.call(
            "PSI/KS statistical drift analysis", "POST",
            "/api/v1/automl/monitoring/statistical-drift",
            {
                "features": ["value", "segment"],
                "reference": reference,
                "current": current,
                "psiThreshold": 0.2,
                "ksAlpha": 0.05,
                "missingRateDeltaThreshold": 0.1,
            },
            validator=lambda body: isinstance(body, dict)
            and isinstance(body.get("features"), list)
            and len(body["features"]) == 2,
        )
        if result.status == 200:
            self.call(
                "Persisted statistical drift history", "GET",
                "/api/v1/automl/monitoring/statistical-runs?limit=5",
            )

    def automl_training_test(self) -> None:
        rows = []
        for index in range(100):
            signal = float(index % 20)
            rows.append({
                "signal": signal,
                "secondary": float((index * 7) % 13),
                "label": 1 if signal >= 10 else 0,
            })
        self.call(
            "Spark ML classification training (opt-in)",
            "POST", "/api/v1/automl/train",
            {
                "name": f"platform-test-automl-{self.run_id}",
                "task": "classification",
                "label": "label",
                "features": ["signal", "secondary"],
                "optimizationMetric": "f1",
                "seed": 42,
                "cvFolds": 2,
                "cvParallelism": 1,
                "maxCandidates": 1,
                "data": rows,
            },
        )

    def load_operations(self) -> list[Operation]:
        reads = [
            Operation("GET", "/health"),
            Operation("GET", "/api/v1/ready"),
            Operation("GET", "/api/v1/capabilities"),
            Operation("GET", "/api/v1/components"),
            Operation("GET", "/api/v1/operators"),
            Operation("GET", "/api/v1/automl/metrics"),
            Operation("GET", "/api/v1/workflows?limit=20"),
            Operation("GET", "/api/v1/jobs?limit=20"),
            Operation("GET", "/api/v1/models"),
            Operation("GET", "/api/v1/models/aliases"),
            Operation("GET", "/api/v1/automl/experiments"),
        ]
        if self.args.scenario in {"balanced", "compute"}:
            reads.extend([
                Operation("GET", "/api/v1/operators/catalog"),
                Operation("GET", "/api/v1/schedules"),
                Operation("GET", "/api/v1/automl/monitoring/statistical-runs?limit=10"),
            ])
        if self.args.scenario == "compute":
            reads.extend([
                Operation(
                    "POST", "/api/v1/engines/spark/sql",
                    {"sql": "SELECT 1 AS load_probe", "limit": 1},
                ),
                Operation(
                    "POST", "/api/v1/catalogs/postgresql/query",
                    {"sql": "SELECT 1 AS load_probe", "parameters": [], "limit": 1},
                ),
                Operation(
                    "POST", "/api/v1/workflows/validate",
                    {"definition": self.workflow_definition()},
                ),
                Operation(
                    "POST", "/api/v1/query/plan",
                    {
                        "sql": "SELECT customer_id FROM customers",
                        "catalog": {
                            "customers": {
                                "source": "postgresql", "rows": 1000,
                                "bytes": 4096, "partitions": 2,
                            }
                        },
                    },
                ),
            ])
        return reads

    def load_test(self) -> dict[str, Any] | None:
        if self.args.requests <= 0 and self.args.duration_seconds <= 0:
            return None
        if self.args.workers > 100 and not self.args.allow_high_load:
            raise ValueError("More than 100 workers requires --allow-high-load")
        operations = self.load_operations()
        if not operations:
            return None

        print("\n== Concurrent load test ==")
        print(
            f"scenario={self.args.scenario}, workers={self.args.workers}, "
            f"requests={self.args.requests}, duration={self.args.duration_seconds}s"
        )
        counter = 0
        counter_lock = threading.Lock()
        start = time.monotonic()
        duration_deadline = (
            start + self.args.duration_seconds if self.args.duration_seconds > 0 else None
        )

        def worker(worker_id: int) -> list[dict[str, Any]]:
            nonlocal counter
            local: list[dict[str, Any]] = []
            while True:
                with counter_lock:
                    if duration_deadline is not None:
                        if time.monotonic() >= duration_deadline:
                            break
                    elif counter >= self.args.requests:
                        break
                    sequence = counter
                    counter += 1
                operation = operations[(sequence + worker_id) % len(operations)]
                result = self.client.request(
                    operation.method, operation.path, operation.payload
                )
                local.append({
                    "method": operation.method,
                    "path": operation.path,
                    "status": result.status,
                    "elapsed_ms": result.elapsed_ms,
                    "bytes_received": result.bytes_received,
                    "error": result.error,
                })
            return local

        results: list[dict[str, Any]] = []
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.args.workers, thread_name_prefix="sbase-load"
        ) as pool:
            futures = [pool.submit(worker, index) for index in range(self.args.workers)]
            for future in concurrent.futures.as_completed(futures):
                results.extend(future.result())

        elapsed = max(time.monotonic() - start, 0.000001)
        latencies = [float(item["elapsed_ms"]) for item in results]
        failures = [
            item for item in results
            if not 200 <= int(item["status"]) < 300
        ]
        statuses = Counter(str(item["status"]) for item in results)
        by_route: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for item in results:
            by_route[f'{item["method"]} {item["path"]}'].append(item)

        route_stats: dict[str, Any] = {}
        for route, items in sorted(by_route.items()):
            route_latencies = [float(item["elapsed_ms"]) for item in items]
            route_stats[route] = {
                "requests": len(items),
                "errors": sum(not 200 <= int(item["status"]) < 300 for item in items),
                "p50_ms": round(percentile(route_latencies, 0.50), 3),
                "p95_ms": round(percentile(route_latencies, 0.95), 3),
                "p99_ms": round(percentile(route_latencies, 0.99), 3),
                "mean_ms": round(statistics.fmean(route_latencies), 3) if route_latencies else 0,
            }

        error_rate = len(failures) / len(results) if results else 1.0
        summary = {
            "scenario": self.args.scenario,
            "workers": self.args.workers,
            "requests": len(results),
            "errors": len(failures),
            "error_rate": round(error_rate, 6),
            "duration_seconds": round(elapsed, 3),
            "throughput_rps": round(len(results) / elapsed, 3),
            "latency_ms": {
                "min": round(min(latencies), 3) if latencies else 0,
                "mean": round(statistics.fmean(latencies), 3) if latencies else 0,
                "p50": round(percentile(latencies, 0.50), 3),
                "p95": round(percentile(latencies, 0.95), 3),
                "p99": round(percentile(latencies, 0.99), 3),
                "max": round(max(latencies), 3) if latencies else 0,
            },
            "http_statuses": dict(statuses),
            "bytes_received": sum(int(item["bytes_received"]) for item in results),
            "routes": route_stats,
            "sample_errors": failures[:20],
        }
        print(
            f"requests={summary['requests']} errors={summary['errors']} "
            f"error_rate={error_rate:.2%} throughput={summary['throughput_rps']:.1f} req/s"
        )
        print(
            "latency ms: "
            f"p50={summary['latency_ms']['p50']:.1f}, "
            f"p95={summary['latency_ms']['p95']:.1f}, "
            f"p99={summary['latency_ms']['p99']:.1f}"
        )
        passed = bool(results) and error_rate <= self.args.max_error_rate
        self.record(
            "Load-test error-rate objective",
            passed,
            detail=f"{error_rate:.2%} errors; allowed {self.args.max_error_rate:.2%}",
        )
        return summary

    def studio_check(self) -> None:
        if not self.args.studio_url:
            return
        result = ApiClient(
            self.args.studio_url, self.args.timeout, "SBase-Platform-Test"
        ).request("GET", "/")
        self.record("React Workflow Studio HTTP", 200 <= result.status < 400, result=result)

    def report(self, load_summary: dict[str, Any] | None) -> Path:
        ended = datetime.now(timezone.utc)
        payload = {
            "tool": "SBase Platform Test",
            "version": self.args.version,
            "run_id": self.run_id,
            "base_url": self.args.base_url,
            "started_at": self.started_at.isoformat(),
            "finished_at": ended.isoformat(),
            "summary": {
                "checks": len(self.checks),
                "passed": sum(item.status == "PASS" for item in self.checks),
                "warnings": self.warnings,
                "failures": self.failures,
            },
            "checks": [asdict(item) for item in self.checks],
            "load_test": load_summary,
        }
        output = Path(self.args.report)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"\nJSON report: {output}")
        return output

    def run(self) -> int:
        if not self.preflight():
            print("\nAPI readiness failed; remaining checks were not started.")
            self.report(None)
            return 2
        self.contract_suite()
        self.optional_connectors()
        self.write_tests()
        self.studio_check()
        load_summary = self.load_test()
        self.report(load_summary)
        print(
            f"\nResult: {sum(item.status == 'PASS' for item in self.checks)} passed, "
            f"{self.warnings} warnings, {self.failures} failures"
        )
        return 1 if self.failures else 0


def run_self_tests() -> int:
    assert percentile([1.0, 2.0, 3.0, 4.0], 0.50) == 2.5
    assert percentile([10.0], 0.99) == 10.0
    assert percentile([], 0.95) == 0.0
    rows = PlatformRunner.sample_rows  # Ensure deterministic fixture generator exists.
    assert callable(rows)
    definition = PlatformRunner.workflow_definition()
    assert len(definition["nodes"]) == 2 and len(definition["edges"]) == 1
    print("Python test runner self-tests passed.")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Deep SBase API/component verification and concurrent load testing."
    )
    parser.add_argument("--env-file", default=".env", help="Optional KEY=VALUE file (default: .env)")
    parser.add_argument("--base-url", default=None, help="API base URL; default uses SBASE_BASE_URL or localhost")
    parser.add_argument("--studio-url", default=os.getenv("SBASE_STUDIO_URL", ""), help="Optional Studio URL")
    parser.add_argument("--timeout", type=float, default=15.0, help="Per-request timeout in seconds")
    parser.add_argument("--workers", type=int, default=12, help="Concurrent load workers (1..256)")
    parser.add_argument("--requests", type=int, default=300, help="Total load requests; ignored when duration is set")
    parser.add_argument("--duration-seconds", type=int, default=0, help="Run continuously for N seconds (supports long soak runs)")
    parser.add_argument("--scenario", choices=("smoke", "balanced", "compute"), default="balanced")
    parser.add_argument("--max-error-rate", type=float, default=0.01, help="Maximum allowed load-test HTTP error ratio")
    parser.add_argument("--include-optional", action="store_true", help="Probe optional Cassandra, Solr and Flink integrations")
    parser.add_argument("--write-tests", action="store_true", help="Create persistent workflow/drift test records")
    parser.add_argument("--automl-training", action="store_true", help="Also run a small Spark ML training job (requires --write-tests)")
    parser.add_argument("--job-timeout", type=int, default=120, help="Workflow job completion timeout in seconds")
    parser.add_argument("--poll-interval", type=float, default=1.0, help="Workflow job polling interval")
    parser.add_argument("--allow-high-load", action="store_true", help="Acknowledge load above 100 concurrent workers")
    parser.add_argument("--report", default="artifacts/platform-test-report.json", help="JSON report output path")
    parser.add_argument("--version", action="version", version="SBase Platform Test 1.0.0")
    parser.add_argument("--self-test", action="store_true", help="Run local test-runner self-tests and exit")
    args = parser.parse_args()

    load_env_file(args.env_file)
    port = os.getenv("SBASE_HTTP_PORT", "8080")
    if args.base_url is None:
        args.base_url = os.getenv("SBASE_BASE_URL", f"http://localhost:{port}")
    if args.workers < 1 or args.workers > 256:
        parser.error("--workers must be between 1 and 256")
    if args.requests < 0 or args.duration_seconds < 0:
        parser.error("--requests and --duration-seconds cannot be negative")
    if args.requests == 0 and args.duration_seconds == 0 and not args.self_test:
        parser.error("set --requests or --duration-seconds to run a load test")
    if args.timeout <= 0 or args.job_timeout <= 0 or args.poll_interval <= 0:
        parser.error("timeouts and poll interval must be positive")
    if not 0 <= args.max_error_rate <= 1:
        parser.error("--max-error-rate must be between 0 and 1")
    if args.automl_training and not args.write_tests:
        parser.error("--automl-training requires --write-tests")
    return args


def main() -> int:
    args = parse_args()
    if args.self_test:
        return run_self_tests()
    runner = PlatformRunner(args)
    try:
        return runner.run()
    except KeyboardInterrupt:
        print("\nInterrupted; current run was stopped.", file=sys.stderr)
        return 130
    except Exception as error:
        print(f"\nFatal test-runner error: {type(error).__name__}: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
