"""OTel MeterProvider factory for questrade-extract.

Dev: point OTEL_EXPORTER_OTLP_ENDPOINT at otel-gui (github.com/metafab/otel-gui)
for zero-config local trace/metric inspection instead of the collector.
"""

from __future__ import annotations

import os

from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader


def setup_meter(service_name: str) -> tuple[MeterProvider, object]:
    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
    exporter = OTLPMetricExporter(endpoint=endpoint, insecure=True)
    reader = PeriodicExportingMetricReader(exporter, export_interval_millis=5_000)
    provider = MeterProvider(metric_readers=[reader])
    return provider, provider.get_meter(service_name)
