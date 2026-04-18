"""OTel MeterProvider factory for questrade-extract.

Dev: point OTEL_EXPORTER_OTLP_ENDPOINT at otel-gui (github.com/metafab/otel-gui)
for zero-config local trace/metric inspection instead of the collector.
"""

from __future__ import annotations

import os

from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import SERVICE_NAME, Resource


def setup_meter(service_name: str) -> tuple[MeterProvider, object]:
    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
    exporter = OTLPMetricExporter(endpoint=endpoint)
    reader = PeriodicExportingMetricReader(exporter, export_interval_millis=30_000)
    resource = Resource.create({SERVICE_NAME: service_name})
    provider = MeterProvider(metric_readers=[reader], resource=resource)
    return provider, provider.get_meter(service_name)
