"""Pruebas adicionales: casos límite del oráculo y pruebas temporales con TestStream.

Complementan a `test_assignment.py` (provisto por la cátedra) con lo que pide la
sección 3 del notebook: pruebas con `TestPipeline` y al menos una prueba
temporal con `TestStream` que evidencie un resultado late aceptado.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import apache_beam as beam
import pytest
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream as BeamTestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue

DATA_PATH = Path(__file__).parents[1] / "data" / "payments.jsonl"
PANE_TIMING = {0: "EARLY", 1: "ON_TIME", 2: "LATE", 3: "UNKNOWN"}


def load_events() -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in DATA_PATH.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def epoch(raw_value: str) -> float:
    return datetime.fromisoformat(raw_value.replace("Z", "+00:00")).timestamp()


def iso(seconds: float) -> str:
    return datetime.fromtimestamp(seconds, tz=UTC).isoformat()


def payment(event_id: str, merchant_id: str, second: int, amount: int) -> dict[str, Any]:
    """Pago CONFIRMED cuyo event_time es 13:00:00 + `second`."""
    return {
        "event_id": event_id,
        "merchant_id": merchant_id,
        "event_time": iso(epoch("2026-07-24T13:00:00Z") + second),
        "amount": amount,
        "status": "CONFIRMED",
    }


class FormatPane(beam.DoFn):
    def process(
        self,
        element,
        window=beam.DoFn.WindowParam,
        pane=beam.DoFn.PaneInfoParam,
    ):
        merchant_id, total = element
        yield (
            merchant_id,
            iso(float(window.start)),
            PANE_TIMING[pane.timing],
            total,
        )


def streaming_panes(solution, stream, *, allowed_lateness_seconds: int = 120):
    """Pipeline de streaming armado con las piezas del notebook."""
    return (
        stream
        | "Confirmados" >> beam.Filter(lambda event: event["status"] == "CONFIRMED")
        | "Ventanas+triggers"
        >> solution.build_trigger_policy(
            window_seconds=60,
            allowed_lateness_seconds=allowed_lateness_seconds,
        )
        | "ClavePorComercio"
        >> beam.Map(lambda event: (event["merchant_id"], event)).with_output_types(tuple[str, dict])
        | "Deduplicar" >> beam.ParDo(solution.DeduplicatePayments())
        | "Monto" >> beam.MapTuple(lambda merchant_id, event: (merchant_id, event["amount"]))
        | "Sumar" >> beam.CombinePerKey(sum)
        | "Pane" >> beam.ParDo(FormatPane())
    )


def streaming_options() -> PipelineOptions:
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    return options


# --------------------------------------------------------------------------- #
# Oráculo determinista: casos límite
# --------------------------------------------------------------------------- #


def test_parse_utc_rejects_invalid_values(solution):
    with pytest.raises(ValueError, match="no-es-fecha"):
        solution.parse_utc("no-es-fecha")


def test_parse_utc_rejects_timestamps_without_utc_marker(solution):
    with pytest.raises(ValueError, match="UTC"):
        solution.parse_utc("2026-07-24T13:00:05")


def test_window_boundary_belongs_to_next_window(solution):
    boundary = datetime(2026, 7, 24, 13, 1, 0, tzinfo=UTC)

    start, end = solution.assign_fixed_window(boundary, 60)

    assert start == boundary
    assert end == datetime(2026, 7, 24, 13, 2, 0, tzinfo=UTC)


def test_default_run_counts_events_accepted_and_totals(solution):
    totals, audit = solution.summarize_payments(load_events())

    assert len(audit) == 9
    assert sum(row["accepted"] for row in audit) == 5
    assert totals == [
        {
            "merchant_id": "m-azul",
            "window_start": "2026-07-24T13:00:00+00:00",
            "window_end": "2026-07-24T13:01:00+00:00",
            "total": 170_000,
        },
        {
            "merchant_id": "m-azul",
            "window_start": "2026-07-24T13:02:00+00:00",
            "window_end": "2026-07-24T13:03:00+00:00",
            "total": 200_000,
        },
        {
            "merchant_id": "m-verde",
            "window_start": "2026-07-24T13:00:00+00:00",
            "window_end": "2026-07-24T13:01:00+00:00",
            "total": 80_000,
        },
        {
            "merchant_id": "m-verde",
            "window_start": "2026-07-24T13:01:00+00:00",
            "window_end": "2026-07-24T13:02:00+00:00",
            "total": 90_000,
        },
    ]


def test_audit_explains_every_decision(solution):
    _, audit = solution.summarize_payments(load_events())

    assert [(row["event_id"], row["reason"]) for row in audit] == [
        ("p-001", "accepted"),
        ("p-002", "accepted"),
        ("p-003", "not_confirmed"),
        ("p-004", "accepted"),
        ("p-002", "duplicate"),
        ("p-005", "accepted"),
        ("p-006", "accepted"),
        ("p-008", "not_confirmed"),  # llegó a las 13:02:30, antes que p-007
        ("p-007", "too_late"),
    ]
    p004 = next(row for row in audit if row["event_id"] == "p-004")
    assert p004["delay_seconds"] == 53
    assert p004["revision"] is True
    p001 = next(row for row in audit if row["event_id"] == "p-001")
    assert p001["revision"] is False


def test_without_deduplication_the_duplicate_inflates_the_total(solution):
    totals, _ = solution.summarize_payments(load_events(), deduplicate=False)

    verde = next(
        row
        for row in totals
        if row["merchant_id"] == "m-verde" and row["window_start"] == "2026-07-24T13:00:00+00:00"
    )
    assert verde["total"] == 160_000


# --------------------------------------------------------------------------- #
# Beam en batch (TestPipeline)
# --------------------------------------------------------------------------- #


def test_batch_pipeline_filters_deduplicates_and_separates_windows(solution):
    """En batch no existe atraso: p-007 entra a su ventana (110k y no 80k)."""
    with BeamTestPipeline() as pipeline:
        output = solution.build_windowed_totals_pipeline(pipeline, load_events())
        assert_that(
            output | beam.Map(lambda row: (row["merchant_id"], row["window_start"], row["total"])),
            equal_to(
                [
                    ("m-azul", "2026-07-24T13:00:00+00:00", 170_000),
                    ("m-azul", "2026-07-24T13:02:00+00:00", 200_000),
                    ("m-verde", "2026-07-24T13:00:00+00:00", 110_000),
                    ("m-verde", "2026-07-24T13:01:00+00:00", 90_000),
                ]
            ),
        )


def test_stateful_dofn_drops_repeated_id_within_a_key(solution):
    events = [
        ("m-a", {"event_id": "x", "amount": 1}),
        ("m-a", {"event_id": "x", "amount": 1}),
        ("m-a", {"event_id": "y", "amount": 2}),
    ]

    with BeamTestPipeline() as pipeline:
        output = (
            pipeline
            | beam.Create(events)
            | beam.WindowInto(beam.window.FixedWindows(60))
            | beam.ParDo(solution.DeduplicatePayments())
            | beam.Map(lambda item: item[1]["event_id"])
        )
        assert_that(output, equal_to(["x", "y"]))


# --------------------------------------------------------------------------- #
# Beam en streaming (TestStream)
# --------------------------------------------------------------------------- #


def test_stream_late_event_within_tolerance_revises_the_total(solution):
    base = epoch("2026-07-24T13:00:00Z")
    stream = (
        BeamTestStream()
        .advance_watermark_to(base)
        .add_elements([TimestampedValue(payment("p-1", "m-a", 5, 10), base + 5)])
        .advance_watermark_to(base + 70)  # cierra [13:00, 13:01): pane ON_TIME
        .add_elements([TimestampedValue(payment("p-2", "m-a", 42, 20), base + 42)])
        .advance_watermark_to_infinity()
    )

    with BeamTestPipeline(options=streaming_options()) as pipeline:
        panes = streaming_panes(solution, pipeline | stream)
        assert_that(
            panes,
            equal_to(
                [
                    ("m-a", "2026-07-24T13:00:00+00:00", "ON_TIME", 10),
                    ("m-a", "2026-07-24T13:00:00+00:00", "LATE", 30),
                ]
            ),
        )


def test_stream_event_beyond_allowed_lateness_is_dropped(solution):
    base = epoch("2026-07-24T13:00:00Z")
    stream = (
        BeamTestStream()
        .advance_watermark_to(base)
        .add_elements([TimestampedValue(payment("p-1", "m-a", 5, 10), base + 5)])
        .advance_watermark_to(base + 60 + 120 + 1)  # fin de ventana + lateness
        .add_elements([TimestampedValue(payment("p-2", "m-a", 42, 20), base + 42)])
        .advance_watermark_to_infinity()
    )

    with BeamTestPipeline(options=streaming_options()) as pipeline:
        panes = streaming_panes(solution, pipeline | stream)
        assert_that(
            panes,
            equal_to([("m-a", "2026-07-24T13:00:00+00:00", "ON_TIME", 10)]),
        )


def test_stream_late_duplicate_does_not_produce_a_revision(solution):
    base = epoch("2026-07-24T13:00:00Z")
    stream = (
        BeamTestStream()
        .advance_watermark_to(base)
        .add_elements([TimestampedValue(payment("p-1", "m-a", 5, 10), base + 5)])
        .advance_watermark_to(base + 70)
        .add_elements([TimestampedValue(payment("p-1", "m-a", 5, 10), base + 5)])
        .advance_watermark_to_infinity()
    )

    with BeamTestPipeline(options=streaming_options()) as pipeline:
        panes = streaming_panes(solution, pipeline | stream)
        assert_that(
            panes,
            equal_to([("m-a", "2026-07-24T13:00:00+00:00", "ON_TIME", 10)]),
        )


def test_stream_replay_of_dataset_converges_to_the_oracle(solution):
    """Reproducir el dataset en orden de llegada, con watermark = arrival_time.

    El último pane de cada (comercio, ventana) debe coincidir con el oráculo.
    """
    events = sorted(load_events(), key=lambda event: epoch(event["arrival_time"]))
    stream = BeamTestStream().advance_watermark_to(epoch("2026-07-24T13:00:00Z"))
    for event in events:
        stream = stream.advance_watermark_to(epoch(event["arrival_time"])).add_elements(
            [TimestampedValue(event, epoch(event["event_time"]))]
        )
    stream = stream.advance_watermark_to_infinity()

    with BeamTestPipeline(options=streaming_options()) as pipeline:
        panes = streaming_panes(solution, pipeline | stream)
        assert_that(
            panes,
            equal_to(
                [
                    ("m-azul", "2026-07-24T13:00:00+00:00", "ON_TIME", 120_000),
                    ("m-azul", "2026-07-24T13:00:00+00:00", "LATE", 170_000),
                    ("m-azul", "2026-07-24T13:02:00+00:00", "ON_TIME", 200_000),
                    ("m-verde", "2026-07-24T13:00:00+00:00", "ON_TIME", 80_000),
                    ("m-verde", "2026-07-24T13:01:00+00:00", "ON_TIME", 90_000),
                ]
            ),
        )


# --------------------------------------------------------------------------- #
# Sink: casos adicionales
# --------------------------------------------------------------------------- #


def test_idempotency_key_ignores_total_so_revisions_overwrite(solution):
    on_time = {"merchant_id": "m-a", "window_start": "2026-07-24T13:00:00+00:00", "total": 10}
    revision = {**on_time, "total": 30}

    assert solution.make_idempotency_key(on_time) == solution.make_idempotency_key(revision)

    materialized, _ = solution.simulate_sink_retries([on_time, revision], attempts=2)
    assert [row["total"] for row in materialized] == [30]


def test_four_results_two_attempts(solution):
    totals, _ = solution.summarize_payments(load_events())

    upserted, upsert_audit = solution.simulate_sink_retries(totals, attempts=2, idempotent=True)
    appended, append_audit = solution.simulate_sink_retries(totals, attempts=2, idempotent=False)

    assert (len(upsert_audit), len(upserted)) == (8, 4)
    assert (len(append_audit), len(appended)) == (8, 8)
    assert sum(row["total"] for row in upserted) == sum(row["total"] for row in totals)
    assert sum(row["total"] for row in appended) == 2 * sum(row["total"] for row in totals)
