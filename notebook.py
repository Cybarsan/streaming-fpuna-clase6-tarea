import marimo

__generated_with = "0.23.15"
app = marimo.App(width="full")


@app.cell
def _():
    from collections.abc import Iterable
    from datetime import datetime
    from typing import Any

    import apache_beam as beam
    import marimo as mo
    from apache_beam.coders import StrUtf8Coder
    from apache_beam.transforms.timeutil import TimeDomain
    from apache_beam.transforms.userstate import (
        SetStateSpec,
        TimerSpec,
        on_timer,
    )

    return (
        Any,
        Iterable,
        SetStateSpec,
        StrUtf8Coder,
        TimeDomain,
        TimerSpec,
        beam,
        datetime,
        mo,
        on_timer,
    )


@app.cell
def _(mo):
    mo.md(r"""
    # Tarea 3 · Beam avanzado

    **Ventanas, estado por clave y efectos externos idempotentes**

    Este notebook contiene la solución completa: cada celda de código
    implementa el contrato de su firma. La evidencia de ejecución está en la
    sección 6 y las decisiones en la sección 7.

    ## Problema

    Implementá un pipeline que produzca el total confirmado por comercio y
    minuto aun cuando los pagos lleguen fuera de orden, duplicados o sean
    reintentados al escribir el resultado.

    El archivo `data/payments.jsonl` contiene:

    - eventos `CONFIRMED`, `PENDING` y `REJECTED`;
    - un `event_id` duplicado;
    - eventos fuera de orden;
    - un evento que supera 120 segundos de atraso.

    ## Reglas

    1. Usar `event_time` como timestamp del dominio.
    2. Aplicar ventanas fijas de 60 segundos.
    3. Aceptar hasta 120 segundos de lateness.
    4. Deduplicar por `event_id` dentro del comercio.
    5. Emitir panes acumulativos.
    6. Escribir mediante una clave idempotente `merchant_id|window_start`.
    """)
    return


@app.cell
def _(datetime):
    def parse_utc(raw_value: str) -> datetime:
        """Convertir un timestamp ISO-8601 terminado en Z a datetime UTC."""
        from datetime import UTC

        message = (
            f"Timestamp inválido {raw_value!r}: se espera ISO-8601 en UTC "
            "terminado en 'Z', por ejemplo 2026-07-24T13:00:05Z"
        )
        if not isinstance(raw_value, str) or not raw_value.endswith("Z"):
            raise ValueError(message)
        try:
            parsed = datetime.fromisoformat(raw_value[:-1])
        except ValueError as error:
            raise ValueError(message) from error
        if parsed.tzinfo is not None:
            # Algo como "...+02:00Z" mezcla dos zonas horarias.
            raise ValueError(message)
        return parsed.replace(tzinfo=UTC)

    return (parse_utc,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 1. Tiempo de evento

    Completá `parse_utc`.

    El resultado debe:

    - ser timezone-aware;
    - aceptar los timestamps del dataset;
    - rechazar valores inválidos con una excepción clara.

    Después, usá esa función cuando construyas cada `TimestampedValue`.
    """)
    return


@app.cell
def _(datetime):
    def assign_fixed_window(
        timestamp: datetime,
        size_seconds: int = 60,
    ) -> tuple[datetime, datetime]:
        """Retornar los límites [inicio, fin) de la ventana fija."""
        from datetime import UTC, timedelta

        if timestamp.tzinfo is None:
            raise ValueError("assign_fixed_window requiere un datetime timezone-aware")
        if size_seconds <= 0:
            raise ValueError("size_seconds debe ser positivo")

        # Las ventanas se alinean al epoch Unix, igual que FixedWindows de Beam.
        # Se opera con timedelta (enteros) para no perder precisión con floats.
        epoch = datetime(1970, 1, 1, tzinfo=UTC)
        offset = (timestamp - epoch) % timedelta(seconds=size_seconds)
        start = timestamp - offset
        return start, start + timedelta(seconds=size_seconds)

    return (assign_fixed_window,)


@app.cell
def _(Any, Iterable, assign_fixed_window, parse_utc):
    def summarize_payments(
        events: Iterable[dict[str, Any]],
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
        deduplicate: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Crear totales deterministas y una auditoría de cada evento.

        Retornar `(totals, audit)`.

        Cada fila de `totals` debe contener `merchant_id`, `window_start`,
        `window_end` y `total`; los límites de ventana se expresan como strings
        ISO-8601.

        Cada fila de `audit` debe contener `event_id`, `merchant_id`,
        `delay_seconds`, `duplicate`, `too_late`, `accepted`, `revision` y
        `reason`. `revision` es verdadero cuando un evento aceptado llega
        después del cierre de su ventana.
        """
        # Orden de las decisiones (el mismo que sigue el pipeline de Beam):
        #   1. estado != CONFIRMED            -> reason="not_confirmed"
        #   2. event_id ya visto en el comercio -> reason="duplicate"
        #   3. atraso > lateness permitida    -> reason="too_late"
        #   4. en otro caso                   -> reason="accepted"
        # Los eventos se recorren en orden de llegada: el primero en llegar es
        # el original y los siguientes con el mismo event_id son duplicados.
        ordered = sorted(events, key=lambda event: parse_utc(event["arrival_time"]))

        seen_ids: set[tuple[str, str]] = set()
        sums: dict[tuple[str, Any], int] = {}
        windows: dict[tuple[str, Any], Any] = {}
        audit: list[dict[str, Any]] = []

        for event in ordered:
            merchant_id = event["merchant_id"]
            event_time = parse_utc(event["event_time"])
            arrival_time = parse_utc(event["arrival_time"])
            window_start, window_end = assign_fixed_window(event_time, window_seconds)
            delay_seconds = (arrival_time - event_time).total_seconds()

            confirmed = event["status"] == "CONFIRMED"
            dedup_key = (merchant_id, event["event_id"])
            duplicate = confirmed and deduplicate and dedup_key in seen_ids
            too_late = delay_seconds > allowed_lateness_seconds

            if not confirmed:
                reason = "not_confirmed"
            elif duplicate:
                reason = "duplicate"
            elif too_late:
                reason = "too_late"
            else:
                reason = "accepted"
            accepted = reason == "accepted"

            if confirmed:
                seen_ids.add(dedup_key)
            if accepted:
                total_key = (merchant_id, window_start)
                sums[total_key] = sums.get(total_key, 0) + event["amount"]
                windows[total_key] = window_end

            audit.append(
                {
                    "event_id": event["event_id"],
                    "merchant_id": merchant_id,
                    "delay_seconds": delay_seconds,
                    "duplicate": duplicate,
                    "too_late": too_late,
                    "accepted": accepted,
                    # Llegó con la ventana ya cerrada: corrige un total ya emitido.
                    "revision": accepted and arrival_time >= window_end,
                    "reason": reason,
                }
            )

        totals = [
            {
                "merchant_id": merchant_id,
                "window_start": window_start.isoformat(),
                "window_end": windows[(merchant_id, window_start)].isoformat(),
                "total": total,
            }
            for (merchant_id, window_start), total in sorted(sums.items())
        ]
        return totals, audit

    return (summarize_payments,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 2. Contrato determinista antes de Beam

    Implementá `assign_fixed_window` y `summarize_payments`.

    Esta versión pura de Python funciona como oráculo para el pipeline:

    - solo cuenta pagos `CONFIRMED`;
    - la ventana depende de `event_time`;
    - un duplicado no cambia el total;
    - el atraso se calcula con `arrival_time - event_time`;
    - la auditoría conserva la razón de cada decisión;
    - un late aceptado tiene `accepted=True` y `revision=True`;
    - un evento fuera de tolerancia tiene `reason="too_late"`.

    Para la configuración por defecto, documentá cuántos eventos entran,
    cuántos se aceptan y cuántos totales se producen.
    """)
    return


@app.cell
def _(Any, DeduplicatePayments, beam, datetime, parse_utc):
    def build_windowed_totals_pipeline(
        pipeline: Any,
        events: list[dict[str, Any]],
        *,
        window_seconds: int = 60,
    ) -> Any:
        """Construir y retornar la PCollection de totales por ventana.

        Usar Create, TimestampedValue, Filter, WindowInto, una clave por
        comercio, CombinePerKey y metadatos de WindowParam.
        """
        from datetime import UTC

        from apache_beam.transforms.window import FixedWindows, TimestampedValue

        def with_event_time(event: dict[str, Any]) -> Any:
            # El timestamp del elemento es el event_time, no la llegada.
            return TimestampedValue(event, parse_utc(event["event_time"]).timestamp())

        def window_bound(timestamp: Any) -> str:
            return datetime.fromtimestamp(float(timestamp), tz=UTC).isoformat()

        class FormatTotal(beam.DoFn):
            def process(self, element, window=beam.DoFn.WindowParam):
                merchant_id, total = element
                yield {
                    "merchant_id": merchant_id,
                    "window_start": window_bound(window.start),
                    "window_end": window_bound(window.end),
                    "total": total,
                }

        return (
            pipeline
            | "CrearEventos" >> beam.Create(events)
            | "TiempoDeEvento" >> beam.Map(with_event_time)
            | "SoloConfirmados" >> beam.Filter(lambda event: event["status"] == "CONFIRMED")
            | "VentanasFijas" >> beam.WindowInto(FixedWindows(window_seconds))
            | "ClavePorComercio"
            >> beam.Map(lambda event: (event["merchant_id"], event)).with_output_types(
                tuple[str, dict]
            )
            | "Deduplicar" >> beam.ParDo(DeduplicatePayments())
            | "ExtraerMonto"
            >> beam.MapTuple(lambda merchant_id, event: (merchant_id, event["amount"]))
            | "SumarPorComercio" >> beam.CombinePerKey(sum)
            | "MetadatosDeVentana" >> beam.ParDo(FormatTotal())
        )

    return (build_windowed_totals_pipeline,)


@app.cell
def _(
    Any,
    SetStateSpec,
    StrUtf8Coder,
    TimeDomain,
    TimerSpec,
    beam,
    on_timer,
):
    class DeduplicatePayments(beam.DoFn):
        """Eliminar event_id repetidos dentro de cada clave de comercio."""

        SEEN_IDS = SetStateSpec("seen_ids", StrUtf8Coder())
        EXPIRY = TimerSpec("expiry", TimeDomain.WATERMARK)

        def __init__(self, allowed_lateness_seconds: int = 120):
            self.allowed_lateness_seconds = allowed_lateness_seconds

        def process(
            self,
            element: tuple[str, dict[str, Any]],
            seen_ids=beam.DoFn.StateParam(SEEN_IDS),
            window=beam.DoFn.WindowParam,
            expiry=beam.DoFn.TimerParam(EXPIRY),
        ):
            """Emitir el elemento completo solo en su primera aparición."""
            # El estado es por clave (merchant_id) y por ventana: dos comercios
            # con el mismo event_id nunca comparten el conjunto.
            _, event = element
            event_id = event["event_id"]
            if event_id in set(seen_ids.read()):
                return
            seen_ids.add(event_id)
            # Pasada la ventana + lateness ya no puede llegar un duplicado que
            # el pipeline acepte, así que el conjunto deja de ser necesario.
            expiry.set(window.end + self.allowed_lateness_seconds)
            yield element

        @on_timer(EXPIRY)
        def expire(self, seen_ids=beam.DoFn.StateParam(SEEN_IDS)):
            """Limpiar el estado cuando vence el timer de event time."""
            seen_ids.clear()

    return (DeduplicatePayments,)


@app.cell
def _(Any, beam):
    def build_trigger_policy(
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
    ) -> Any:
        """Crear la transformación WindowInto para streaming.

        Configurar un pane on-time por watermark, una estimación early por
        processing time, revisiones late y modo ACCUMULATING.
        """
        from apache_beam.transforms import trigger
        from apache_beam.transforms.window import FixedWindows
        from apache_beam.utils.timestamp import Duration

        class SecondsDuration(Duration):
            """Duration de Beam que además expone `.seconds`.

            La prueba provista lee `windowfn.size.seconds` y
            `allowed_lateness.seconds`, pero `Duration` (Beam 2.74) solo
            guarda `micros`. Beam conserva la instancia tal cual
            (`Duration.of` no copia), así que la subclase alcanza y el
            comportamiento del pipeline no cambia.
            """

            @property
            def seconds(self) -> int:
                return self.micros // 1_000_000

        return beam.WindowInto(
            FixedWindows(SecondsDuration(window_seconds)),
            trigger=trigger.AfterWatermark(
                # Estimación parcial 10 s (processing time) después del primer
                # elemento del pane, mientras la ventana sigue abierta.
                early=trigger.AfterProcessingTime(10),
                # Una revisión por cada elemento tardío aceptado.
                late=trigger.AfterCount(1),
            ),
            # Cada pane contiene el total completo de la ventana, de modo que
            # el sink puede sobrescribir (UPSERT) en lugar de sumar deltas.
            accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
            allowed_lateness=SecondsDuration(allowed_lateness_seconds),
        )

    return (build_trigger_policy,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 3. Pipeline Beam, estado y triggers

    Completá:

    - `build_windowed_totals_pipeline`;
    - `DeduplicatePayments.process`;
    - `build_trigger_policy`.

    La clave debe ser `merchant_id` antes de usar estado. La salida debe
    recuperar los límites de ventana con `WindowParam`.

    Agregá pruebas con `TestPipeline` y al menos una prueba temporal con
    `TestStream` que evidencie un resultado late aceptado.

    ### Expiración

    Extendé la deduplicación con un timer de event time que limpie el estado
    al finalizar la ventana más la lateness permitida. Explicá por qué un
    estado sin expiración crece indefinidamente.
    """)
    return


@app.cell
def _(Any):
    def make_idempotency_key(result: dict[str, Any]) -> str:
        """Construir merchant_id|window_start para un resultado lógico."""
        # La clave identifica el resultado lógico (comercio + ventana) y no
        # incluye el total: una revisión late reemplaza al pane anterior.
        return f"{result['merchant_id']}|{result['window_start']}"

    def simulate_sink_retries(
        results: list[dict[str, Any]],
        *,
        attempts: int = 2,
        idempotent: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Simular intentos de escritura y retornar `(materialized, audit)`.

        En modo idempotente, múltiples intentos del mismo resultado deben dejar
        una sola fila materializada. En modo append, cada intento agrega una.
        """
        append_sink: list[dict[str, Any]] = []
        upsert_sink: dict[str, dict[str, Any]] = {}
        audit: list[dict[str, Any]] = []
        operation = "UPSERT" if idempotent else "POST"

        # Cada intento reenvía el lote completo, como hace un runner cuando
        # reintenta un bundle cuya confirmación se perdió.
        for attempt in range(1, attempts + 1):
            for result in results:
                key = make_idempotency_key(result)
                row = {**result, "idempotency_key": key}
                if idempotent:
                    upsert_sink[key] = row
                    materialized_rows = len(upsert_sink)
                else:
                    append_sink.append(row)
                    materialized_rows = len(append_sink)
                audit.append(
                    {
                        "attempt": attempt,
                        "operation": operation,
                        "idempotency_key": key,
                        "total": result["total"],
                        "materialized_rows": materialized_rows,
                    }
                )

        materialized = list(upsert_sink.values()) if idempotent else append_sink
        return materialized, audit

    return make_idempotency_key, simulate_sink_retries


@app.cell
def _(mo):
    mo.md(r"""
    ## 4. Efectos externos

    Completá `make_idempotency_key` y `simulate_sink_retries`.

    En este ejercicio los sinks **no son servicios externos reales**. Son
    estructuras Python en memoria que representan dos contratos de escritura:

    | Modo simulado | Estructura interna | Operación |
    |---|---|---|
    | `POST` append-only | `list` | `append(row)` en cada intento |
    | `UPSERT` idempotente | `dict` | `sink[idempotency_key] = row` |

    `simulate_sink_retries` siempre retorna dos **listas**:

    1. `materialized`: estado final visible del sink;
    2. `audit`: todos los intentos realizados.

    En modo append-only, `materialized` contiene una fila por intento. En modo
    idempotente, se usa internamente un diccionario y al final se retornan
    `list(upsert_sink.values())`.

    Para cuatro resultados y dos intentos existen ocho filas de auditoría. El
    modo append-only materializa ocho filas; el UPSERT materializa cuatro
    porque el segundo intento reemplaza la misma clave lógica.

    ## 5. Pruebas obligatorias

    El proyecto ya incluye los tests. Ejecutalos con:

    ```bash
    uv run pytest
    ```

    Garantías cubiertas por la suite (todas en verde):

    - [x] un duplicado no modifica el total;
    - [x] claves distintas no comparten estado;
    - [x] un evento fuera de orden cae en su ventana de evento;
    - [x] un evento con atraso aceptado produce una revisión;
    - [x] un evento demasiado tardío queda auditado;
    - [x] dos escrituras del mismo resultado dejan una sola entidad;
    - [x] el timer limpia el estado cuando corresponde.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 6. Evidencia de ejecución

    Las celdas siguientes ejecutan la solución sobre `data/payments.jsonl`:

    1. el oráculo determinista (`summarize_payments`);
    2. el pipeline Beam en batch (`build_windowed_totals_pipeline`);
    3. una reproducción en streaming con `TestStream`, en orden de llegada y
       con watermark igual a `arrival_time`, usando `build_trigger_policy` y
       `DeduplicatePayments`;
    4. los dos contratos de sink ante un reintento.
    """)
    return


@app.cell
def _(mo):
    import json

    events = [
        json.loads(line)
        for line in (mo.notebook_dir() / "data" / "payments.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip()
    ]
    return events, json


@app.cell
def _(DeduplicatePayments, beam, build_trigger_policy, datetime, json, parse_utc):
    def replay_stream(
        events: list[dict],
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
    ) -> list[dict]:
        """Reproducir los eventos en orden de llegada y retornar cada pane."""
        import tempfile
        from datetime import UTC
        from pathlib import Path

        from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
        from apache_beam.testing.test_stream import TestStream
        from apache_beam.transforms.window import TimestampedValue

        timing_names = {0: "EARLY", 1: "ON_TIME", 2: "LATE", 3: "UNKNOWN"}

        def seconds(raw_value: str) -> float:
            return parse_utc(raw_value).timestamp()

        # El watermark avanza con la llegada: un evento cuyo event_time quedó
        # por detrás del watermark es, por definición, un evento late.
        ordered = sorted(events, key=lambda event: seconds(event["arrival_time"]))
        stream = TestStream().advance_watermark_to(seconds(ordered[0]["event_time"]))
        for event in ordered:
            stream = stream.advance_watermark_to(seconds(event["arrival_time"])).add_elements(
                [TimestampedValue(event, seconds(event["event_time"]))]
            )
        stream = stream.advance_watermark_to_infinity()

        with tempfile.TemporaryDirectory() as directory:
            output_path = str(Path(directory) / "panes.jsonl")

            class RecordPane(beam.DoFn):
                def process(
                    self,
                    element,
                    window=beam.DoFn.WindowParam,
                    pane=beam.DoFn.PaneInfoParam,
                ):
                    merchant_id, total = element
                    row = {
                        "merchant_id": merchant_id,
                        "window_start": datetime.fromtimestamp(
                            float(window.start), tz=UTC
                        ).isoformat(),
                        "window_end": datetime.fromtimestamp(float(window.end), tz=UTC).isoformat(),
                        "pane_timing": timing_names[pane.timing],
                        "pane_index": pane.index,
                        "total": total,
                    }
                    # El DirectRunner corre en este proceso: un archivo local
                    # alcanza para recuperar los panes en el notebook.
                    with open(output_path, "a", encoding="utf-8") as output:
                        output.write(json.dumps(row) + "\n")

            options = PipelineOptions()
            options.view_as(StandardOptions).streaming = True
            with beam.Pipeline(options=options) as pipeline:
                _ = (
                    pipeline
                    | stream
                    | "Confirmados" >> beam.Filter(lambda event: event["status"] == "CONFIRMED")
                    | "VentanasYTriggers"
                    >> build_trigger_policy(
                        window_seconds=window_seconds,
                        allowed_lateness_seconds=allowed_lateness_seconds,
                    )
                    | "ClavePorComercio"
                    >> beam.Map(lambda event: (event["merchant_id"], event)).with_output_types(
                        tuple[str, dict]
                    )
                    | "Deduplicar" >> beam.ParDo(DeduplicatePayments(allowed_lateness_seconds))
                    | "Monto"
                    >> beam.MapTuple(lambda merchant_id, event: (merchant_id, event["amount"]))
                    | "Sumar" >> beam.CombinePerKey(sum)
                    | "RegistrarPane" >> beam.ParDo(RecordPane())
                )

            lines = Path(output_path).read_text(encoding="utf-8").splitlines()

        panes = [json.loads(line) for line in lines]
        return sorted(
            panes,
            key=lambda row: (row["merchant_id"], row["window_start"], row["pane_index"]),
        )

    return (replay_stream,)


@app.cell
def _(
    beam,
    build_windowed_totals_pipeline,
    events,
    json,
    replay_stream,
    simulate_sink_retries,
    summarize_payments,
):
    import tempfile
    from pathlib import Path as _Path

    oracle_totals, oracle_audit = summarize_payments(events)

    # Pipeline batch: se materializa en un archivo temporal para mostrarlo.
    with tempfile.TemporaryDirectory() as _directory:
        with beam.Pipeline() as _pipeline:
            _ = (
                build_windowed_totals_pipeline(_pipeline, events)
                | "AJson" >> beam.Map(json.dumps)
                | "Escribir" >> beam.io.WriteToText(str(_Path(_directory) / "totals"), num_shards=1)
            )
        batch_totals = sorted(
            (
                json.loads(_line)
                for _file in _Path(_directory).glob("totals*")
                for _line in _file.read_text(encoding="utf-8").splitlines()
            ),
            key=lambda row: (row["merchant_id"], row["window_start"]),
        )

    stream_panes_120 = replay_stream(events, allowed_lateness_seconds=120)
    stream_panes_180 = replay_stream(events, allowed_lateness_seconds=180)

    upsert_rows, upsert_audit = simulate_sink_retries(oracle_totals, attempts=2, idempotent=True)
    append_rows, append_audit = simulate_sink_retries(oracle_totals, attempts=2, idempotent=False)
    return (
        append_audit,
        append_rows,
        batch_totals,
        oracle_audit,
        oracle_totals,
        stream_panes_120,
        stream_panes_180,
        upsert_audit,
        upsert_rows,
    )


@app.cell
def _(
    append_audit,
    append_rows,
    batch_totals,
    events,
    mo,
    oracle_audit,
    oracle_totals,
    stream_panes_120,
    stream_panes_180,
    upsert_audit,
    upsert_rows,
):
    def _table(rows):
        return mo.ui.table(rows, selection=None, pagination=False)

    _accepted = sum(row["accepted"] for row in oracle_audit)
    _expected = sum(row["total"] for row in oracle_totals)

    mo.vstack(
        [
            mo.md(
                f"""
                ### 6.1 Oráculo (configuración por defecto)

                Entran **{len(events)}** eventos, se aceptan **{_accepted}** y se
                producen **{len(oracle_totals)}** totales.
                """
            ),
            _table(oracle_totals),
            mo.md("**Auditoría** (en orden de llegada)"),
            _table(oracle_audit),
            mo.md(
                r"""
                ### 6.2 Pipeline Beam en batch

                En batch el watermark salta de $-\infty$ a $+\infty$: no existe
                dato tardío, por eso `p-007` (169 s de atraso) **sí** entra y
                `m-verde 13:00` vale 110.000 en lugar de 80.000. El duplicado
                `p-002` se elimina igual.
                """
            ),
            _table(batch_totals),
            mo.md(
                """
                ### 6.3 Streaming con `TestStream` (lateness = 120 s)

                El último pane de cada ventana coincide con el oráculo. `p-004`
                genera una revisión `LATE` acumulativa (120.000 → 170.000); el
                duplicado tardío de `p-002` no genera pane; `p-007` llega con el
                watermark en 13:03:40, pasado el límite 13:01:00 + 120 s, y se
                descarta.
                """
            ),
            _table(stream_panes_120),
            mo.md(
                """
                **Mismo stream con lateness = 180 s**: `p-007` ahora se acepta y
                revisa `m-verde 13:00` a 110.000.
                """
            ),
            _table(stream_panes_180),
            mo.md(
                f"""
                ### 6.4 Sink con 2 intentos

                | Modo | Intentos auditados | Filas materializadas | Suma materializada |
                |---|---:|---:|---:|
                | `UPSERT` idempotente | {len(upsert_audit)} | {len(upsert_rows)} | {
                    sum(row["total"] for row in upsert_rows):,} |
                | `POST` append-only | {len(append_audit)} | {len(append_rows)} | {
                    sum(row["total"] for row in append_rows):,} |

                Suma correcta: **{_expected:,}**. El modo append la duplica.
                """
            ),
            _table(upsert_rows),
        ]
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 7. Decisiones y explicación

    - **Tiempo de evento.** `parse_utc` solo acepta ISO-8601 terminado en `Z`
      y devuelve un `datetime` aware; cada elemento entra a Beam como
      `TimestampedValue(event, event_time)`. `arrival_time` nunca decide la
      ventana: solo mide el atraso.
    - **Ventanas.** Fijas de 60 s alineadas al epoch, intervalo `[inicio, fin)`.
    - **Triggers.** `AfterWatermark(early=AfterProcessingTime(10),
      late=AfterCount(1))` en modo `ACCUMULATING`: una estimación temprana, un
      pane on-time al pasar el watermark y una revisión por cada tardío
      aceptado. Cada pane trae el total completo, así el sink sobrescribe.
    - **Estado.** `DeduplicatePayments` guarda un `SetState` de `event_id` por
      clave (`merchant_id`) y ventana; por eso dos comercios pueden repetir un
      `event_id` sin interferirse.
    - **Timer.** Un timer de event time en `fin de ventana + lateness` limpia
      el conjunto. Sin expiración, cada `event_id` visto quedaría guardado
      para siempre: en un stream no acotado el estado crece linealmente con
      los eventos (memoria, checkpoints y costo cada vez mayores) aunque esas
      ventanas ya no puedan recibir datos aceptables.
    - **Idempotencia.** La clave `merchant_id|window_start` identifica el
      resultado lógico y no incluye el total: reintentos y revisiones late
      convergen a una sola fila (`UPSERT`), mientras que un `POST` append-only
      duplica.

    ### Trade-offs

    - Más lateness = totales más completos, pero estado retenido más tiempo y
      resultados que tardan más en ser definitivos.
    - `ACCUMULATING` exige un sink que sobrescriba; con uno que sume habría
      que usar `DISCARDING` (deltas) y se perdería la idempotencia simple.
    - El oráculo mide el atraso como `arrival_time - event_time`; Beam decide
      con el watermark (`watermark > fin de ventana + lateness`). Coinciden en
      este dataset, pero no son la misma regla: un evento a mitad de ventana
      con 130 s de atraso es `too_late` para el oráculo y Beam aún lo acepta.
    - En batch no hay datos tardíos, así que el pipeline batch incluye `p-007`.
    - La deduplicación lee el conjunto completo en cada elemento: simple y
      suficiente con pocos ids por comercio y ventana; a gran escala convendría
      un estado por `event_id` (clave compuesta) o `MapState`.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Entrega

    Publicá un repositorio propio con:

    1. este notebook completamente implementado;
    2. la suite de pruebas provista ejecutada y completamente verde;
    3. README con instrucciones Docker o `uv`;
    4. explicación breve de ventanas, triggers, estado, timer e
       idempotencia;
    5. evidencia de ejecución y resultados.

    ### Criterios sugeridos

    | Criterio | Peso |
    |---|---:|
    | Contrato temporal y ventanas | 25% |
    | Estado, deduplicación y expiración | 25% |
    | Idempotencia y reintentos | 20% |
    | Pruebas y casos límite | 20% |
    | Reproducibilidad y explicación | 10% |

    Se evalúa corrección conceptual y evidencia, no complejidad innecesaria.
    """)
    return


if __name__ == "__main__":
    app.run()
