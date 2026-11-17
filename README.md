# Tarea 3 — Beam avanzado

Solución de la Tarea 3 de **Streaming de datos y sus aplicaciones**: un pipeline
de pagos que produce el total confirmado por comercio y minuto con tiempo de
evento, ventanas, estado por clave y una salida idempotente.

Todo el código de la solución está en `notebook.py` (notebook Marimo). Los tests
cargan las funciones directamente desde ese archivo.

## Cómo reproducir

### Con uv

```bash
uv sync --frozen
uv run pytest                              # 27 pruebas
uv run ruff check notebook.py
uv run marimo check --strict notebook.py
uv run marimo edit notebook.py             # abrir el notebook
```

### Con Docker

```bash
docker compose up --build notebook         # http://localhost:2718
docker compose exec notebook uv run pytest
```

## Qué se implementó

| Pieza | Función | Resumen |
|---|---|---|
| Tiempo de evento | `parse_utc` | ISO-8601 terminado en `Z` → `datetime` aware en UTC; cualquier otro valor lanza `ValueError` con el valor recibido. |
| Ventanas | `assign_fixed_window` | Ventana fija `[inicio, fin)` alineada al epoch, igual que `FixedWindows`. |
| Oráculo | `summarize_payments` | Totales y auditoría en Python puro; sirve de referencia para Beam. |
| Pipeline batch | `build_windowed_totals_pipeline` | `Create → TimestampedValue → Filter → WindowInto → clave por comercio → deduplicación → CombinePerKey → WindowParam`. |
| Estado y timer | `DeduplicatePayments` | `SetState` de `event_id` por comercio y ventana; timer de event time que lo limpia. |
| Triggers | `build_trigger_policy` | `AfterWatermark(early=AfterProcessingTime(10), late=AfterCount(1))`, `ACCUMULATING`, lateness configurable. |
| Sink | `make_idempotency_key`, `simulate_sink_retries` | Clave `merchant_id\|window_start`; `UPSERT` (dict) frente a `POST` append-only (list). |

## Resultados sobre `data/payments.jsonl`

Con la configuración por defecto (ventanas de 60 s, lateness de 120 s) **entran
9 eventos, se aceptan 5 y se producen 4 totales**:

| Comercio | Ventana (UTC) | Total |
|---|---|---:|
| m-azul | 13:00 – 13:01 | 170.000 |
| m-azul | 13:02 – 13:03 | 200.000 |
| m-verde | 13:00 – 13:01 | 80.000 |
| m-verde | 13:01 – 13:02 | 90.000 |

Los 4 eventos no aceptados quedan auditados con su razón:

| Evento | Razón | Detalle |
|---|---|---|
| p-003 | `not_confirmed` | estado `PENDING` |
| p-008 | `not_confirmed` | estado `REJECTED` |
| p-002 (2.ª llegada) | `duplicate` | mismo `event_id` en el mismo comercio |
| p-007 | `too_late` | 169 s de atraso, supera los 120 s |

`p-004` llega 53 s tarde, con su ventana ya cerrada: se acepta y queda marcado
como `revision`. En streaming produce un pane `LATE` acumulativo que corrige el
total de m-azul 13:00 de 120.000 a 170.000.

## Evidencia de ejecución

- [`evidencia/pytest.txt`](evidencia/pytest.txt): salida de `uv run pytest -v`
  (27 pruebas aprobadas).
- [`evidencia/ejecucion.txt`](evidencia/ejecucion.txt): totales, auditoría, panes
  de streaming y sink, tal como los calcula el notebook.
- [`evidencia/notebook.html`](evidencia/notebook.html): notebook ejecutado y
  exportado (`uv run marimo export html notebook.py -o evidencia/notebook.html`).
- La sección 6 del notebook vuelve a ejecutar todo al abrirlo.

## Pruebas

`tests/test_assignment.py` es la suite provista por la cátedra (13 pruebas, sin
modificar). `tests/test_streaming.py` agrega 14 pruebas propias:

| Garantía | Prueba |
|---|---|
| Un duplicado no modifica el total | `test_duplicate_does_not_change_total`, `test_stream_late_duplicate_does_not_produce_a_revision` |
| Claves distintas no comparten estado | `test_stateful_dofn_keeps_keys_isolated`, `test_deduplication_is_isolated_by_merchant` |
| Un evento fuera de orden cae en su ventana de evento | `test_out_of_order_event_uses_its_event_time_window` |
| Un atraso aceptado produce una revisión | `test_late_event_within_tolerance_is_a_revision`, `test_stream_late_event_within_tolerance_revises_the_total` (`TestStream`) |
| Un evento demasiado tardío queda auditado / descartado | `test_event_beyond_lateness_is_audited`, `test_stream_event_beyond_allowed_lateness_is_dropped` (`TestStream`) |
| Dos escrituras dejan una sola entidad | `test_retries_converge_to_one_materialized_entity`, `test_four_results_two_attempts` |
| El timer limpia el estado | `test_timer_handler_clears_state` |
| Streaming y oráculo coinciden | `test_stream_replay_of_dataset_converges_to_the_oracle` (`TestStream`) |

## Decisiones

- **Tiempo de evento.** La ventana depende solo de `event_time`. `arrival_time`
  se usa únicamente para medir el atraso y ordenar la llegada.
- **Orden de las reglas.** Estado → duplicado → atraso. Es el mismo orden del
  pipeline (filtro, deduplicación con estado, descarte por lateness al agrupar).
- **Estado por clave.** El estado de Beam es por clave y ventana; la clave es
  `merchant_id`, así que dos comercios pueden repetir un `event_id`.
- **Expiración.** El timer se fija en `fin de ventana + lateness`. Sin él, cada
  `event_id` visto quedaría guardado para siempre: en un stream no acotado el
  estado crece linealmente con los eventos (más memoria, checkpoints más
  pesados) aunque esas ventanas ya no puedan recibir datos aceptables.
- **Panes acumulativos.** Cada pane lleva el total completo de la ventana. Junto
  con la clave `merchant_id|window_start`, que no incluye el total, permite que
  reintentos y revisiones late sobrescriban la misma fila.

## Trade-offs

- **Lateness.** Más tolerancia da totales más completos, pero retiene estado más
  tiempo y demora el resultado definitivo. Con 180 s, `p-007` se acepta y
  m-verde 13:00 pasa a 110.000.
- **`ACCUMULATING` exige un sink que sobrescriba.** Con un sink que suma habría
  que emitir deltas (`DISCARDING`) y se perdería la idempotencia simple.
- **Oráculo y watermark no son la misma regla.** El oráculo marca `too_late` si
  `arrival_time - event_time` supera la tolerancia; Beam descarta cuando el
  watermark supera `fin de ventana + lateness`. Coinciden en este dataset, pero
  un evento a mitad de ventana con 130 s de atraso es `too_late` para el oráculo
  y Beam todavía lo acepta.
- **Batch no tiene datos tardíos.** El pipeline batch incluye `p-007` (m-verde
  13:00 = 110.000); el descarte por atraso solo se observa en streaming.
- **Costo de la deduplicación.** Se lee el conjunto completo por elemento: simple
  y suficiente con pocos ids por comercio y ventana; a gran escala convendría
  estado por `event_id`.

## Nota sobre una prueba provista

`test_trigger_policy_has_lateness_and_accumulating_panes` lee
`windowfn.size.seconds` y `allowed_lateness.seconds`, pero la clase `Duration` de
Beam 2.74 solo expone `micros`. Para aprobarla sin tocar la prueba,
`build_trigger_policy` usa una subclase mínima de `Duration` con la propiedad
`seconds`. No cambia el comportamiento del pipeline.

No se modificó `data/payments.jsonl`.
