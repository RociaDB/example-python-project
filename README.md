# erp-example

A small ERP — **quotes, orders, invoices, stock, customers, suppliers** —
built on [`rociadb-sdk`](https://pypi.org/project/rociadb-sdk/), the RociaDB
Python SDK.

The business logic is deliberately plain. The point is to use every part of
the SDK once, at the place where it is the right tool.

## Running it

```bash
uv sync

ROCIA_NO_AUTH=1 uv run erp-example
```

Python 3.10 through 3.14, the range the SDK supports. There is nothing to
install beyond `uv sync`: the SDK
ships its generated protobuf stubs with it, so no `protoc` is needed. Without
[uv](https://docs.astral.sh/uv/), `pip install -e .` puts the same
`erp-example` command on your path.

With no RociaDB listening on `127.0.0.1:50051` it stops on a clear message
rather than a traceback, and exits 1.

| Variable | Default | Meaning |
|---|---|---|
| `ROCIA_HOST` | `http://127.0.0.1:50051` | host and port, **no path** |
| `ROCIA_TENANT` | `demo` | business partition to write to |
| `ROCIA_NO_AUTH` | unset | set it to skip authentication (local dev) |
| `ROCIA_CLEANUP` | unset | set it to delete the demo data at the end |
| `AUTH_TOKEN_URL`, `AUTH_CLIENT_ID`, `AUTH_CLIENT_SECRET` | — | OAuth2 credentials |

Leave `ROCIA_NO_AUTH` unset and the SDK reads the three `AUTH_*` variables
itself — that is the builder's default behaviour. Set all three and the
example also runs a short tour of the `rociadb_sdk.auth` helpers on their own.

## What it does

```
customer ──requested──▶ quote ──converted_to──▶ order ──billed_as──▶ invoice

supplier ──supplies──▶ product
```

Each business record is a **document** (the source of truth: lines, totals,
status) plus a **graph node** (an index for navigation). Shipments and
deliveries write `stock_moves` documents and update the product's stock; the
invoice text and the stock export go to the **file service**.

The program runs eight steps in order, printing what each SDK call returned:
seed, catalogue queries, the sales flow, attachments, graph traversals,
deployment exploration, error handling, and optional cleanup.

## Layout

Six files, one per SDK service area plus the context they share:

| File | What it covers |
|---|---|
| [`model.py`](src/erp_example/model.py) | business types, VAT and totals, the only logic testable without a server |
| [`erp.py`](src/erp_example/erp.py) | the client, the tenant, the idempotency-key prefix, the graph and bucket names |
| [`documents.py`](src/erp_example/documents.py) | write, read, search, query, delete documents |
| [`graph.py`](src/erp_example/graph.py) | nodes and edges, single and batched, traversals both ways |
| [`files.py`](src/erp_example/files.py) | the three uploads, the two downloads, the wire contract |
| [`main.py`](src/erp_example/main.py) | the builder, tenants, token lifecycle, error handling, the demo |

`erp.py` exists so that nothing imports `main.py`: the four service modules all
need the client and the tenant, and `main.py` imports all four, so holding that
context in the entry point would put a cycle through every one of them —
and Python resolves a cycle like that by handing out a half-initialised
module, an `ImportError` on a name that is plainly defined.

Money is stored in cents and VAT rates in basis points, so no rounding
depends on the order of operations. Cents are a plain `int`, and there is
nothing else to reach for: Python integers are arbitrary-precision, so the
same type covers a line total and the `uint64` counts the SDK hands back.

## Things worth knowing about RociaDB

**Nothing is declared.** A collection, a graph or a bucket exists from the
first write to it. That is convenient, and it is why names live in constants:
a typo does not fail, it silently creates one more collection.

**The document is the source of truth; the graph is an index.** No RPC reads
an edge's value back — `neighbors_out` returns a `node_id` and an `edge_id`,
nothing else. Product nodes are written *enriched* so a traversal can show a
name without another `get_document`.

**No two writes are atomic.** `create_document` writes the document and then
the node with no transaction between them: if the second fails, the document
is left unbound, which is why the example shows the repair. Ordering is a
choice every time — `move_stock` updates the stock *before* writing its
trace, because an up-to-date stock with no trace is easier to reconcile than
the reverse.

**Deletes do not all behave the same.** `delete_document` and `delete_file`
are idempotent; `delete_edge` returns `NOT_FOUND` on a missing edge; and
**nothing deletes a node** — there is no RPC for it, so an orphaned node stays
listed by `list_nodes`.

**Only the cursor ends a paginated walk.** A short page, or even an empty
one, is not the end: an index entry can briefly outlive the document it
points to. Loop while `next_cursor` is not `None`, and expect one extra empty
page when the total is an exact multiple of the limit.

**Idempotency keys are a design choice.** The server deduplicates on
`(tenant_id, operation, request_id)` for 24 hours. A stable key is what makes
an interrupted import safe to replay — but if this demo reused the same keys
every run, a second run after a cleanup would write nothing at all. So the
prefix changes per run and the key is stable within one.

**`tenant_id` is a business partition, not a security boundary.** It is
derived from no identity: any authenticated client can address any tenant.
Deciding who may touch what is the application's job.

**Only one error is worth retrying.** `UNAUTHENTICATED` is temporary (refresh
the token, replay); `PERMISSION_DENIED` is final (the token is valid but
lacks the scope). Everything else is in `code` and `reason`.

## Things worth knowing in Python

**All of it is `async`.** The SDK is built on `grpc.aio`, so every RPC is a
coroutine and `asyncio.run` is what starts the loop. `close()` is awaited
too — closing a channel is itself asynchronous — and belongs in a `finally`
so a failed step still releases the four channels and the cached token.
`download_file_stream` is the one call you do *not* await: it is an async
generator function, so `async for` drives it directly.

**A decoder is a callable, not a claim.** `get_document(..., decoder=...)`
hands the parsed JSON to a function of yours and returns whatever it builds;
omit it and you get the raw `dict` back, which is the honest answer when the
shape is unknown. The SDK validates nothing beyond "is this valid JSON" —
unparseable JSON raises `RociaDbDecodeError` before your decoder is ever
called, and a decoder that rejects an unexpected shape is your check, not
the SDK's.

**A dataclass is not JSON.** `asdict` on the way out recurses into the nested
`Line` and `Totals`, which is exactly what a decoder has to undo on the way
back — that is all `Invoice.from_json` is. `dataclasses.replace` is the frozen
dataclass's answer to a spread: one changed field, everything else copied.

**`from` is a reserved word**, so `add_edge` takes `from_id` and `to_id`.
`limit`, `cursor`, `request_id` and `decoder` are all keyword-only, which is
why the calls here read the way they do.

**Do not translate upload method names by ear.** `upload_file_chunked` is the
assisted middle tier — it is TypeScript's `uploadFileStream`, **not** Rust's
`upload_file_stream`. `upload_file_raw` is the zero-validation escape hatch,
and *that* is Rust's `upload_file_stream`. The two near-identical names are
opposite tiers.

**A checksum is 32 raw bytes, never hex.** `hashlib.sha256(...).digest()`
gives exactly that; `stat_file` hands the same 32 bytes back, so `.hex()` them
for display.

**`RociaDbError` is a class hierarchy, not one class with a discriminant.**
`except RociaDbError` catches every SDK failure; `except RociaDbStatusError`
narrows to a server answer, the only subclass that carries `code`, `reason`
and the untouched `grpc_error`. `is_unauthenticated()` and
`is_permission_denied()` are defined on the base, so either is safe on any
caught error without checking the type first. The cost of a hierarchy is that
mypy cannot prove an `isinstance` chain exhaustive the way `tsc` proves a
switch over a closed union, so `advice()` needs a fallback arm — a seventh
subclass would land there silently rather than fail to type-check.

**A background refresh task is yours to close.** Standalone, `TokenManager`
renews in a task you start with `spawn_refresh` and stop with `aclose()`, in a
`finally` — `build()` does both for the client's own manager. Do not close the
handle in the same breath as `request_refresh()`: cancelling the task while
waking it can leave `aclose()` waiting a whole refresh interval.

**`grpcio` ships no type information**, so `types-grpcio` is a dev dependency
— it is what lets `mypy --strict` see the `grpc.StatusCode` that `main.py`
names.

## Development

```bash
uv run pytest -q
uv run mypy
uv run ruff check .
uv run ruff format --check .
```

The six unit tests are deterministic and need no server: VAT and totals, and
the node/edge id conventions. The demo itself needs a running RociaDB.

## Licence

Apache-2.0, like the SDK.
