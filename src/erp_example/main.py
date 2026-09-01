"""A small ERP — quotes, orders, invoices, stock, customers, suppliers —
built on the RociaDB Python SDK.

The business side is deliberately small. The point is to use every part of the
SDK once, at the place where it is the right tool.

Run it with:

```text
ROCIA_NO_AUTH=1 uv run erp-example
```
"""

from __future__ import annotations

import asyncio
import dataclasses
import os
import sys
import time
from collections.abc import Sequence
from typing import Literal

import grpc
from rociadb_sdk import (
    RociaDbAuthError,
    RociaDbBuilder,
    RociaDbClient,
    RociaDbConnectionError,
    RociaDbDecodeError,
    RociaDbEncodeError,
    RociaDbError,
    RociaDbStatusError,
    RociaDbValidationError,
)

# `fetch_token` and `TokenManager` are not re-exported from the package root:
# they are useful independently of any client, so they stay in `rociadb_sdk.auth`.
from rociadb_sdk.auth import TokenManager, fetch_token

from . import documents, files, graph
from .erp import BUCKET, GRAPH, Erp
from .model import (
    INVOICE_ISSUED,
    INVOICE_PAID,
    ORDER_PREPARING,
    ORDER_SHIPPED,
    QUOTE_ACCEPTED,
    QUOTE_SENT,
    VAT_REDUCED,
    VAT_STANDARD,
    Customer,
    Invoice,
    Line,
    Order,
    Product,
    Quote,
    StockMove,
    Supplier,
    money,
    totals,
)

# Fixed dates keep the example readable and its output stable.
TODAY = "2026-09-01"
DUE_DATE = "2026-10-01"


async def main() -> int:
    host = os.environ.get("ROCIA_HOST", "http://127.0.0.1:50051")
    tenant = os.environ.get("ROCIA_TENANT", "demo")

    try:
        client = await build_client(host)
    except RociaDbError as error:
        fail(error)
        return 1

    erp = Erp(client=client, tenant=tenant, run=f"run-{int(time.time())}")
    print(f'Tenant "{erp.tenant}", graph "{GRAPH}", bucket "{BUCKET}"')

    try:
        await seed(erp)
        await query_catalogue(erp)
        await sell(erp)
        await attachments(erp)
        await traverse(erp)
        await explore(erp)
        await show_error_handling(erp)

        if "ROCIA_CLEANUP" in os.environ:
            await cleanup(erp)

        print("\nDone.")
        return 0
    except RociaDbError as error:
        # Only the SDK's own failures are reported this way. A bug in this
        # program is not one of them, and its traceback is worth more than a
        # one-line message, so it goes up.
        fail(error)
        return 1
    finally:
        # Release the four gRPC channels and the cached token, in a `finally`
        # so a failed step still gives them up. It is a coroutine, unlike its
        # TypeScript counterpart, because closing an `grpc.aio` channel is
        # itself asynchronous. The client must not be used afterwards.
        await client.close()


async def build_client(host: str) -> RociaDbClient:
    """Build the client.

    Authentication is on by default: without `disable_auth()`, the builder
    reads `AUTH_TOKEN_URL`, `AUTH_CLIENT_ID` and `AUTH_CLIENT_SECRET` itself at
    `build()` time. Set `ROCIA_NO_AUTH=1` for a local server.
    """
    # Every setter returns the builder, so the whole thing chains. The direct
    # `RociaDbClient.connect(host, auth_token_url=..., ...)` entry point takes
    # the same settings as keyword arguments, for callers assembling
    # configuration rather than writing it out.
    builder = RociaDbBuilder().host(host).connect_timeout(10.0)

    credentials = auth_from_environment()
    if "ROCIA_NO_AUTH" in os.environ:
        builder.disable_auth()
    elif credentials is not None:
        # Passing them explicitly is the same thing the builder would do from
        # the environment; shown here because credentials usually come from a
        # vault rather than the process environment.
        builder.auth_client_credentials(*credentials)

    # One client per upstream configuration, reused across every call: it owns
    # the gRPC channels and the cached token, and a single instance is already
    # safe to call from many `asyncio` tasks at once.
    return await builder.build()


async def seed(erp: Erp) -> None:
    """Step 1: customers, suppliers, products, and who supplies what."""
    step("1. Customers, suppliers and catalogue")

    for customer in demo_customers():
        await documents.create_customer(erp, customer)
    for supplier in demo_suppliers():
        await documents.create_supplier(erp, supplier)
    print("   3 customers and 2 suppliers written, each with its graph node")

    products = demo_products()
    for product in products:
        await documents.import_product(erp, product)
    # Product nodes are written separately and enriched, so a traversal can
    # show a name without reading each document again.
    await graph.put_product_nodes(erp, products)
    print(f"   {len(products)} products imported")

    # A product added outside the import: `put_document` writes the document,
    # `put_node` writes its node. That is what `create_document` does in one call.
    extra = Product(
        id="P-006",
        reference="GLUE-PU",
        name="Polyurethane glue 310 ml",
        family="hardware",
        unit_price=890,
        vat_rate=VAT_STANDARD,
        stock=24,
        min_stock=10,
        active=True,
    )
    await documents.save_product(erp, extra)
    await graph.put_node(
        erp,
        graph.node("product", extra.id),
        graph.DocRef(collection=documents.PRODUCTS, id=extra.id),
    )
    print("   1 product added outside the import (put_document + put_node)")

    # `create_document` writes the document and then the node, with no
    # transaction between them: if the second write fails, the document is left
    # unbound. This is the repair — same payload, stable key, so running it
    # twice costs nothing.
    await graph.put_node_with_key(
        erp,
        graph.node("customer", "C-003"),
        graph.DocRef(collection=documents.CUSTOMERS, id="C-003"),
    )
    print("   customer C-003 node binding re-asserted (put_node with a request_id)")

    # Edges last: both endpoints must already exist as nodes.
    await graph.link_supplier_products(
        erp, "S-001", [("P-001", 780), ("P-002", 1180), ("P-003", 190)]
    )
    await graph.link_supplier_products(erp, "S-002", [("P-004", 9800), ("P-001", 830)])
    print('   "supplies" edges created')

    # A delivery from a supplier: one stock move in.
    product = await move_stock(erp, "P-005", "in", 50, "delivery from S-001")
    print(f"   received 50 x {product.name}, stock now {product.stock}")


async def query_catalogue(erp: Erp) -> None:
    """Step 2: the four ways to read documents."""
    step("2. Reading the catalogue")

    products = await documents.all_products(erp)
    value = sum(product.unit_price * product.stock for product in products)
    print(f"   list_documents: {len(products)} products, stock worth {money(value)}")

    found_products, total = await documents.search_products(erp, "hardware", "screw")
    print(
        '   query_documents (family = hardware AND name contains "screw"): '
        f"{len(found_products)} of {total}"
    )
    for product in found_products:
        print(f"     - {product.name} at {money(product.unit_price)}")

    to_reorder = await documents.products_to_reorder(erp)
    print(
        "   below minimum stock: "
        + join([f"{p.reference} ({p.stock}/{p.min_stock})" for p in to_reorder])
    )

    email = "orders@bertrand.example"
    customers = await documents.find_customer_by_email(erp, email)
    name = customers[0].name if customers else "nothing"
    print(f'   find_documents_by_field on "{email}": {name}')


async def sell(erp: Erp) -> None:
    """Step 3: quote, order, shipment, invoice, payment."""
    step("3. Quote, order, invoice")

    customer = await documents.get(erp, documents.CUSTOMERS, "C-001", Customer.from_json)
    screws = await documents.get(erp, documents.PRODUCTS, "P-001", Product.from_json)
    brackets = await documents.get(erp, documents.PRODUCTS, "P-003", Product.from_json)

    lines = [
        Line(
            product_id=screws.id,
            name=screws.name,
            quantity=12,
            unit_price=screws.unit_price,
            vat_rate=screws.vat_rate,
        ),
        Line(
            product_id=brackets.id,
            name=brackets.name,
            quantity=80,
            unit_price=brackets.unit_price,
            vat_rate=brackets.vat_rate,
        ),
    ]

    # The quote.
    quote = Quote(
        id="Q-2026-0001",
        customer_id=customer.id,
        status=QUOTE_SENT,
        date=TODAY,
        lines=lines,
        totals=totals(lines),
    )
    await documents.create_bound(erp, documents.QUOTES, "quote", quote.id, quote)
    await graph.link(
        erp,
        graph.REQUESTED,
        graph.node("customer", customer.id),
        graph.node("quote", quote.id),
    )
    print(
        f"   quote {quote.id} for {customer.name}: {money(quote.totals.net)} net, "
        f"{money(quote.totals.vat)} VAT, {money(quote.totals.gross)} gross"
    )

    # Accepted, so it becomes an order. `dataclasses.replace` is the frozen
    # dataclass's answer to a spread: one changed field, everything else copied.
    accepted = dataclasses.replace(quote, status=QUOTE_ACCEPTED)
    await documents.put(erp, documents.QUOTES, accepted.id, accepted)

    order = Order(
        id="SO-2026-0001",
        customer_id=customer.id,
        quote_id=quote.id,
        status=ORDER_PREPARING,
        date=TODAY,
        lines=quote.lines,
        totals=quote.totals,
    )
    await documents.create_bound(erp, documents.ORDERS, "order", order.id, order)
    await graph.link_with_key(
        erp,
        graph.CONVERTED_TO,
        graph.node("quote", quote.id),
        graph.node("order", order.id),
    )
    print(f"   quote accepted, order {order.id} created")

    # Shipped: one stock move out per line.
    for line in order.lines:
        product = await move_stock(erp, line.product_id, "out", line.quantity, order.id)
        print(f"     - {product.reference}: -{line.quantity} leaves {product.stock} in stock")
    await documents.put(
        erp, documents.ORDERS, order.id, dataclasses.replace(order, status=ORDER_SHIPPED)
    )

    # Invoiced.
    invoice = Invoice(
        id="INV-2026-0001",
        customer_id=customer.id,
        order_id=order.id,
        status=INVOICE_ISSUED,
        date=TODAY,
        due_date=DUE_DATE,
        lines=order.lines,
        totals=order.totals,
    )
    await documents.create_bound(erp, documents.INVOICES, "invoice", invoice.id, invoice)
    await graph.link_with_key(
        erp,
        graph.BILLED_AS,
        graph.node("order", order.id),
        graph.node("invoice", invoice.id),
    )
    print(
        f"   invoice {invoice.id} issued, due {invoice.due_date}, "
        f"{money(invoice.totals.gross)} gross"
    )

    unpaid, total = await documents.unpaid_invoices(erp)
    print(
        "   query_documents (status in [issued, overdue], due date first): "
        f"{len(unpaid)} of {total}"
    )

    # Paid.
    await documents.put(
        erp, documents.INVOICES, invoice.id, dataclasses.replace(invoice, status=INVOICE_PAID)
    )
    print(f"   invoice {invoice.id} marked {INVOICE_PAID}")

    moves = await documents.moves_for_product(erp, "P-001")
    print("   stock moves on P-001: " + join([f"{m.direction} {m.quantity}" for m in moves]))

    # A second quote, declined by the customer.
    #
    # Order matters: `delete_document` is idempotent, `delete_edge` is not.
    # Removing the document first means a restart after a crash passes quietly
    # over the done half and finishes the edge; the other way round it would
    # hit NOT_FOUND.
    drill = await documents.get(erp, documents.PRODUCTS, "P-004", Product.from_json)
    declined_lines = [
        Line(
            product_id=drill.id,
            name=drill.name,
            quantity=2,
            unit_price=drill.unit_price,
            vat_rate=drill.vat_rate,
        )
    ]
    declined = Quote(
        id="Q-2026-0002",
        customer_id="C-002",
        status=QUOTE_SENT,
        date=TODAY,
        lines=declined_lines,
        totals=totals(declined_lines),
    )
    await documents.create_bound(erp, documents.QUOTES, "quote", declined.id, declined)
    customer_node = graph.node("customer", declined.customer_id)
    quote_node = graph.node("quote", declined.id)
    await graph.link(erp, graph.REQUESTED, customer_node, quote_node)

    await documents.remove_with_key(erp, documents.QUOTES, declined.id)
    await graph.unlink_with_key(erp, graph.edge(graph.REQUESTED, customer_node, quote_node))
    print(f"   quote {declined.id} declined and removed")


async def attachments(erp: Erp) -> None:
    """Step 4: the three uploads and the two downloads."""
    step("4. Attachments")

    # 4a. The invoice as a text document: it fits in memory, so `upload_file`
    #     handles chunking and hashing.
    invoice = await documents.get(erp, documents.INVOICES, "INV-2026-0001", Invoice.from_json)
    detail = "\n".join(
        f"{line.quantity} x {line.name} = {money(line.unit_price * line.quantity)}"
        for line in invoice.lines
    )
    text = (
        f"INVOICE {invoice.id}\nDue {invoice.due_date}\n\n"
        f"{detail}\n\nTotal: {money(invoice.totals.gross)}\n"
    )
    invoice_bytes = text.encode("utf-8")

    invoice_file = f"invoices/{invoice.id}.txt"
    await files.upload(erp, invoice_file, invoice_bytes, "text/plain; charset=utf-8")
    print(f"   upload_file: {invoice_file} ({len(invoice_bytes)} bytes)")

    # 4b. The stock export: produced in pieces, re-chunked by the SDK.
    products = await documents.all_products(erp)
    rows = "\n".join(
        ";".join([p.reference, p.name, p.family, str(p.stock), str(p.min_stock), str(p.unit_price)])
        for p in products
    )
    csv = f"reference;name;family;stock;min_stock;unit_price\n{rows}\n"

    export_file = f"exports/stock-{TODAY}.csv"
    await files.upload_chunked(erp, export_file, csv.encode("utf-8"), "text/csv")
    print(f"   upload_file_chunked: {export_file}")

    # 4c. A short note, one hand-built message.
    await files.upload_raw(erp, "notes/reorder.txt", b"Check P-005 before the next order.\n")
    print("   upload_file_raw: notes/reorder.txt")

    info = await files.stat(erp, invoice_file)
    print(f"   stat_file: {info.size_bytes} bytes, {info.content_type}, created {info.created_at}")
    # The checksum comes back as the raw 32 bytes, not hex: display it as hex.
    print(f"   sha256: {info.checksum.hex()}")

    downloaded = await files.download(erp, invoice_file)
    print(
        f"   download_file: {len(downloaded)} bytes back, identical: {downloaded == invoice_bytes}"
    )

    streamed = await files.download_streamed(erp, export_file)
    print(f"   download_file_stream: {streamed} bytes read")

    print("   list_files: " + join(await files.list_files(erp)))

    await files.remove_with_key(erp, "notes/reorder.txt")
    print("   delete_file: note removed")


async def traverse(erp: Erp) -> None:
    """Step 5: graph traversals."""
    step("5. Graph traversals")

    supplied = await graph.products_of_supplier(erp, "S-001")
    print("   get_outgoing_neighbor_nodes (S-001 -supplies->): " + join([p.name for p in supplied]))

    sources = await graph.suppliers_of_product(erp, "P-001")
    print("   get_incoming_neighbor_nodes (-supplies-> P-001): " + join(sources))

    quotes = await graph.neighbors_out(erp, graph.node("customer", "C-001"), graph.REQUESTED)
    print("   neighbors_out (C-001 -requested->): " + join([n.node_id for n in quotes]))

    orders = await graph.neighbors_in(erp, graph.node("invoice", "INV-2026-0001"), graph.BILLED_AS)
    print("   neighbors_in (-billed_as-> INV-2026-0001): " + join([n.node_id for n in orders]))

    # The node `create_document` wrote: a pointer to the document.
    node_id = graph.node("invoice", "INV-2026-0001")
    print(f"   get_node without a decoder (raw): {await graph.raw_node(erp, node_id)}")
    doc_ref = await graph.node_ref(erp, node_id)
    print(
        f'   get_node with a decoder (typed): collection "{doc_ref.collection}", id "{doc_ref.id}"'
    )


async def explore(erp: Erp) -> None:
    """Step 6: what the deployment holds, and the token."""
    step("6. Exploring the deployment")

    # `list_tenants` is the one RPC not scoped to a tenant. It enumerates the
    # whole deployment and may be refused by a dedicated policy, so
    # PERMISSION_DENIED here means "not your role", not "broken".
    #
    # Worth knowing: `tenant_id` is a business partition, not a security
    # boundary. It is derived from no identity — any authenticated client can
    # address any tenant. Enforcing who may touch what is the application's job.
    try:
        page = await erp.client.list_tenants(limit=50)
        print("   list_tenants: " + join(page.items))
    except RociaDbStatusError as error:
        if not error.is_permission_denied():
            raise
        print("   list_tenants: refused, this RPC covers the whole deployment")

    collections = await documents.list_collections(erp)
    print("   list_collections: " + join([f"{c.collection} ({c.count})" for c in collections]))
    print("   list_graphs: " + join(await graph.list_graphs(erp)))
    print(f"   list_nodes: {len(await graph.list_nodes(erp))} nodes")
    print("   list_buckets: " + join(await files.list_buckets(erp)))
    print(f"   customers (free total_count): {await documents.count_customers(erp)}")

    # Both token calls are no-ops when the client was built with
    # `disable_auth()`, so callers need not know how it was built.
    await erp.client.refresh_auth_token()
    print("   refresh_auth_token: renewed now, caller waits")
    erp.client.invalidate_auth_token()
    print("   invalidate_auth_token: wakes the background refresh, does not wait")

    credentials = auth_from_environment()
    if credentials is not None:
        await auth_module_demo(*credentials)


async def auth_module_demo(token_url: str, client_id: str, client_secret: str) -> None:
    """The auth helpers used without a `RociaDbClient`.

    `build()` sets all of this up for you. They are exported from
    `rociadb_sdk.auth` for when the same token has to be used elsewhere, next
    to a service of your own.
    """
    # One token, once. No caching, no renewal.
    token = await fetch_token(token_url, client_id, client_secret)

    # The manager caches and renews. Construction is synchronous and fetches
    # nothing: the first token arrives with the first `get_authorization_header()`.
    manager = TokenManager(token_url, client_id, client_secret)
    header = await manager.get_authorization_header()

    # Renewal is a background task here, not something checked inline on each
    # call, so standalone you have to start it — and own it. `build()` does
    # this for the client's own manager, and `close()` is what stops it. The
    # `finally` is not decoration: the task outlives anything that fails above
    # it, and nothing else will stop it.
    handle = manager.spawn_refresh(manager.refresh_interval())
    try:
        # Two ways to renew. `request_refresh` only nudges the task above and
        # returns at once; `refresh_now` awaits the round trip itself. In that
        # order the nudge is serviced while we wait here — do not close the
        # handle straight after a nudge instead, because cancelling the task in
        # the same breath as waking it can leave `aclose()` waiting a whole
        # refresh interval for a loop that swallowed the cancellation.
        manager.request_refresh()
        await manager.refresh_now()
    finally:
        await handle.aclose()

    print(
        f"   auth helpers: {token.token_type} token, valid {token.expires_in}s, "
        f'header "{header[:13]}…"'
    )


async def show_error_handling(erp: Erp) -> None:
    """Step 7: what a `RociaDbError` tells you.

    A class hierarchy, not one class with a discriminant field: `except
    RociaDbStatusError` narrows to a server answer, `except RociaDbError`
    catches every SDK failure alike. Two questions decide whether to retry, and
    they are the ones to ask first.
    """
    step("7. Reading an error")

    try:
        await documents.get(erp, documents.CUSTOMERS, "C-DOES-NOT-EXIST", Customer.from_json)
        print("   unexpectedly found a customer that should not exist")
    except RociaDbError as error:
        # UNAUTHENTICATED is the only case worth retrying: refresh the token,
        # then replay. PERMISSION_DENIED is final — the token is valid but
        # lacks the scope, so refreshing changes nothing. Both predicates are
        # defined on the base class, so neither needs the type checked first.
        print(f"   unauthenticated: {error.is_unauthenticated()}")
        print(f"   permission_denied: {error.is_permission_denied()}")

        print(f"   type: {type(error).__name__}")
        if isinstance(error, RociaDbStatusError):
            # Only this subclass ever carries a gRPC code: everything else was
            # raised client-side, before any RPC. `code` is a `grpc.StatusCode`,
            # `reason` the server's own finer-grained word (`not_found`,
            # `invalid_argument`, ...), and `grpc_error` the untouched
            # `AioRpcError`, so nothing is lost against calling the generated
            # client directly.
            print(f"   operation: {error.operation}")
            print(f"   code: {error.code.name}")
            print(f"   is NOT_FOUND: {error.code == grpc.StatusCode.NOT_FOUND}")
            print(f"   reason: {error.reason or '(none)'}")
            print(f"   details: {error.grpc_error.details() or '(none)'}")
        print(f"   message: {error}")
        print(f"   what to do: {advice(error)}")


def advice(error: RociaDbError) -> str:
    """One line of guidance per error class.

    Ordered subclass-first, and the last arm is not decoration: unlike a closed
    union of string literals, a class hierarchy gives mypy nothing to prove
    exhaustive, so a seventh subclass would silently land there rather than
    fail to type-check.
    """
    if isinstance(error, RociaDbStatusError):
        return "the server refused the call; read reason to know why"
    if isinstance(error, RociaDbConnectionError):
        return "check the server is up, and that the host carries no path"
    if isinstance(error, RociaDbAuthError):
        return "check AUTH_TOKEN_URL / AUTH_CLIENT_ID / AUTH_CLIENT_SECRET"
    if isinstance(error, RociaDbEncodeError):
        return f"{error.context} could not be serialized; fix the model"
    if isinstance(error, RociaDbDecodeError):
        return f"{error.context} no longer matches the dataclass; fix the decoder"
    if isinstance(error, RociaDbValidationError):
        return "rejected client-side; nothing was sent"
    return "an SDK error this example does not know about; treat it as a refusal"


async def cleanup(erp: Erp) -> None:
    """Step 8: remove the demo data.

    Three delete semantics meet here: `delete_document` and `delete_file` are
    idempotent, `delete_edge` is not, and **nothing deletes a node** — there is
    no RPC for it, so a node with no edges and no document stays listed by
    `list_nodes`.
    """
    step("8. Cleanup")

    # Edges first, while the traversals still find them. `Neighbor` carries the
    # real edge id, so there is nothing to rebuild.
    edges = 0
    for node_id in await graph.list_nodes(erp):
        for label in (graph.SUPPLIES, graph.REQUESTED, graph.CONVERTED_TO, graph.BILLED_AS):
            for neighbor in await graph.neighbors_out(erp, node_id, label):
                await graph.unlink(erp, neighbor.edge_id)
                edges += 1

    docs = 0
    for collection in (
        documents.STOCK_MOVES,
        documents.INVOICES,
        documents.ORDERS,
        documents.QUOTES,
        documents.PRODUCTS,
        documents.CUSTOMERS,
        documents.SUPPLIERS,
    ):
        # No decoder: the raw JSON is all this needs, and every collection here
        # holds a different shape.
        page = await erp.client.list_documents(erp.tenant, collection, limit=200)
        for document in page.items:
            document_id = document.get("id")
            if document_id is None:
                continue
            await documents.remove(erp, collection, document_id)
            docs += 1

    removed = 0
    for file_id in await files.list_files(erp):
        await files.remove(erp, file_id)
        removed += 1

    print(f"   {docs} documents, {edges} edges and {removed} files deleted")
    print(f"   {len(await graph.list_nodes(erp))} nodes remain: no RPC deletes a node")


async def move_stock(
    erp: Erp, product_id: str, direction: Literal["in", "out"], quantity: int, source: str
) -> Product:
    """Move stock and record the move.

    Two writes, not a transaction: RociaDB offers no atomicity across
    documents. The stock is updated first, so a crash in between leaves an
    up-to-date stock with no trace rather than a trace with no effect — the
    easier of the two to reconcile.
    """
    stored = await documents.get(erp, documents.PRODUCTS, product_id, Product.from_json)
    delta = quantity if direction == "in" else -quantity
    if stored.stock + delta < 0:
        raise ValueError(f"not enough stock on {product_id}")

    product = dataclasses.replace(stored, stock=stored.stock + delta)
    await documents.save_product(erp, product)

    stock_move = StockMove(
        id=f"MOV-{product_id}-{direction}-{erp.run}",
        product_id=product_id,
        direction=direction,
        quantity=quantity,
        source=source,
    )
    await documents.put(erp, documents.STOCK_MOVES, stock_move.id, stock_move)
    return product


def auth_from_environment() -> tuple[str, str, str] | None:
    """The three OAuth2 variables, or `None` if any one of them is missing."""
    token_url = os.environ.get("AUTH_TOKEN_URL")
    client_id = os.environ.get("AUTH_CLIENT_ID")
    client_secret = os.environ.get("AUTH_CLIENT_SECRET")
    if not token_url or not client_id or not client_secret:
        return None
    return token_url, client_id, client_secret


def fail(error: BaseException) -> None:
    """Print the failure the way a CLI should. The caller returns the exit code."""
    print(f"\nFailed: {error}", file=sys.stderr)


def step(title: str) -> None:
    print(f"\n{title}")


def join(values: Sequence[str]) -> str:
    return ", ".join(values) if values else "none"


def demo_customers() -> list[Customer]:
    return [
        Customer(
            id="C-001",
            name="Bertrand Joinery",
            email="orders@bertrand.example",
            city="Nantes",
            active=True,
        ),
        Customer(
            id="C-002",
            name="Woodcraft Studio",
            email="buying@woodcraft.example",
            city="Rennes",
            active=True,
        ),
        Customer(
            id="C-003",
            name="Morel Framing",
            email="accounts@morel.example",
            city="Angers",
            active=False,
        ),
    ]


def demo_suppliers() -> list[Supplier]:
    return [
        Supplier(
            id="S-001",
            name="Central Fasteners",
            email="sales@fasteners.example",
            lead_time_days=5,
        ),
        Supplier(id="S-002", name="ProTools", email="sales@protools.example", lead_time_days=12),
    ]


def demo_products() -> list[Product]:
    return [
        Product(
            id="P-001",
            reference="SCR-4X30",
            name="Wood screw 4x30 (box of 200)",
            family="hardware",
            unit_price=1250,
            vat_rate=VAT_STANDARD,
            stock=120,
            min_stock=40,
            active=True,
        ),
        Product(
            id="P-002",
            reference="SCR-5X50",
            name="Wood screw 5x50 (box of 100)",
            family="hardware",
            unit_price=1890,
            vat_rate=VAT_STANDARD,
            stock=18,
            min_stock=30,
            active=True,
        ),
        Product(
            id="P-003",
            reference="BRK-RAFT",
            name="Galvanised rafter bracket",
            family="hardware",
            unit_price=340,
            vat_rate=VAT_STANDARD,
            stock=640,
            min_stock=150,
            active=True,
        ),
        Product(
            id="P-004",
            reference="DRL-18V",
            name="Cordless drill 18V",
            family="tools",
            unit_price=14900,
            vat_rate=VAT_STANDARD,
            stock=7,
            min_stock=4,
            active=True,
        ),
        Product(
            id="P-005",
            reference="DOC-FIT",
            name="Printed fitting guide",
            family="documentation",
            unit_price=450,
            vat_rate=VAT_REDUCED,
            stock=2,
            min_stock=25,
            active=True,
        ),
    ]


def run() -> None:
    """Console-script entry point. `asyncio.run` is what starts the loop the
    whole SDK runs on, and closes it once `main` returns."""
    raise SystemExit(asyncio.run(main()))


if __name__ == "__main__":
    run()
