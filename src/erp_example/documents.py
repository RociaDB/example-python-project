"""The document service: write, read, search, query, delete."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import asdict
from typing import Any, TypeVar

from rociadb_sdk import (
    CollectionInfo,
    DocumentQueryFilter,
    DocumentQueryOperator,
    DocumentQuerySort,
    DocumentSortDirection,
)

from .erp import GRAPH, Erp
from .model import (
    INVOICE_ISSUED,
    INVOICE_OVERDUE,
    Customer,
    Invoice,
    Product,
    StockMove,
    Supplier,
)

# Collections are never declared: one appears the first time a document is
# written to it. Constants keep a typo from silently creating another one.
CUSTOMERS = "customers"
SUPPLIERS = "suppliers"
PRODUCTS = "products"
QUOTES = "quotes"
ORDERS = "orders"
INVOICES = "invoices"
STOCK_MOVES = "stock_moves"

T = TypeVar("T")


async def create_customer(erp: Erp, customer: Customer) -> None:
    """Write a customer, plus the graph node pointing back at it.

    `create_document` does two things: it writes the document, then, when
    `node_label` and `node_graph` are both given, it upserts the node
    `"{label}:{id}"` holding a `{"collection": ..., "id": ...}` pointer to the
    document. Supplying only one of the two is rejected client-side, before any
    RPC, with `RociaDbValidationError`.

    The two writes are not atomic. If the node write fails, the document is
    left without its binding.
    """
    await erp.client.create_document(
        erp.tenant,
        CUSTOMERS,
        customer.id,
        asdict(customer),
        node_label="customer",
        node_graph=GRAPH,
    )


async def create_supplier(erp: Erp, supplier: Supplier) -> None:
    """Same thing with an idempotency key we choose.

    The server deduplicates on `(tenant_id, operation, request_id)` for 24
    hours, so replaying an interrupted import with the same keys is safe. Note
    the key covers only the document write: the node binding generates its own.
    """
    await erp.client.create_document(
        erp.tenant,
        SUPPLIERS,
        supplier.id,
        asdict(supplier),
        node_label="supplier",
        node_graph=GRAPH,
        request_id=erp.key(f"supplier:{supplier.id}"),
    )


async def save_product(erp: Erp, product: Product) -> None:
    """Write a product. `put_document` writes the document only, no graph node."""
    await erp.client.put_document(erp.tenant, PRODUCTS, product.id, asdict(product))


async def import_product(erp: Erp, product: Product) -> None:
    """Same, with a stable key: this is the replayable-import case."""
    await erp.client.put_document(
        erp.tenant,
        PRODUCTS,
        product.id,
        asdict(product),
        request_id=erp.key(f"import:{product.id}"),
    )


async def get(erp: Erp, collection: str, document_id: str, decoder: Callable[[Any], T]) -> T:
    """Read one document and rebuild the dataclass it was written from.

    `decoder` is where a Python caller says what a document *is*. The SDK parses
    the stored JSON and passes the result to this callable; without one it hands
    back the raw `dict`/`list`/scalar and claims nothing. Unparseable JSON fails
    first, as a `RociaDbDecodeError`, before the decoder is ever called — a
    decoder that raises on an unexpected shape is the caller's own check, not
    the SDK's.
    """
    value: T = await erp.client.get_document(erp.tenant, collection, document_id, decoder=decoder)
    return value


async def put(erp: Erp, collection: str, document_id: str, record: Any) -> None:
    """Write any business record (quote, order, invoice, stock move).

    A dataclass is not JSON, so the conversion is explicit: `asdict` recurses
    into the nested `Line` and `Totals` on the way out, which is exactly what
    a decoder has to undo on the way back.
    """
    await erp.client.put_document(erp.tenant, collection, document_id, asdict(record))


async def create_bound(
    erp: Erp, collection: str, label: str, document_id: str, record: Any
) -> None:
    """Write a document and bind it to a graph node, with a chosen key."""
    await erp.client.create_document(
        erp.tenant,
        collection,
        document_id,
        asdict(record),
        node_label=label,
        node_graph=GRAPH,
        request_id=erp.key(f"{collection}:{document_id}"),
    )


async def all_products(erp: Erp) -> list[Product]:
    """Every product, page after page.

    This is the cursor pattern: the cursor is opaque, you pass it back
    unchanged, and it is `None` once the server has nothing more. Note what
    ends the loop — the missing cursor, never a short page: an index entry that
    briefly outlives the document it points to can make a page in the middle of
    a walk come back short, or even empty, without it being the last one.
    """
    products: list[Product] = []
    cursor: str | None = None
    while True:
        page = await erp.client.list_documents(
            erp.tenant, PRODUCTS, limit=50, cursor=cursor, decoder=Product.from_json
        )
        products.extend(page.items)
        if page.next_cursor is None:
            return products
        cursor = page.next_cursor


async def count_customers(erp: Erp) -> int:
    """How many customers, without fetching them.

    On `list_documents` the count is free — the server keeps a per-collection
    counter — where `query_documents` has to evaluate the whole filtered set to
    produce it. It arrives as a plain `int`: a protobuf `uint64` needs no
    special numeric type in Python.
    """
    page = await erp.client.list_documents(erp.tenant, CUSTOMERS, limit=1)
    return page.total_count


async def find_customer_by_email(erp: Erp, email: str) -> list[Customer]:
    """Find customers by exact e-mail.

    `find_documents_by_field` matches one field exactly, and the value must be
    a JSON scalar (string, number, boolean, `None`). An object or a list is
    rejected with `INVALID_ARGUMENT`.
    """
    page = await erp.client.find_documents_by_field(
        erp.tenant, CUSTOMERS, "email", email, limit=20, decoder=Customer.from_json
    )
    customers: list[Customer] = page.items
    return customers


async def moves_for_product(erp: Erp, product_id: str) -> list[StockMove]:
    """Stock moves for one product."""
    page = await erp.client.find_documents_by_field(
        erp.tenant,
        STOCK_MOVES,
        "product_id",
        product_id,
        limit=50,
        decoder=StockMove.from_json,
    )
    moves: list[StockMove] = page.items
    return moves


async def search_products(erp: Erp, family: str, word: str) -> tuple[list[Product], int]:
    """Search the catalogue: one family, one word in the name, sorted.

    Filters combine with AND — there is no OR. `CONTAINS` is a case-insensitive
    substring, but a term shorter than 3 characters is not indexable, and a
    query where no filter is indexable is refused rather than served by a full
    scan — which is why the `EQ` on `family` is there alongside it.

    Field names are the ones in the stored JSON, so they are snake_case here
    for the same reason the documents are: a filter on a field that does not
    exist matches nothing rather than failing.
    """
    page = await erp.client.query_documents(
        erp.tenant,
        PRODUCTS,
        filters=[
            DocumentQueryFilter(field="family", operator=DocumentQueryOperator.EQ, values=[family]),
            DocumentQueryFilter(
                field="name", operator=DocumentQueryOperator.CONTAINS, values=[word]
            ),
        ],
        sort=[DocumentQuerySort(field="name", direction=DocumentSortDirection.ASC)],
        limit=50,
        decoder=Product.from_json,
    )
    products: list[Product] = page.items
    return products, page.total_count


async def products_to_reorder(erp: Erp) -> list[Product]:
    """Products to reorder.

    The operators are `EQ`, `IN` and `CONTAINS` only — there is no comparison
    between two fields, so "stock < min_stock" cannot be a filter. We ask the
    server for active products sorted by stock and compare here, on a set it
    has already narrowed and ordered.
    """
    page = await erp.client.query_documents(
        erp.tenant,
        PRODUCTS,
        filters=[
            DocumentQueryFilter(field="active", operator=DocumentQueryOperator.EQ, values=[True])
        ],
        sort=[DocumentQuerySort(field="stock", direction=DocumentSortDirection.ASC)],
        limit=50,
        decoder=Product.from_json,
    )
    return [product for product in page.items if product.stock < product.min_stock]


async def unpaid_invoices(erp: Erp) -> tuple[list[Invoice], int]:
    """Invoices still to be collected, oldest due date first.

    `IN` takes several values on one field where `EQ` takes one. Dates are
    stored as ISO strings, which is what makes the server's lexicographic sort
    match chronological order.
    """
    page = await erp.client.query_documents(
        erp.tenant,
        INVOICES,
        filters=[
            DocumentQueryFilter(
                field="status",
                operator=DocumentQueryOperator.IN,
                values=[INVOICE_ISSUED, INVOICE_OVERDUE],
            )
        ],
        sort=[DocumentQuerySort(field="due_date", direction=DocumentSortDirection.ASC)],
        limit=50,
        decoder=Invoice.from_json,
    )
    invoices: list[Invoice] = page.items
    return invoices, page.total_count


async def list_collections(erp: Erp) -> list[CollectionInfo]:
    """Which collections exist for this tenant, and how many documents each holds."""
    page = await erp.client.list_collections(erp.tenant, limit=50)
    return page.items


async def remove(erp: Erp, collection: str, document_id: str) -> None:
    """Delete a document.

    This is idempotent: deleting an id that is not there succeeds, unlike
    `delete_edge`.
    """
    await erp.client.delete_document(erp.tenant, collection, document_id)


async def remove_with_key(erp: Erp, collection: str, document_id: str) -> None:
    """Delete with a chosen key, so a restarted cleanup does not replay."""
    await erp.client.delete_document(
        erp.tenant,
        collection,
        document_id,
        request_id=erp.key(f"delete:{collection}:{document_id}"),
    )
