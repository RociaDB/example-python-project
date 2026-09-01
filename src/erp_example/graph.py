"""The graph service: who supplies what, which quote became which invoice.

Two things shape this module:

- **The graph is an index, not the source of truth.** No RPC reads an edge's
  value back — `neighbors_out` returns a `node_id` and an `edge_id`, nothing
  else. Anything you need to read lives in the document.
- **An edge needs both endpoints first.** `add_edge` returns `NOT_FOUND` if
  `from_id` or `to_id` is not already a node. Nodes first, edges after.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import asdict, dataclass
from typing import Any, TypeVar

from rociadb_sdk import EdgeInput, Neighbor, NodeInput

from .documents import PRODUCTS
from .erp import GRAPH, Erp
from .model import Product

# Edge labels.
SUPPLIES = "supplies"
REQUESTED = "requested"
CONVERTED_TO = "converted_to"
BILLED_AS = "billed_as"

D = TypeVar("D", bound="DocRef")


@dataclass(frozen=True)
class DocRef:
    """The value `create_document` writes into the node it binds: a pointer
    back to the document."""

    collection: str
    id: str

    @classmethod
    def from_json(cls: type[D], raw: Any) -> D:
        # Typed on `cls` rather than on `DocRef`, so `ProductNode.from_json`
        # is a `ProductNode` decoder without redefining anything.
        return cls(**raw)


@dataclass(frozen=True)
class ProductNode(DocRef):
    """A node holding the pointer plus a couple of denormalized fields, so a
    traversal can show something readable without re-reading each document."""

    reference: str
    name: str


def node(label: str, node_id: str) -> str:
    """Node ids follow `"{label}:{id}"` — the same shape `create_document` uses."""
    return f"{label}:{node_id}"


def edge(label: str, from_id: str, to_id: str) -> str:
    """Edge ids are ours to choose; `delete_edge` takes only this id, so it has
    to be rebuildable without reading the graph first."""
    return f"{label}|{from_id}|{to_id}"


async def put_product_nodes(erp: Erp, products: Sequence[Product]) -> None:
    """Write all product nodes in one batch.

    `put_nodes` keeps at most `CONCURRENT_REQUESTS` (10) calls in flight. It is
    not atomic: the first failure cancels the calls already dispatched
    alongside it, so earlier items may be stored and later ones never sent.
    Retrying is safe precisely because each item carries its own key — the
    batch helpers read `request_id` off each `NodeInput` rather than taking one
    for the whole call.

    A node's value must be a JSON **object** — a scalar or a list is rejected.
    """
    nodes = [
        NodeInput(
            node_id=node("product", product.id),
            value=asdict(
                ProductNode(
                    collection=PRODUCTS,
                    id=product.id,
                    reference=product.reference,
                    name=product.name,
                )
            ),
            request_id=erp.key(f"node:{product.id}"),
        )
        for product in products
    ]

    await erp.client.put_nodes(erp.tenant, GRAPH, nodes)


async def put_node(erp: Erp, node_id: str, value: DocRef) -> None:
    """Write one node. `put_node` generates its own idempotency key."""
    await erp.client.put_node(erp.tenant, GRAPH, node_id, asdict(value))


async def put_node_with_key(erp: Erp, node_id: str, value: DocRef) -> None:
    """Write one node with a chosen key — what you would use to repair a
    document whose node binding never made it."""
    await erp.client.put_node(
        erp.tenant, GRAPH, node_id, asdict(value), request_id=erp.key(f"repair:{node_id}")
    )


async def link_supplier_products(
    erp: Erp, supplier_id: str, products: Sequence[tuple[str, int]]
) -> None:
    """Link a supplier to the products it supplies, in one batch.

    The edge value records the purchase terms. It is useful when reading the
    data server-side, but the SDK cannot read it back.
    """
    from_id = node("supplier", supplier_id)
    edges = [
        EdgeInput(
            edge_id=edge(SUPPLIES, from_id, node("product", product_id)),
            from_id=from_id,
            to_id=node("product", product_id),
            label=SUPPLIES,
            value={"purchase_price": purchase_price},
            request_id=erp.key(f"supplies:{supplier_id}:{product_id}"),
        )
        for product_id, purchase_price in products
    ]

    await erp.client.add_edges(erp.tenant, GRAPH, edges)


async def link(erp: Erp, label: str, from_id: str, to_id: str) -> None:
    """Add one edge. The SDK generates the idempotency key.

    The endpoints are `from_id` and `to_id` because `from` is a reserved word
    in Python and cannot name a parameter.
    """
    await erp.client.add_edge(
        erp.tenant,
        GRAPH,
        edge(label, from_id, to_id),
        from_id,
        to_id,
        label,
        {"note": "created by the demo"},
    )


async def link_with_key(erp: Erp, label: str, from_id: str, to_id: str) -> None:
    """Add one edge with a chosen key, so a retry after a timeout does not
    create a second one."""
    await erp.client.add_edge(
        erp.tenant,
        GRAPH,
        edge(label, from_id, to_id),
        from_id,
        to_id,
        label,
        {"note": "created by the demo"},
        request_id=erp.key(f"{label}:{from_id}:{to_id}"),
    )


async def products_of_supplier(erp: Erp, supplier_id: str) -> list[ProductNode]:
    """The products a supplier supplies, node values included.

    `get_outgoing_neighbor_nodes` does in one call what `neighbors_out` plus a
    `get_node` per result would do — it follows every page and hydrates each
    payload, with the same bounded concurrency as `put_nodes`. Because product
    nodes carry the name, nothing else has to be read.
    """
    neighbors = await erp.client.get_outgoing_neighbor_nodes(
        erp.tenant, GRAPH, node("supplier", supplier_id), SUPPLIES, decoder=ProductNode.from_json
    )
    return [neighbor.value for neighbor in neighbors]


async def suppliers_of_product(erp: Erp, product_id: str) -> list[str]:
    """Who supplies a product: the same traversal, backwards."""
    neighbors = await erp.client.get_incoming_neighbor_nodes(
        erp.tenant, GRAPH, node("product", product_id), SUPPLIES, decoder=DocRef.from_json
    )
    return [neighbor.value.id for neighbor in neighbors]


async def neighbors_out(erp: Erp, node_id: str, label: str) -> list[Neighbor]:
    """Raw outgoing neighbors. Prefer this over the hydrating helper above once
    a node has many edges: this one paginates, that one returns everything."""
    page = await erp.client.neighbors_out(erp.tenant, GRAPH, node_id, label, limit=50)
    return page.items


async def neighbors_in(erp: Erp, node_id: str, label: str) -> list[Neighbor]:
    """Raw incoming neighbors."""
    page = await erp.client.neighbors_in(erp.tenant, GRAPH, node_id, label, limit=50)
    return page.items


async def raw_node(erp: Erp, node_id: str) -> Any:
    """A node as stored, without committing to a shape.

    `get_node` is one method on both sides of that choice: omit `decoder` and
    the parsed JSON comes back as-is, which is the honest answer when the shape
    is unknown.
    """
    return await erp.client.get_node(erp.tenant, GRAPH, node_id)


async def node_ref(erp: Erp, node_id: str) -> DocRef:
    """The `{"collection": ..., "id": ...}` pointer a bound node carries."""
    ref: DocRef = await erp.client.get_node(erp.tenant, GRAPH, node_id, decoder=DocRef.from_json)
    return ref


async def list_graphs(erp: Erp) -> list[str]:
    """The graphs in this tenant. Like collections, one exists as soon as a
    node is written to it."""
    page = await erp.client.list_graphs(erp.tenant, limit=50)
    return page.items


async def list_nodes(erp: Erp) -> list[str]:
    """Every node id in the graph."""
    nodes: list[str] = []
    cursor: str | None = None
    while True:
        page = await erp.client.list_nodes(erp.tenant, GRAPH, limit=200, cursor=cursor)
        nodes.extend(page.items)
        if page.next_cursor is None:
            return nodes
        cursor = page.next_cursor


async def unlink(erp: Erp, edge_id: str) -> None:
    """Delete an edge.

    This is **not** idempotent: a missing edge returns `NOT_FOUND`, unlike
    `delete_document` and `delete_file`.
    """
    await erp.client.delete_edge(erp.tenant, GRAPH, edge_id)


async def unlink_with_key(erp: Erp, edge_id: str) -> None:
    """Delete an edge with a chosen key."""
    await erp.client.delete_edge(
        erp.tenant, GRAPH, edge_id, request_id=erp.key(f"unlink:{edge_id}")
    )
