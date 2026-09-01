"""The shared context every module needs: the client, the tenant, and the
prefix that makes idempotency keys unique per run.

It lives in its own module because the four service modules all need it and
`main.py` imports all four: holding it there would make each of them import
the entry point back, and Python resolves a cycle like that by handing out a
half-initialised module — an `ImportError` on a name that is plainly defined.
"""

from __future__ import annotations

from dataclasses import dataclass

from rociadb_sdk import RociaDbClient

# Graphs and buckets are never declared either: they exist from the first node
# or file written to them.
GRAPH = "erp"
BUCKET = "attachments"


@dataclass(frozen=True)
class Erp:
    client: RociaDbClient
    tenant: str
    #: Unique per run, so nothing this demo writes is deduplicated against a
    #: previous one.
    run: str

    def key(self, name: str) -> str:
        """An idempotency key for one write.

        The server deduplicates on `(tenant_id, operation, request_id)` for 24
        hours. A *stable* key is what makes an interrupted import safe to
        replay — but if this demo reused the same keys on every run, a second
        run after a cleanup would write nothing at all: the server would see
        yesterday's writes replayed. So the prefix changes per run, and the key
        is stable within one.
        """
        return f"{self.run}:{name}"
