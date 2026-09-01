from erp_example.graph import SUPPLIES, edge, node


def test_node_ids_follow_the_sdk_convention() -> None:
    # `create_document` builds exactly "{label}:{id}". If that changed, every
    # traversal here would look at the wrong node.
    assert node("customer", "C-1") == "customer:C-1"
    assert node("invoice", "INV-2026-1") == "invoice:INV-2026-1"


def test_edge_ids_are_rebuildable() -> None:
    assert edge(SUPPLIES, "supplier:S-1", "product:P-1") == "supplies|supplier:S-1|product:P-1"
