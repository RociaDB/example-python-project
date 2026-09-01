"""The file service: invoice attachments and stock exports.

Three ways to upload, from most to least help:

| Method | Does for you | Use when |
|---|---|---|
| `upload_file` | chunks, hashes, checks the size | the file is in memory |
| `upload_file_chunked` | re-chunks a source, checks the total | the source is a stream and
  you know its size and hash up front |
| `upload_file_raw` | nothing | you build every message yourself |

**Do not translate those names by ear between SDKs.** `upload_file_chunked` is
TypeScript's `uploadFileStream`, and `upload_file_raw` is Rust's
`upload_file_stream` — the two near-identical names are the *opposite* tiers.

The wire contract the first two honour for you: the **first** message carries
the metadata (tenant, bucket, file id, `size_bytes`, `content_type`,
`checksum`, `request_id`); later messages are read only for their `chunk`; no
chunk may exceed 1 MiB; the chunks must add up to `size_bytes` exactly; and
`checksum` must be exactly 32 bytes — the server checks its length, never its
content.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator

from rociadb_sdk import UPLOAD_CHUNK_BYTES, FileMetadata, RawUploadMessage

from .erp import BUCKET, Erp

#: The pieces `upload_chunked` produces, to show the SDK re-buffering them.
SOURCE_PIECE_BYTES = 64 * 1024


async def upload(erp: Erp, file_id: str, content: bytes, content_type: str) -> None:
    """Upload a file already in memory.

    With `checksum` omitted, `upload_file` hashes the buffer itself and slices
    it into 1 MiB messages. We pass it explicitly here to show the rule: it
    must be exactly 32 bytes, or the call fails client-side before sending.
    """
    await erp.client.upload_file(
        erp.tenant,
        BUCKET,
        file_id,
        content,
        content_type=content_type,
        checksum=_sha256(content),
        request_id=erp.key(f"upload:{file_id}"),
    )


async def upload_chunked(erp: Erp, file_id: str, content: bytes, content_type: str) -> None:
    """Upload content produced in pieces, without holding all of it at once.

    `size_bytes` and `checksum` travel on the very first gRPC message, before a
    single byte has been read from the source, so both must be known up front —
    that is the one thing `upload_file` can do for you and this cannot. If the
    source ends up producing a different total, the upload fails rather than
    storing a file whose recorded size is a lie.

    The pieces here are 64 KiB; `upload_file_chunked` re-buffers them into 1 MiB
    messages whatever size they arrive in, and takes a plain generator or an
    async one indifferently.
    """
    await erp.client.upload_file_chunked(
        erp.tenant,
        BUCKET,
        file_id,
        len(content),
        _sha256(content),
        _pieces(content, SOURCE_PIECE_BYTES),
        content_type=content_type,
        request_id=erp.key(f"export:{file_id}"),
    )


async def upload_raw(erp: Erp, file_id: str, content: bytes) -> None:
    """Upload by building the protobuf-backed message yourself.

    `upload_file_raw` is the low-level escape hatch: no re-chunking, no size
    cap applied, no checksum computed, and no first-message/later-message
    distinction. A wrong `size_bytes`, or a checksum that does not match the
    bytes, goes through silently — the server only checks the checksum's
    length. It is also the one mutating call with no `request_id` default:
    every `RawUploadMessage` carries its own. This note fits in one message,
    which is the only case worth hand-writing.
    """
    if len(content) > UPLOAD_CHUNK_BYTES:
        raise ValueError("upload_raw only handles content that fits in one 1 MiB message")

    message = RawUploadMessage(
        tenant_id=erp.tenant,
        bucket=BUCKET,
        file_id=file_id,
        size_bytes=len(content),
        content_type="text/plain",
        checksum=_sha256(content),
        chunk=content,
        request_id=erp.key(f"raw:{file_id}"),
    )

    await erp.client.upload_file_raw([message])


async def stat(erp: Erp, file_id: str) -> FileMetadata:
    """Metadata without downloading the file."""
    return await erp.client.stat_file(erp.tenant, BUCKET, file_id)


async def download(erp: Erp, file_id: str) -> bytes:
    """Download the whole file."""
    return await erp.client.download_file(erp.tenant, BUCKET, file_id)


async def download_streamed(erp: Erp, file_id: str) -> int:
    """Download as a stream, never holding the whole file in memory.

    `download_file_stream` is an async generator function, so the call itself
    is not awaited — `async for` drives it. Counting bytes here, but this is
    the same loop you would write to pipe it to a file. Leaving the loop early
    cancels the gRPC call instead of letting it run to the end, so an early
    `break` costs nothing.
    """
    total = 0
    async for chunk in erp.client.download_file_stream(erp.tenant, BUCKET, file_id):
        total += len(chunk)
    return total


async def list_buckets(erp: Erp) -> list[str]:
    """The buckets in this tenant."""
    page = await erp.client.list_buckets(erp.tenant, limit=50)
    return page.items


async def list_files(erp: Erp) -> list[str]:
    """The files in our bucket."""
    files: list[str] = []
    cursor: str | None = None
    while True:
        page = await erp.client.list_files(erp.tenant, BUCKET, limit=50, cursor=cursor)
        files.extend(page.items)
        if page.next_cursor is None:
            return files
        cursor = page.next_cursor


async def remove(erp: Erp, file_id: str) -> None:
    """Delete a file. Idempotent, like `delete_document`."""
    await erp.client.delete_file(erp.tenant, BUCKET, file_id)


async def remove_with_key(erp: Erp, file_id: str) -> None:
    """Delete a file with a chosen key."""
    await erp.client.delete_file(
        erp.tenant, BUCKET, file_id, request_id=erp.key(f"delete-file:{file_id}")
    )


def _sha256(content: bytes) -> bytes:
    """The 32-byte digest the server insists on. Any other length is refused.

    `.digest()`, not `.hexdigest()`: the checksum travels as raw bytes, and hex
    would be 64 of them.
    """
    return hashlib.sha256(content).digest()


def _pieces(content: bytes, size: int) -> Iterator[bytes]:
    """Cut a buffer into pieces, standing in for a source read in chunks."""
    for offset in range(0, len(content), size):
        yield content[offset : offset + size]
