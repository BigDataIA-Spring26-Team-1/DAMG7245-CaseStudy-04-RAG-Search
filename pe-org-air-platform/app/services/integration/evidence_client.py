from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

# Assumption: you already have Snowflake connectivity in your platform.
# If you already have a helper like `get_snowflake_connection()`, use it here.
# Otherwise, wire it to your existing Snowflake service module.
from app.services.snowflake import get_snowflake_connection  # <-- adapt if your path differs


@dataclass(frozen=True)
class EvidenceChunkRow:
    """
    A normalized view of a chunk row joined with its document metadata.
    Keep fields minimal but sufficient for:
      - indexing
      - metadata filtering
      - justification citations
    """
    document_id: str
    chunk_id: str
    chunk_text: str
    chunk_index: int

    # metadata (from documents table)
    company_id: str
    source_type: str                  # e.g., "sec_filing", "job_posting", etc.
    signal_category: Optional[str]    # if you store this
    confidence: Optional[float]
    source_url: Optional[str]
    fiscal_year: Optional[int]

    # optional extra fields (safe to include if present)
    title: Optional[str]
    doc_type: Optional[str]           # e.g., "10-K"
    published_at: Optional[str]       # ISO string if available


class EvidenceClient:
    """
    Snowflake-backed evidence reader.

    Code review notes:
    - Uses Snowflake as source of truth
    - Supports batch pagination via chunk_index + LIMIT/OFFSET
    - Keeps Chroma as a derived, rebuildable index
    """

    def __init__(self, schema: Optional[str] = None) -> None:
        self.schema = schema  # e.g., "PUBLIC" if needed

    def _qualify(self, table: str) -> str:
        return f"{self.schema}.{table}" if self.schema else table

    def iter_chunks_for_company(
        self,
        company_id: str,
        batch_size: int = 500,
        min_confidence: Optional[float] = None,
    ) -> Iterable[List[EvidenceChunkRow]]:
        """
        Yields batches of joined (document_chunks + documents) for a given company.
        """
        docs = self._qualify("documents")
        chunks = self._qualify("document_chunks")

        where_parts = ["d.company_id = %s"]
        params: List[Any] = [company_id]

        if min_confidence is not None:
            # documents table currently has no confidence column;
            # treat SEC document chunks as high-confidence evidence.
            where_parts.append("1.0 >= %s")
            params.append(float(min_confidence))

        where_sql = " AND ".join(where_parts)

        # Using OFFSET pagination for simplicity.
        # If your table is huge, we can switch to keyset pagination on (document_id, chunk_index).
        offset = 0

        sql = f"""
        SELECT
            d.id AS document_id,
            c.id AS chunk_id,
            c.content AS chunk_text,
            c.chunk_index,

            d.company_id,
            'sec_filing' AS source_type,
            NULL AS signal_category,
            1.0 AS confidence,
            d.source_url,
            YEAR(d.filing_date) AS fiscal_year,
            d.filing_type AS title,
            d.filing_type AS doc_type,
            TO_VARCHAR(d.filing_date) AS published_at
        FROM {chunks} c
        JOIN {docs} d
          ON d.id = c.document_id
        WHERE {where_sql}
        ORDER BY c.document_id, c.chunk_index
        LIMIT %s OFFSET %s
        """

        conn = get_snowflake_connection()
        try:
            while True:
                cur = conn.cursor()
                try:
                    cur.execute(sql, params + [batch_size, offset])
                    rows = cur.fetchall()
                finally:
                    cur.close()

                if not rows:
                    break

                batch: List[EvidenceChunkRow] = []
                for r in rows:
                    batch.append(
                        EvidenceChunkRow(
                            document_id=str(r[0]),
                            chunk_id=str(r[1]),
                            chunk_text=str(r[2] or ""),
                            chunk_index=int(r[3] or 0),

                            company_id=str(r[4]),
                            source_type=str(r[5] or ""),
                            signal_category=r[6],
                            confidence=float(r[7]) if r[7] is not None else None,
                            source_url=r[8],
                            fiscal_year=int(r[9]) if r[9] is not None else None,
                            title=r[10],
                            doc_type=r[11],
                            published_at=str(r[12]) if r[12] is not None else None,
                        )
                    )

                yield batch
                offset += batch_size
        finally:
            conn.close()
