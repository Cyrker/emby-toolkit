from __future__ import annotations

from typing import Any

from sqlalchemy import text

from etk.platform.observability.redaction import redact_sensitive

# 直接投影已存在的任务与缓存；不把本地刮削伪造成 115 文件整理，也不改写历史任务。
_RECORDS = """
WITH jobs AS (
    SELECT j.id, j.status AS job_status, j.created_at, j.finished_at,
           j.request_payload_json->>'display_name' AS display_name,
           j.request_payload_json->'parameters' AS parameters,
           COALESCE(j.result_json, a.error_details_json->'task_outcome', '{}'::jsonb) AS outcome,
           j.error_summary_json->>'message' AS error_message
    FROM job_runs j
    LEFT JOIN LATERAL (
        SELECT error_details_json FROM job_attempts WHERE job_id=j.id ORDER BY attempt_no DESC LIMIT 1
    ) a ON TRUE
    WHERE j.task_key IN ('native-operation:media.reprocess-item', 'p8-review-reprocess-one')
      AND COALESCE(j.request_payload_json->'parameters'->>'media_info_only', 'false') <> 'true'
), identified AS (
    SELECT *, COALESCE(parameters->>'item_id', parameters->>'emby_item_id', outcome->'details'->>'item_id') AS item_id
    FROM jobs
), snapshots AS (
    SELECT j.*, m.title, m.item_type, m.tmdb_id, m.parent_series_tmdb_id, m.season_number, m.episode_number,
           m.overview, asset.path,
           COALESCE(j.outcome->'details'->'metadata'->'incomplete', '[]'::jsonb) AS incomplete,
           COALESCE(j.outcome->'details'->'metadata'->'failures', '[]'::jsonb) AS failures
    FROM identified j
    LEFT JOIN LATERAL (
        SELECT * FROM media_metadata WHERE emby_item_ids_json @> jsonb_build_array(j.item_id)
        ORDER BY last_updated_at DESC LIMIT 1
    ) m ON TRUE
    LEFT JOIN LATERAL (
        SELECT v->>'path' AS path FROM jsonb_array_elements(COALESCE(m.asset_details_json, '[]'::jsonb)) v
        WHERE v->>'emby_item_id'=j.item_id LIMIT 1
    ) asset ON TRUE
), records AS (
    SELECT *, CASE
        WHEN job_status NOT IN ('succeeded','failed','cancelled','skipped') THEN 'running'
        WHEN job_status='cancelled' THEN 'cancelled'
        WHEN jsonb_array_length(failures)>0 THEN 'failed'
        WHEN jsonb_array_length(incomplete)>0 THEN 'incomplete'
        WHEN job_status='failed' THEN 'failed'
        WHEN item_type IN ('Movie','Series','Episode') AND (
            COALESCE(btrim(overview),'')='' OR item_type='Episode' AND (
                COALESCE(btrim(title),'')='' OR title ~* '^(第\\s*[0-9]+\\s*[集话話]|episode\\s*[0-9]+|ep\\s*[0-9]+)$'
            )
        ) THEN 'incomplete'
        WHEN job_status='skipped' THEN 'skipped' ELSE 'completed' END AS status
    FROM snapshots
)
"""


def list_scrape_records(uow: Any, *, page: int, per_page: int, search: str, status: str) -> dict[str, Any]:
    filters = (
        "WHERE (:status='' OR status=:status) "
        "AND (:search='' OR concat_ws(' ',display_name,title,path,item_id) ILIKE :pattern)"
    )
    params = {
        "status": status,
        "search": search,
        "pattern": f"%{search}%",
        "limit": per_page,
        "offset": (page - 1) * per_page,
    }
    total = uow.session.execute(text(_RECORDS + "SELECT count(*) FROM records " + filters), params).scalar_one()
    rows = uow.session.execute(
        text(
            _RECORDS
            + "SELECT * FROM records "
            + filters
            + " ORDER BY created_at DESC,id DESC LIMIT :limit OFFSET :offset"
        ),
        params,
    ).mappings()
    return {"items": [_record(dict(row)) for row in rows], "total": total, "page": page, "per_page": per_page}


def _record(row: dict[str, Any]) -> dict[str, Any]:
    outcome = row.get("outcome") or {}
    metadata = (outcome.get("details") or {}).get("metadata") or {}
    path = row.get("path") or ""
    display = row.get("display_name") or "媒体刮削"
    retry = (outcome.get("details") or {}).get("metadata_retry") or {}
    source = "手动刮削"
    if (row.get("parameters") or {}).get("metadata_retry_count"):
        source = "自动补抓"
    elif display.startswith("主动入库:"):
        source = "STRM 入库" if path.lower().endswith(".strm") else "本地入库" if path else "自动入库"
    incomplete = row.get("incomplete") or []
    return {
        "id": str(row["id"]),
        "item_id": row.get("item_id"),
        "name": display,
        "title": row.get("title"),
        "source": source,
        "path": path,
        "status": row["status"],
        "job_status": row["job_status"],
        "created_at": row["created_at"].isoformat(),
        "finished_at": row["finished_at"].isoformat() if row.get("finished_at") else None,
        "selected": metadata.get("selected"),
        "verified": metadata.get("verified"),
        "message": redact_sensitive(str(outcome.get("message") or row.get("error_message") or "")),
        "incomplete": incomplete[:20],
        "retry": retry,
        "completeness_checked": "incomplete" in metadata,
        "season_number": row.get("season_number"),
        "episode_number": row.get("episode_number"),
    }
