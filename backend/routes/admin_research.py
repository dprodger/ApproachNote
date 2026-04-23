"""
Admin endpoints for the durable research job queue (research_jobs +
source_quotas — see sql/migrations/015_research_jobs.sql).

Mounted under /admin/research/. The gate_admin_paths hook in app.py runs
check_admin_or_respond() for every /admin/* URL, so no decorators are
needed here — anyone reaching these handlers is already an authenticated
admin (g.current_user is set).

Routes:
    GET  /admin/research/jobs           — paginated list, filter by source/status/target
    GET  /admin/research/jobs/<id>      — full job row including payload + result
    GET  /admin/research/stats          — counts by (source, status) + totals
    GET  /admin/research/quotas         — source_quotas snapshot
    POST /admin/research/jobs/<id>/retry  — reset to queued, run_after = now()
    POST /admin/research/jobs/<id>/cancel — mark dead
    POST /admin/research/enqueue        — manual job creation
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from flask import Blueprint, jsonify, request

from core import research_jobs
from db_utils import get_db_connection

logger = logging.getLogger(__name__)

admin_research_bp = Blueprint(
    'admin_research', __name__, url_prefix='/admin/research',
)

# Filterable status values; keep in sync with the CHECK constraint in 015.
_VALID_STATUSES = frozenset({'queued', 'running', 'done', 'failed', 'dead'})

# Hard cap on `limit` so a misuse can't fetch the whole table.
_MAX_LIMIT = 500


# ---------------------------------------------------------------------------
# Serialisation helpers
# ---------------------------------------------------------------------------

def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _serialize_job(row: dict[str, Any], *, include_payload: bool) -> dict[str, Any]:
    """Render a research_jobs row as JSON. payload + result included only on
    the detail endpoint to keep the list response light."""
    out: dict[str, Any] = {
        'id': row['id'],
        'source': row['source'],
        'job_type': row['job_type'],
        'target_type': row['target_type'],
        'target_id': str(row['target_id']),
        'status': row['status'],
        'priority': row['priority'],
        'attempts': row['attempts'],
        'max_attempts': row['max_attempts'],
        'run_after': _iso(row.get('run_after')),
        'claimed_at': _iso(row.get('claimed_at')),
        'claimed_by': row.get('claimed_by'),
        'finished_at': _iso(row.get('finished_at')),
        'last_error': row.get('last_error'),
        'created_at': _iso(row.get('created_at')),
        'updated_at': _iso(row.get('updated_at')),
    }
    if include_payload:
        out['payload'] = row.get('payload') or {}
        out['result'] = row.get('result')
    return out


def _serialize_quota(row: dict[str, Any]) -> dict[str, Any]:
    used = row['units_used']
    limit = row['units_limit']
    remaining = max(limit - used, 0)
    return {
        'source': row['source'],
        'window': row['window_name'],
        'units_used': used,
        'units_limit': limit,
        'units_remaining': remaining,
        'percent_used': round(used * 100.0 / limit, 1) if limit else None,
        'resets_at': _iso(row['resets_at']),
        'updated_at': _iso(row.get('updated_at')),
    }


# ---------------------------------------------------------------------------
# Listing + detail
# ---------------------------------------------------------------------------

@admin_research_bp.route('/jobs', methods=['GET'])
def list_jobs():
    """Filter & paginate research_jobs.

    Query params (all optional):
        source       — exact match
        status       — exact match (one of queued/running/done/failed/dead)
        target_type  — exact match
        target_id    — exact UUID match
        job_type     — exact match
        limit        — max rows (default 50, capped at 500)
        offset       — page offset (default 0)
    """
    filters: list[str] = []
    params: list[Any] = []

    for arg in ('source', 'status', 'target_type', 'target_id', 'job_type'):
        val = request.args.get(arg)
        if val is None:
            continue
        if arg == 'status' and val not in _VALID_STATUSES:
            return jsonify({
                'error': 'invalid status',
                'allowed': sorted(_VALID_STATUSES),
            }), 400
        filters.append(f"{arg} = %s")
        params.append(val)

    where = f"WHERE {' AND '.join(filters)}" if filters else ''

    try:
        limit = min(int(request.args.get('limit', 50)), _MAX_LIMIT)
        offset = max(int(request.args.get('offset', 0)), 0)
    except ValueError:
        return jsonify({'error': 'limit and offset must be integers'}), 400

    sql = f"""
        SELECT * FROM research_jobs
        {where}
        ORDER BY created_at DESC, id DESC
        LIMIT %s OFFSET %s
    """
    count_sql = f"SELECT count(*) AS total FROM research_jobs {where}"

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (*params, limit, offset))
            rows = cur.fetchall()
            cur.execute(count_sql, params)
            total = cur.fetchone()['total']

    return jsonify({
        'total': total,
        'limit': limit,
        'offset': offset,
        'jobs': [_serialize_job(r, include_payload=False) for r in rows],
    })


@admin_research_bp.route('/jobs/<int:job_id>', methods=['GET'])
def get_job(job_id: int):
    """Full job row, including payload + result."""
    row = research_jobs.get_job(job_id)
    if row is None:
        return jsonify({'error': 'not found', 'job_id': job_id}), 404
    return jsonify(_serialize_job(row, include_payload=True))


# ---------------------------------------------------------------------------
# Aggregates
# ---------------------------------------------------------------------------

@admin_research_bp.route('/stats', methods=['GET'])
def stats():
    """Counts by (source, status) + last-24h throughput + total dead."""
    sql_by_source = """
        SELECT source, status, count(*) AS n
        FROM research_jobs
        GROUP BY source, status
        ORDER BY source, status
    """
    sql_throughput = """
        SELECT
          count(*) FILTER (WHERE status = 'done' AND finished_at >= now() - interval '24 hours') AS done_24h,
          count(*) FILTER (WHERE status = 'dead' AND finished_at >= now() - interval '24 hours') AS dead_24h,
          count(*) FILTER (WHERE status = 'queued')  AS queued_now,
          count(*) FILTER (WHERE status = 'running') AS running_now,
          count(*) FILTER (WHERE status = 'dead')    AS dead_total
        FROM research_jobs
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_by_source)
            by_source = cur.fetchall()
            cur.execute(sql_throughput)
            totals = cur.fetchone()

    # Pivot into {source: {status: n}}
    pivoted: dict[str, dict[str, int]] = {}
    for row in by_source:
        pivoted.setdefault(row['source'], {})[row['status']] = row['n']

    return jsonify({
        'by_source': pivoted,
        'totals': dict(totals) if totals else {},
    })


@admin_research_bp.route('/quotas', methods=['GET'])
def quotas():
    """All source_quotas rows, with computed remaining + percent used."""
    sql = """
        SELECT source, window_name, units_used, units_limit, resets_at, updated_at
        FROM source_quotas
        ORDER BY source, window_name
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    return jsonify({'quotas': [_serialize_quota(r) for r in rows]})


# ---------------------------------------------------------------------------
# Mutations
# ---------------------------------------------------------------------------

@admin_research_bp.route('/jobs/<int:job_id>/retry', methods=['POST'])
def retry_job(job_id: int):
    """Move a job back to 'queued' so a worker picks it up immediately.
    Resets attempts to 0 so the user gets a fresh retry budget. Allowed
    from any status — useful for re-running a 'done' job too."""
    sql = """
        UPDATE research_jobs
        SET status     = 'queued',
            attempts   = 0,
            run_after  = now(),
            claimed_at = NULL,
            claimed_by = NULL,
            finished_at = NULL,
            last_error = NULL
        WHERE id = %s
        RETURNING *
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (job_id,))
            row = cur.fetchone()
    if row is None:
        return jsonify({'error': 'not found', 'job_id': job_id}), 404
    logger.info("admin: retry job_id=%s", job_id)
    return jsonify({'job': _serialize_job(row, include_payload=True)})


@admin_research_bp.route('/jobs/<int:job_id>/cancel', methods=['POST'])
def cancel_job(job_id: int):
    """Mark a job 'dead'. No-op if already terminal."""
    sql = """
        UPDATE research_jobs
        SET status      = 'dead',
            finished_at = COALESCE(finished_at, now()),
            last_error  = COALESCE(last_error, 'cancelled by admin')
        WHERE id = %s AND status IN ('queued', 'running', 'failed')
        RETURNING *
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (job_id,))
            row = cur.fetchone()
    if row is None:
        # Could be missing OR already terminal — distinguish for the caller.
        existing = research_jobs.get_job(job_id)
        if existing is None:
            return jsonify({'error': 'not found', 'job_id': job_id}), 404
        return jsonify({
            'error': 'already terminal',
            'job_id': job_id,
            'status': existing['status'],
        }), 409
    logger.info("admin: cancel job_id=%s", job_id)
    return jsonify({'job': _serialize_job(row, include_payload=True)})


@admin_research_bp.route('/enqueue', methods=['POST'])
def enqueue():
    """Manual job creation. Body:

        {
          "source": "youtube",
          "job_type": "match_recording",
          "target_type": "recording",
          "target_id": "<uuid>",
          "payload": {...},        // optional
          "priority": 50,          // optional, defaults to 100
          "max_attempts": 5        // optional
        }
    """
    body = request.get_json(silent=True) or {}
    required = ('source', 'job_type', 'target_type', 'target_id')
    missing = [k for k in required if not body.get(k)]
    if missing:
        return jsonify({'error': 'missing fields', 'fields': missing}), 400

    job_id = research_jobs.enqueue(
        source=body['source'],
        job_type=body['job_type'],
        target_type=body['target_type'],
        target_id=body['target_id'],
        payload=body.get('payload'),
        priority=int(body.get('priority', 100)),
        max_attempts=int(body.get('max_attempts', 5)),
    )
    if job_id is None:
        return jsonify({'error': 'enqueue failed (see worker logs)'}), 500
    return jsonify({'job_id': job_id}), 201
