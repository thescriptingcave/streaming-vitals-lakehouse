#!/usr/bin/env python3
"""Provision the "Vitals Live" dashboard in Superset against the Iceberg/Trino catalog.

Idempotent-ish provisioning flow that was verified on the local scaffold:

  1. REST API  -> create Trino database, register the `vitals_1m` dataset,
                   create 3 charts, create the dashboard and set its layout.
  2. SQLite    -> link dashboard <-> charts in `dashboard_slices` (this Superset
                   build has no REST endpoint for attaching charts; membership is
                   read from the `slices` relationship).

Requires:
  - Superset running on SUPERSET_BASE (default http://127.0.0.1:18088), bootstrapped
    with admin/admin (see superset/docker-bootstrap.sh).
  - sqlite3 available on the host; the Superset metadata DB lives at superset_home/.
"""

import http.cookiejar
import json
import sqlite3
import sys
import urllib.error
import urllib.request
from pathlib import Path

BASE = "http://127.0.0.1:18088"
SUPERSET_DB = Path(__file__).resolve().parent.parent / "superset_home" / "superset.db"
DS_NAME = "vitals_1m"
DB_NAME = "Trino (Iceberg healthcare)"
TRINO_URI = "trino://admin@trino:8080/iceberg/healthcare"
DASH_TITLE = "Vitals Live"

COOKIEJAR = http.cookiejar.CookieJar()
OPENER = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(COOKIEJAR))
CSRF: dict[str, str] = {}


def call(method, path, payload=None, token=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
        if method in ("POST", "PUT", "DELETE") and CSRF.get("token"):
            headers["X-CSRFToken"] = CSRF["token"]
    body = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(BASE + path, data=body, headers=headers, method=method)
    try:
        with OPENER.open(req, timeout=90) as r:
            data = r.read().decode()
            return r.status, json.loads(data) if data else {}
    except urllib.error.HTTPError as e:
        data = e.read().decode()
        try:
            return e.code, json.loads(data) if data else {}
        except Exception:
            return e.code, {"raw": data[:400]}


def get_all(path, token):
    out: list[dict[str, object]] = []
    page = 0
    while True:
        code, d = call("GET", f"{path}?q=(page:{page},page_size:100)", None, token)
        if code != 200:
            raise RuntimeError(f"GET {path} -> {code} {d}")
        rows = d.get("result") or []
        out += rows
        if len(rows) < 100 or page >= 5:
            break
        page += 1
    return out


def metric(col, agg, label=None):
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": col},
        "aggregate": agg,
        "label": label or f"{agg} {col}",
    }


def chart_payload(name, viz_type, dsaid, params):
    # Canonical chart params: `datasource` must be the string "N__table" (the
    # shape Explore/echarts expect), not a {"id","type"} dict - a dict stored
    # in params makes every chart show "Missing parameters" and crashes the
    # dashboard grid with "Cannot read properties of undefined ('width')".
    canonical = {
        "datasource": f"{dsaid}__table",
        "adhoc_filters": [],
    }
    canonical.update(params)
    return {
        "viz_type": viz_type,
        "slice_name": name,
        "datasource_id": dsaid,
        "datasource_type": "table",
        "params": json.dumps(canonical),
    }


def main():
    code, d = call(
        "POST",
        "/api/v1/security/login",
        {"username": "admin", "password": "admin", "provider": "db", "refresh": True},
    )
    assert code == 200, d
    token = d["access_token"]
    code, d = call("GET", "/api/v1/security/csrf_token/", None, token)
    assert code == 200, d
    CSRF["token"] = d["result"]
    print("login + csrf ok")

    # --- Trino database -----------------------------------------------------
    databases = get_all("/api/v1/database", token)
    db = next((x for x in databases if x.get("database_name") == DB_NAME), None)
    if not db:
        code, d = call(
            "POST",
            "/api/v1/database/",
            {
                "database_name": DB_NAME,
                "sqlalchemy_uri": TRINO_URI,
                "engine": "trino",
                "allow_dml": True,
                "extra": json.dumps({"allows_virtual_table_explore": True}),
                "expose_in_sqllab": True,
            },
            token,
        )
        assert code in (200, 201), (code, d)
        dbid = d["id"]
    else:
        dbid = db["id"]
    # allow_dml must be True so the workshop bootstrap (DROP/CREATE/INSERT on
    # the patients dimension, docs/WORKSHOP.md) can run from SQL Lab. Heal any
    # install that recreated the DB with the read-only default.
    code, d = call("PUT", f"/api/v1/database/{dbid}", {"allow_dml": True}, token)
    assert code == 200, (code, d)
    print("database:", dbid, "(allow_dml: True)")

    # --- Dataset ------------------------------------------------------------
    datasets = get_all("/api/v1/dataset", token)
    ds = next(
        (x for x in datasets if x.get("table_name") == DS_NAME and x.get("schema") == "healthcare"),
        None,
    )
    if not ds:
        code, d = call(
            "POST",
            "/api/v1/dataset/",
            {"database": dbid, "schema": "healthcare", "table_name": DS_NAME, "owners": [1]},
            token,
        )
        assert code in (200, 201), (code, d)
        dsaid = d["id"]
    else:
        dsaid = ds["id"]
    print("dataset:", dsaid)

    # --- Charts -------------------------------------------------------------
    chart_specs = [
        chart_payload(
            "Heart rate trend (avg + max)",
            "echarts_timeseries_line",
            dsaid,
            {
                "viz_type": "echarts_timeseries_line",
                "granularity_sqla": "window_start",
                "time_grain_sqla": "PT1M",
                "time_range": "No filter",
                "metrics": [
                    metric("avg_heart_rate", "AVG", "Avg HR"),
                    metric("max_heart_rate", "MAX", "Max HR"),
                ],
                "groupby": ["patient_id"],
                "y_axis_format": "SMART_NUMBER",
                "show_legend": True,
                "line_style": "smooth",
                "opacity": 0.6,
            },
        ),
        chart_payload(
            "Min SpO2 over time",
            "echarts_timeseries_line",
            dsaid,
            {
                "viz_type": "echarts_timeseries_line",
                "granularity_sqla": "window_start",
                "time_grain_sqla": "PT1M",
                "time_range": "No filter",
                "metrics": [metric("min_spo2", "MIN", "Min SpO2")],
                "groupby": ["patient_id"],
                "y_axis_format": "SMART_NUMBER",
                "show_legend": True,
                "line_style": "smooth",
            },
        ),
        chart_payload(
            "Readings per patient (total)",
            "echarts_timeseries_bar",
            dsaid,
            {
                "viz_type": "echarts_timeseries_bar",
                "granularity_sqla": "window_start",
                "time_grain_sqla": "PT1M",
                "time_range": "No filter",
                "metrics": [metric("reading_count", "SUM", "Total readings")],
                "groupby": ["patient_id"],
                "color_scheme": "SupersetColors",
            },
        ),
    ]
    charts = get_all("/api/v1/chart", token)
    ids = []
    for spec in chart_specs:
        ex = next((c for c in charts if c.get("slice_name") == spec["slice_name"]), None)
        if ex:
            try:
                old_params = json.loads(ex.get("params") or "{}")
            except Exception:
                old_params = {}
            viz_changed = old_params.get("viz_type") != spec["viz_type"]
            stale = isinstance(old_params.get("datasource"), dict) or viz_changed
            if stale:
                code, d = call("DELETE", f"/api/v1/chart/{ex['id']}", None, token)
                assert code in (200, 204), (code, d)
                print(f"deleted stale chart {ex['id']} ({spec['slice_name']})")
                charts = [c for c in charts if c.get("id") != ex["id"]]
                ex = None
        if ex:
            ids.append(ex["id"])
            continue
        code, d = call("POST", "/api/v1/chart/", spec, token)
        assert code in (200, 201), (code, d)
        ids.append(d["id"])
    print("charts:", ids)

    # --- Dashboard + layout -------------------------------------------------
    dashboards = get_all("/api/v1/dashboard", token)
    dash = next((x for x in dashboards if x.get("dashboard_title") == DASH_TITLE), None)
    if not dash:
        code, d = call(
            "POST",
            "/api/v1/dashboard/",
            {
                "dashboard_title": DASH_TITLE,
                "slug": "vitals-live",
                "published": True,
                "json_metadata": json.dumps({"color_scheme": "SupersetColors"}),
            },
            token,
        )
        assert code in (200, 201), (code, d)
        dashid = d["id"]
    else:
        dashid = dash["id"]
    print("dashboard:", dashid)

    layout = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {
            "type": "ROOT",
            "id": "ROOT_ID",
            "children": ["DASHBOARD_GRID_ID"],
            "parents": [],
            "meta": {"width": 12, "height": 0},
        },
        "HEADER_ID": {
            "type": "HEADER",
            "id": "HEADER_ID",
            "children": [],
            "parents": ["ROOT_ID"],
            "meta": {"width": 12, "height": 0},
        },
        "DASHBOARD_GRID_ID": {
            "type": "GRID",
            "id": "DASHBOARD_GRID_ID",
            "children": ["ROW-1", "ROW-2"],
            "parents": ["ROOT_ID"],
            "meta": {"width": 12, "height": 19},
        },
        "ROW-1": {
            "type": "ROW",
            "id": "ROW-1",
            "children": ["C1", "C2"],
            "parents": ["DASHBOARD_GRID_ID"],
            "meta": {"width": 12, "height": 11},
        },
        "ROW-2": {
            "type": "ROW",
            "id": "ROW-2",
            "children": ["C3"],
            "parents": ["DASHBOARD_GRID_ID"],
            "meta": {"width": 12, "height": 8},
        },
    }
    for i, cid in enumerate(ids):
        key = f"C{i + 1}"
        layout[key] = {
            "type": "CHART",
            "id": key,
            "children": [],
            "parents": [f"ROW-{i // 2 + 1}"],
            "meta": {"chartId": cid, "width": 6 if i < 2 else 12, "height": 11 if i < 2 else 8},
        }
    code, d = call(
        "PUT",
        f"/api/v1/dashboard/{dashid}",
        {"dashboard_title": DASH_TITLE, "published": True, "position_json": json.dumps(layout)},
        token,
    )
    assert code == 200, (code, d)
    print("layout saved")

    # --- Charts attachment (dashboard_slices) -------------------------------
    if not SUPERSET_DB.exists():
        sys.exit("superset.db not found; attach charts via superset UI instead")
    with sqlite3.connect(SUPERSET_DB, timeout=10) as conn:
        cur = conn.cursor()
        ph = ",".join("?" * len(ids))
        cur.execute(
            f"DELETE FROM dashboard_slices WHERE dashboard_id=? AND slice_id NOT IN ({ph})",
            [dashid] + ids,
        )
        cur.execute("SELECT slice_id FROM dashboard_slices WHERE dashboard_id=?", (dashid,))
        existing = {row[0] for row in cur.fetchall()}
        for cid in ids:
            if cid not in existing:
                cur.execute(
                    "INSERT INTO dashboard_slices (dashboard_id, slice_id) VALUES (?, ?)",
                    (dashid, cid),
                )
        conn.commit()
    print("charts linked in dashboard_slices:", ids)

    # --- Verify -------------------------------------------------------------
    code, d = call("GET", f"/api/v1/dashboard/{dashid}/charts", None, token)
    attached = [c.get("id") for c in (d.get("result") or [])] if code == 200 else []
    print("linked charts via API:", attached)

    print()
    print("Dashboard : " + print_url(dashid))


def print_url(dashid):
    return f"http://127.0.0.1:18088/superset/dashboard/{dashid}/"


if __name__ == "__main__":
    main()
