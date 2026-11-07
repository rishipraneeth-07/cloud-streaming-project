import os
import json
import base64
import urllib.request
import urllib.error

BASE = "http://localhost:3000"
UID = "cloud-pipeline"
USER = os.getenv("GRAFANA_USER", "admin")
PASSWORD = os.getenv("GRAFANA_PASSWORD", "admin")

token = base64.b64encode(f"{USER}:{PASSWORD}".encode()).decode()

def api(path, method="GET", payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        BASE + path,
        data=data,
        method=method,
        headers={
            "Authorization": f"Basic {token}",
            "Content-Type": "application/json"
        }
    )
    try:
        with urllib.request.urlopen(req) as response:
            body = response.read()
            return json.loads(body) if body else {}
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"Grafana API error {e.code}: {e.read().decode()}")

result = api(f"/api/dashboards/uid/{UID}")
dashboard = result["dashboard"]
folder_id = result["meta"].get("folderId", 0)

datasources = api("/api/datasources")
prometheus = next(
    (ds for ds in datasources if ds.get("type") == "prometheus"),
    None
)

if not prometheus:
    raise RuntimeError("No Prometheus datasource found in Grafana.")

ds = {"type": "prometheus", "uid": prometheus["uid"]}
panels = dashboard.setdefault("panels", [])

# Remove duplicate failed-record panels, keeping the first.
seen = False
cleaned = []
for panel in panels:
    if panel.get("title") == "Total Records Failed":
        if seen:
            continue
        seen = True
    cleaned.append(panel)
panels = cleaned

next_id = max((p.get("id", 0) for p in panels), default=0) + 1
next_y = max(
    (p.get("gridPos", {}).get("y", 0) +
     p.get("gridPos", {}).get("h", 8) for p in panels),
    default=0
) + 1

def target(expr, ref="A", legend="{{__name__}}", fmt="time_series"):
    return {
        "expr": expr,
        "refId": ref,
        "legendFormat": legend,
        "instant": True,
        "range": False,
        "format": fmt
    }

def add_panel(title, panel_type, targets, description="", unit="short"):
    global next_id, next_y

    if any(p.get("title") == title for p in panels):
        print(f"Already exists: {title}")
        return

    width = 12
    x = 0 if len([p for p in panels if p.get("title", "").startswith("MinIO Object Count") or p.get("title", "").startswith("Estimated Storage Cost") or p.get("title", "").startswith("Service Health") or p.get("title", "").startswith("Kafka Monitoring") or p.get("title", "").startswith("Airflow Health")]) % 2 == 0 else 12

    if x == 0 and title != "MinIO Object Count by Tier":
        next_y += 9

    panel = {
        "id": next_id,
        "type": panel_type,
        "title": title,
        "description": description,
        "datasource": ds,
        "gridPos": {"h": 8, "w": width, "x": x, "y": next_y},
        "targets": targets,
        "fieldConfig": {
            "defaults": {"unit": unit},
            "overrides": []
        },
        "options": {}
    }

    if panel_type == "stat":
        panel["options"] = {
            "reduceOptions": {
                "calcs": ["lastNotNull"],
                "fields": "",
                "values": False
            },
            "orientation": "auto",
            "textMode": "auto",
            "colorMode": "value",
            "graphMode": "none",
            "justifyMode": "auto"
        }
    elif panel_type == "bargauge":
        panel["options"] = {
            "reduceOptions": {
                "calcs": ["lastNotNull"],
                "fields": "",
                "values": False
            },
            "orientation": "horizontal",
            "displayMode": "basic",
            "showUnfilled": True
        }
    elif panel_type == "table":
        panel["options"] = {
            "showHeader": True,
            "cellHeight": "sm",
            "footer": {"show": False}
        }

    panels.append(panel)
    next_id += 1
    print(f"Added: {title}")

add_panel(
    "MinIO Object Count by Tier",
    "bargauge",
    [target("project_minio_bucket_objects", legend="{{bucket}}")],
    "Current object count in each MinIO storage bucket.",
    "short"
)

add_panel(
    "Estimated Storage Cost by Tier",
    "bargauge",
    [target("project_minio_estimated_monthly_cost_usd", legend="{{bucket}}")],
    "Illustrative monthly estimates based on configured rates, not actual cloud billing.",
    "currencyUSD"
)

add_panel(
    "Service Health",
    "table",
    [target(
        'project_minio_up or project_postgres_up or project_kafka_up',
        legend="{{__name__}}",
        fmt="table"
    )],
    "Availability metrics: 1 means reachable; 0 means unavailable."
)

add_panel(
    "Kafka Monitoring",
    "stat",
    [
        target("project_kafka_up", "A", "Kafka availability"),
        target("project_kafka_topics", "B", "Topic count")
    ],
    "Kafka availability and number of topics."
)

add_panel(
    "Airflow Health",
    "stat",
    [target("project_airflow_component_up", legend="{{component}}")],
    "Airflow component health. 1 means healthy; 0 means unhealthy."
)

dashboard["panels"] = panels

payload = {
    "dashboard": dashboard,
    "folderId": folder_id,
    "overwrite": True,
    "message": "Add pipeline monitoring panels"
}

saved = api("/api/dashboards/db", "POST", payload)
print("\nDashboard updated successfully.")
print("Dashboard URL:", BASE + f"/d/{UID}")
print("Panels currently configured:", len(panels))
