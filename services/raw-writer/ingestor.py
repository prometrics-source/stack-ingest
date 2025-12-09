#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MQTT -> Postgres ingestor for SignalHub (diagnostics edition).

Adds:
- Per-reason drop counters + periodic metrics (log + optional MQTT publish)
- Heartbeat retained topic
- Startup schema checks with clear errors (quoting reserved names like "offset")
- Detailed subscription logs and mapping reload logs
- Debug payload sampling
- Feature flags via env: DISABLE_VALUES, DISABLE_REPUBLISH, SKIP_SCHEMA_CHECK
- Optional PAHO wire-level trace

Env:
  PG_DSN                 default postgresql://admin:admin@db:5432/prometrics
  LOG_LEVEL              INFO|DEBUG (default INFO)
  METRICS_INTERVAL_SEC   default 30
  HEARTBEAT_INTERVAL_SEC default 20
  DEBUG_SAMPLE_N         log every Nth payload body (default 0=off)
  DISABLE_VALUES         "1" to skip INSERT into data_values
  DISABLE_REPUBLISH      "1" to skip MQTT republish
  METRICS_MQTT           "1" to publish metrics to MQTT
  CLIENT_ID_PREFIX       default "raw-writer"
  SKIP_SCHEMA_CHECK      "1" to skip startup schema validation
  PAHO_TRACE             "1" to enable paho.mqtt wire-level on_log
  BROKER_RELOAD_INTERVAL_SEC  default 60 (reload v_mqtt_publishers_config)
"""

import json
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Set, Tuple

import paho.mqtt.client as mqtt
import psycopg2
from psycopg2.extras import DictCursor

# ---------------------------------------------------------------------------
# Config / logging
# ---------------------------------------------------------------------------

PG_DSN = os.getenv("PG_DSN", "postgresql://admin:admin@db:5432/prometrics")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
METRICS_INTERVAL = int(os.getenv("METRICS_INTERVAL_SEC", "30"))
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL_SEC", "20"))
DEBUG_SAMPLE_N = int(os.getenv("DEBUG_SAMPLE_N", "0"))
DISABLE_VALUES = os.getenv("DISABLE_VALUES", "0") == "1"
DISABLE_REPUBLISH = os.getenv("DISABLE_REPUBLISH", "0") == "1"
METRICS_MQTT = os.getenv("METRICS_MQTT", "0") == "1"
CLIENT_ID_PREFIX = os.getenv("CLIENT_ID_PREFIX", "raw-writer")
SKIP_SCHEMA_CHECK = os.getenv("SKIP_SCHEMA_CHECK", "0") == "1"
PAHO_TRACE = os.getenv("PAHO_TRACE", "0") == "1"

# New: how often to reload broker configs / topics
BROKER_RELOAD_INTERVAL = int(os.getenv("BROKER_RELOAD_INTERVAL_SEC", "60"))

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s",
)
logger = logging.getLogger("raw-writer")

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class BrokerConfig:
    broker_host: str
    broker_port: int
    broker_username: str | None = None
    broker_password: str | None = None
    broker_id: str | None = None
    broker_name: str | None = None
    broker_transport: str = "tcp"         # "tcp" or "websockets"
    broker_ws_path: str | None = None     # e.g. "/mqtt"
    topics: Set[str] = field(default_factory=set)
    topic_to_device: Dict[str, str] = field(default_factory=dict)

    def key(self) -> Tuple[str, int, str | None, str, str | None]:
        return (
            self.broker_host,
            self.broker_port,
            self.broker_username,
            self.broker_transport,
            self.broker_ws_path,
        )


@dataclass
class DataPointMapping:
    device_asset_id: str
    json_key: str
    data_point_id: str
    logical_asset_id: str
    tag: str | None
    scale: float
    offset: float
    data_type: str


# ---------------------------------------------------------------------------
# Postgres helpers
# ---------------------------------------------------------------------------

def create_pg_conn() -> psycopg2.extensions.connection:
    while True:
        try:
            logger.info("Connecting to Postgres...")
            conn = psycopg2.connect(PG_DSN)
            conn.autocommit = True
            logger.info("Connected to Postgres")
            return conn
        except Exception as e:
            logger.error("Failed to connect to Postgres: %s", e, exc_info=True)
            time.sleep(5)


def check_schema(conn) -> None:
    """
    Fast sanity checks for required tables/views. Quotes identifiers so
    reserved names like "offset" don't break the check.
    """
    checks = [
        ("public.mqtt_messages", ["topic", "payload", "qos", "received_at"]),
        ("public.data_values", ["time", "asset_id", "data_point_id", "value", "quality"]),
    ]
    views = [
        (
            "public.v_mqtt_publishers_config",
            [
                "device_id",
                "mqtt_topic",
                "broker_host",
                "broker_port",
                "broker_username",
                "broker_password",
                "broker_transport",
                "broker_ws_path",
                "broker_id",
                "broker_name",
            ],
        ),
        (
            "public.v_data_point_mappings_with_topic",
            [
                "device_asset_id",
                "json_key",
                "data_point_id",
                "logical_asset_id",
                "tag",
                "scale",
                "offset",
                "data_type",
            ],
        ),
        ("public.v_assets_with_path", ["asset_id", "path"]),
    ]

    def _qn(ident: str) -> str:
        return '"' + ident.replace('"', '""') + '"'

    with conn.cursor() as cur:
        # tables exist and have required columns
        for tbl, cols in checks:
            schema, name = tbl.split(".", 1)
            cur.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema=%s AND table_name=%s",
                (schema, name),
            )
            if not cur.fetchone():
                raise RuntimeError(f"Required table missing: {tbl}")

            for c in cols:
                cur.execute(
                    """
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema=%s AND table_name=%s AND column_name=%s
                    """,
                    (schema, name, c),
                )
                if not cur.fetchone():
                    raise RuntimeError(f"Column {c} missing on {tbl}")

        # views exist and expose expected columns
        for vw, cols in views:
            schema, name = vw.split(".", 1)
            cur.execute(
                "SELECT 1 FROM information_schema.views "
                "WHERE table_schema=%s AND table_name=%s",
                (schema, name),
            )
            if not cur.fetchone():
                raise RuntimeError(f"Required view missing: {vw}")

            # Single quoted SELECT to validate columns present (handles "offset")
            select_list = ", ".join(_qn(c) for c in cols)
            cur.execute(f'SELECT {select_list} FROM {schema}.{name} LIMIT 0')


def load_broker_configs() -> List[BrokerConfig]:
    conn = create_pg_conn()
    try:
        if not SKIP_SCHEMA_CHECK:
            check_schema(conn)
        else:
            logger.warning("SKIP_SCHEMA_CHECK=1 — skipping schema validation.")
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  device_id, device_name, broker_id, broker_name, mqtt_topic,
                  broker_host, broker_port, broker_username, broker_password,
                  broker_transport, broker_ws_path
                FROM public.v_mqtt_publishers_config
                """
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    configs: Dict[Tuple[str, int, str | None, str, str | None], BrokerConfig] = {}
    for (
        device_id,
        device_name,
        broker_id,
        broker_name,
        mqtt_topic,
        broker_host,
        broker_port,
        broker_username,
        broker_password,
        broker_transport,
        broker_ws_path,
    ) in rows:
        if not broker_host or not mqtt_topic:
            logger.warning(
                "Skipping device %s (%s): missing broker_host or mqtt_topic",
                device_name,
                device_id,
            )
            continue

        try:
            port = int(broker_port) if broker_port is not None else 1883
        except Exception:
            port = 1883

        transport = (broker_transport or "tcp").strip().lower()
        if transport not in ("tcp", "websockets"):
            logger.warning(
                "Device %s (%s): unknown broker_transport '%s', falling back to 'tcp'",
                device_name,
                device_id,
                broker_transport,
            )
            transport = "tcp"

        ws_path = broker_ws_path.strip() if isinstance(broker_ws_path, str) else None
        topic = str(mqtt_topic).strip()
        dev_id = str(device_id)

        if "+" in topic or "#" in topic:
            logger.warning(
                "Configured MQTT topic contains wildcard: %s (device %s). "
                "This is fine for subscription but will NOT match exact "
                "topic->device mapping for republish/data map. "
                "Ensure publisher topic is concrete in v_mqtt_publishers_config.",
                topic,
                dev_id,
            )

        key = (broker_host, port, broker_username, transport, ws_path)
        cfg = configs.get(key)
        if not cfg:
            cfg = BrokerConfig(
                broker_host=broker_host,
                broker_port=port,
                broker_username=broker_username,
                broker_password=broker_password,
                broker_id=str(broker_id) if broker_id else None,
                broker_name=broker_name,
                broker_transport=transport,
                broker_ws_path=ws_path,
            )
            configs[key] = cfg

        cfg.topics.add(topic)
        cfg.topic_to_device[topic] = dev_id

    if not configs:
        logger.warning("No MQTT publishers found in v_mqtt_publishers_config")

    for cfg in configs.values():
        logger.info(
            "Broker %s:%s (%s, transport=%s, ws_path=%s) topics: %s",
            cfg.broker_host,
            cfg.broker_port,
            cfg.broker_name or cfg.broker_id or "?",
            cfg.broker_transport,
            cfg.broker_ws_path or "",
            ", ".join(sorted(cfg.topics)),
        )
    return list(configs.values())


def load_data_point_mappings(conn) -> Dict[Tuple[str, str], DataPointMapping]:
    sql = """
        SELECT
            device_asset_id,
            json_key,
            data_point_id,
            logical_asset_id,
            tag,
            scale,
            "offset" AS offset_value,
            data_type
        FROM public.v_data_point_mappings_with_topic
    """
    mapping: Dict[Tuple[str, str], DataPointMapping] = {}
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            json_key = (row["json_key"] or "").strip()
            if not json_key:
                continue
            device_asset_id = str(row["device_asset_id"])
            key = (device_asset_id, json_key)
            mapping[key] = DataPointMapping(
                device_asset_id=device_asset_id,
                json_key=json_key,
                data_point_id=str(row["data_point_id"]),
                logical_asset_id=str(row["logical_asset_id"]),
                tag=row["tag"],
                scale=float(row["scale"]),
                offset=float(row["offset_value"]),
                data_type=str(row["data_type"] or "").upper(),
            )
    logger.info("Loaded %d data-point mappings", len(mapping))
    return mapping


def path_to_topic(path: str | None, tag: str | None) -> str | None:
    if not path or not tag:
        return None
    parts: List[str] = []
    for seg in path.split("/"):
        seg = seg.strip()
        if not seg:
            continue
        if ":" in seg:
            seg = seg.split(":", 1)[1]
        parts.append(seg)
    if not parts:
        return None
    base = "/" + "/".join(parts)
    return f"{base}/{tag}"


# ---------------------------------------------------------------------------
# MQTT worker with diagnostics
# ---------------------------------------------------------------------------


class MQTTWorker(threading.Thread):
    def __init__(self, cfg: BrokerConfig):
        name = f"MQTT-{cfg.broker_name or cfg.broker_host}"
        super().__init__(name=name, daemon=True)
        self.cfg = cfg

        transport = "websockets" if self.cfg.broker_transport == "websockets" else "tcp"
        client_id = f"{CLIENT_ID_PREFIX}-{cfg.broker_name or cfg.broker_host}-{int(time.time())}"
        self.client = mqtt.Client(client_id=client_id, transport=transport)
        self.client.enable_logger(logger)

        if PAHO_TRACE:
            def _on_log(client, userdata, level, buf):
                # Map Paho levels ~ Python logging
                lvl = logging.DEBUG if level < 20 else level
                logger.log(lvl, f"PAHO: {buf}")
            self.client.on_log = _on_log

        self.pg_conn: psycopg2.extensions.connection | None = None
        self.dp_map: Dict[Tuple[str, str], DataPointMapping] = {}
        self.topic_to_device: Dict[str, str] = dict(cfg.topic_to_device)
        self.dp_last_reload: float = 0.0

        # Diagnostics counters
        self.metrics = {
            "msgs_rx": 0,
            "msgs_db": 0,
            "datapoints_mapped": 0,
            "values_inserted": 0,
            "repub_ok": 0,
            "drop.no_device": 0,
            "drop.bad_json": 0,
            "drop.not_object": 0,
            "drop.no_dp_map": 0,
            "drop.untagged": 0,
            "drop.no_path": 0,
            "drop.db_fail": 0,
            "drop.repub_fail": 0,
        }
        self._last_metrics_flush = time.time()
        self._last_heartbeat = 0.0
        self._msg_count = 0

        # MQTT setup
        if transport == "websockets":
            ws_path = self.cfg.broker_ws_path or "/mqtt"
            self.client.ws_set_options(path=ws_path)
            logger.info(
                "Using WebSockets for %s:%s with path %s",
                self.cfg.broker_host,
                self.cfg.broker_port,
                ws_path,
            )
        if cfg.broker_username:
            self.client.username_pw_set(cfg.broker_username, cfg.broker_password or "")

        # LWT + heartbeat topic roots
        self.topic_root = f"ingestor/{(cfg.broker_name or cfg.broker_host).replace(' ', '_')}"
        self.client.will_set(
            f"{self.topic_root}/status",
            json.dumps({"state": "offline"}),
            qos=0,
            retain=True,
        )

        # Bind callbacks
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        self.client.on_subscribe = self.on_subscribe

    # ----- dynamic topic subscription --------------------------------------

    def add_or_update_topic(self, topic: str, device_id: str | None) -> None:
        """
        Add a new topic/device mapping at runtime and subscribe if possible.
        Safe to call repeatedly; duplicates are handled by set().
        """
        topic = (topic or "").strip()
        if not topic:
            return

        self.cfg.topics.add(topic)
        if device_id:
            self.topic_to_device[topic] = str(device_id)

        try:
            res, mid = self.client.subscribe(topic, qos=0)
            logger.info(
                "Dynamic subscribe %s -> result=%s mid=%s (broker %s:%s)",
                topic,
                res,
                mid,
                self.cfg.broker_host,
                self.cfg.broker_port,
            )
        except Exception as e:
            logger.error("Dynamic subscribe failed for %s: %s", topic, e, exc_info=True)

    # ----- lifecycle --------------------------------------------------------

    def run(self) -> None:
        logger.info(
            "Starting worker for broker %s:%s (%s, transport=%s, client_id=%s)",
            self.cfg.broker_host,
            self.cfg.broker_port,
            self.cfg.broker_name or self.cfg.broker_id or "?",
            self.cfg.broker_transport,
            self.client._client_id.decode(),
        )

        self.pg_conn = create_pg_conn()
        if not SKIP_SCHEMA_CHECK:
            try:
                check_schema(self.pg_conn)
            except Exception as e:
                logger.error("Schema check failed: %s", e, exc_info=True)
                # Continue so we still emit heartbeat/metrics; maps may be empty.

        self.dp_map = load_data_point_mappings(self.pg_conn)
        self.dp_last_reload = time.time()

        while True:
            try:
                logger.info(
                    "Connecting MQTT client to %s:%s ...",
                    self.cfg.broker_host,
                    self.cfg.broker_port,
                )
                self.client.connect(self.cfg.broker_host, self.cfg.broker_port, keepalive=60)
                self.client.loop_forever()
            except Exception as e:
                logger.error(
                    "MQTT connection error for %s:%s: %s",
                    self.cfg.broker_host,
                    self.cfg.broker_port,
                    e,
                    exc_info=True,
                )
                time.sleep(5)

    # ----- MQTT callbacks ---------------------------------------------------

    def on_connect(self, client: mqtt.Client, userdata, flags, rc):
        logger.info(
            "Connected rc=%s to %s:%s; subscribing to %d topics",
            rc,
            self.cfg.broker_host,
            self.cfg.broker_port,
            len(self.cfg.topics),
        )
        if rc == 0:
            # Heartbeat online
            try:
                client.publish(
                    f"{self.topic_root}/status",
                    json.dumps({"state": "online", "ts": datetime.now(timezone.utc).isoformat()}),
                    qos=0,
                    retain=True,
                )
            except Exception:
                pass
            for t in sorted(self.cfg.topics):
                res, mid = client.subscribe(t, qos=0)
                logger.info("Subscribe %s -> result=%s mid=%s", t, res, mid)
        else:
            logger.error(
                "Failed to connect to MQTT broker %s:%s (rc=%s)",
                self.cfg.broker_host,
                self.cfg.broker_port,
                rc,
            )

    def on_subscribe(self, client, userdata, mid, granted_qos, properties=None):
        logger.debug("on_subscribe mid=%s granted_qos=%s", mid, granted_qos)

    def _maybe_flush_metrics(self):
        now = time.time()
        if now - self._last_metrics_flush >= METRICS_INTERVAL:
            self._last_metrics_flush = now
            summary = dict(self.metrics)
            summary["dp_map_size"] = len(self.dp_map)
            logger.info("METRICS %s", json.dumps(summary, separators=(",", ":")))
            if METRICS_MQTT:
                try:
                    self.client.publish(
                        f"{self.topic_root}/metrics",
                        json.dumps({**summary, "ts": datetime.now(timezone.utc).isoformat()}),
                        qos=0,
                        retain=False,
                    )
                except Exception as e:
                    logger.warning("Failed to publish metrics: %s", e)

        # heartbeat
        if now - self._last_heartbeat >= HEARTBEAT_INTERVAL:
            self._last_heartbeat = now
            try:
                self.client.publish(
                    f"{self.topic_root}/heartbeat",
                    json.dumps({"ts": datetime.now(timezone.utc).isoformat()}),
                    qos=0,
                    retain=False,
                )
            except Exception:
                pass

    def on_message(self, client: mqtt.Client, userdata, msg: mqtt.MQTTMessage):
        self.metrics["msgs_rx"] += 1
        self._msg_count += 1

        topic = msg.topic
        qos = msg.qos

        try:
            payload_text = msg.payload.decode("utf-8", errors="ignore")
        except Exception:
            payload_text = ""

        if DEBUG_SAMPLE_N and (self._msg_count % DEBUG_SAMPLE_N == 0):
            logger.info("Sample payload @ %s: %s", topic, payload_text[:512])

        # Ensure PG connection
        if self.pg_conn is None or self.pg_conn.closed:
            logger.warning("Postgres connection lost in worker, reconnecting...")
            self.pg_conn = create_pg_conn()
            if not SKIP_SCHEMA_CHECK:
                try:
                    check_schema(self.pg_conn)
                except Exception as e:
                    logger.error("Schema check failed after reconnect: %s", e, exc_info=True)
            self.dp_map = load_data_point_mappings(self.pg_conn)
            self.dp_last_reload = time.time()

        # Periodic reload of dp_map
        now_ts = time.time()
        if now_ts - self.dp_last_reload > 60:
            try:
                self.dp_map = load_data_point_mappings(self.pg_conn)
                self.dp_last_reload = now_ts
            except Exception as e:
                logger.error("Error reloading data point mappings: %s", e, exc_info=True)

        now = datetime.now(timezone.utc)

        device_id = self.topic_to_device.get(topic)
        if not device_id:
            self.metrics["drop.no_device"] += 1
            logger.debug(
                "Message on %s has no device mapping (exact match). "
                "If config uses wildcards, exact topic->device map will miss. "
                "Store the concrete topic in v_mqtt_publishers_config.",
                topic,
            )
            # Still store raw message
            try:
                with self.pg_conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO public.mqtt_messages (topic, payload, qos, received_at)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (topic, payload_text, int(qos), now),
                    )
                    self.metrics["msgs_db"] += 1
            except Exception as e:
                self.metrics["drop.db_fail"] += 1
                logger.error("DB insert (unknown device) failed: %s", e, exc_info=True)
            self._maybe_flush_metrics()
            return

        # Parse JSON
        try:
            data = json.loads(payload_text) if payload_text else {}
        except json.JSONDecodeError:
            self.metrics["drop.bad_json"] += 1
            logger.warning("Invalid JSON on topic %s: %r", topic, payload_text[:200])
            data = {}

        if not isinstance(data, dict):
            self.metrics["drop.not_object"] += 1
            logger.warning("Expected JSON object for topic %s, got %r", topic, type(data))
            data = {}

        message_id = None
        try:
            with self.pg_conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO public.mqtt_messages (topic, payload, qos, received_at)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id
                    """,
                    (topic, payload_text, int(qos), now),
                )
                row = cur.fetchone()
                if row:
                    message_id = str(row[0])
                self.metrics["msgs_db"] += 1
        except Exception as e:
            self.metrics["drop.db_fail"] += 1
            logger.error("DB insert mqtt_messages failed: %s", e, exc_info=True)

        if not data or not self.dp_map:
            self._maybe_flush_metrics()
            return

        # Map & scale
        dv_rows: List[Tuple[datetime, str, str, float, int]] = []
        asset_ids_for_paths: Set[str] = set()
        per_key_publish_payloads: List[Tuple[str, str | None, str | None, float]] = []

        for key, raw_val in data.items():
            map_key = (str(device_id), str(key))
            mp = self.dp_map.get(map_key)
            if not mp:
                self.metrics["drop.no_dp_map"] += 1
                continue

            if not mp.tag:
                self.metrics["drop.untagged"] += 1
                continue

            # numeric conversion
            try:
                if mp.data_type in ("BOOL", "BOOLEAN"):
                    base_val = 1.0 if (
                        raw_val if isinstance(raw_val, bool) else float(raw_val) != 0.0
                    ) else 0.0
                else:
                    base_val = float(raw_val)
            except Exception:
                self.metrics["drop.no_dp_map"] += 1
                logger.debug("Cannot convert %r for %s.%s", raw_val, device_id, key)
                continue

            scaled = base_val * mp.scale + mp.offset
            quality = 192  # good

            if not DISABLE_VALUES:
                dv_rows.append((now, mp.logical_asset_id, mp.data_point_id, scaled, quality))

            asset_ids_for_paths.add(mp.logical_asset_id)
            per_key_publish_payloads.append(
                (mp.logical_asset_id, mp.data_point_id, mp.tag, scaled)
            )
            self.metrics["datapoints_mapped"] += 1

        # Insert values
        if dv_rows and not DISABLE_VALUES:
            try:
                with self.pg_conn.cursor() as cur:
                    cur.executemany(
                        """
                        INSERT INTO public.data_values ("time", asset_id, data_point_id, value, quality)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        dv_rows,
                    )
                self.metrics["values_inserted"] += len(dv_rows)
            except Exception as e:
                self.metrics["drop.db_fail"] += 1
                logger.error("Insert into data_values failed: %s", e, exc_info=True)

        # Fetch paths
        asset_paths: Dict[str, str] = {}
        if asset_ids_for_paths:
            try:
                with self.pg_conn.cursor(cursor_factory=DictCursor) as cur:
                    cur.execute(
                        """
                        SELECT asset_id, path
                        FROM public.v_assets_with_path
                        WHERE asset_id::text = ANY(%s)
                        """,
                        (list(asset_ids_for_paths),),
                    )
                    for row in cur.fetchall():
                        asset_paths[str(row["asset_id"])] = row["path"]
            except Exception as e:
                logger.error("Fetch v_assets_with_path failed: %s", e, exc_info=True)

        # Republish
        if not DISABLE_REPUBLISH:
            for asset_id, dp_id, tag, scaled in per_key_publish_payloads:
                path = asset_paths.get(asset_id)
                out_topic = path_to_topic(path, tag)
                if not out_topic:
                    self.metrics["drop.no_path"] += 1
                    continue

                payload = {
                    "value": scaled,
                    "ts": now.isoformat(),
                    "quality": 192,
                    "asset_id": asset_id,
                    "data_point_id": dp_id,
                }
                if message_id:
                    payload["source_message_id"] = message_id

                try:
                    self.client.publish(out_topic, json.dumps(payload), qos=0, retain=False)
                    self.metrics["repub_ok"] += 1
                except Exception as e:
                    self.metrics["drop.repub_fail"] += 1
                    logger.error("Republish to %s failed: %s", out_topic, e, exc_info=True)

        self._maybe_flush_metrics()

    def on_disconnect(self, client: mqtt.Client, userdata, rc):
        if rc != 0:
            logger.warning(
                "Unexpected MQTT disconnect from %s:%s (rc=%s)",
                self.cfg.broker_host,
                self.cfg.broker_port,
                rc,
            )
        try:
            client.publish(
                f"{self.topic_root}/status",
                json.dumps({"state": "offline"}),
                qos=0,
                retain=True,
            )
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    logger.info("raw-writer starting up")
    logger.info("Using PG_DSN=%s", PG_DSN)
    logger.info(
        "Flags: DISABLE_VALUES=%s DISABLE_REPUBLISH=%s METRICS_MQTT=%s DEBUG_SAMPLE_N=%s",
        DISABLE_VALUES,
        DISABLE_REPUBLISH,
        METRICS_MQTT,
        DEBUG_SAMPLE_N,
    )
    logger.info("BROKER_RELOAD_INTERVAL_SEC=%s", BROKER_RELOAD_INTERVAL)

    # Initial load of broker configs
    configs = load_broker_configs()
    if not configs:
        logger.warning("No broker configs found; staying idle. Check v_mqtt_publishers_config.")

    # Keep workers keyed by BrokerConfig.key()
    workers: Dict[Tuple[str, int, str | None, str, str | None], MQTTWorker] = {}

    for cfg in configs:
        if not cfg.topics:
            logger.warning(
                "Broker %s:%s has no topics; skipping worker",
                cfg.broker_host,
                cfg.broker_port,
            )
            continue
        key = cfg.key()
        if key in workers:
            continue
        w = MQTTWorker(cfg)
        workers[key] = w
        w.start()

    # Background thread: periodically reload broker configs / topics
    def reload_brokers_loop():
        while True:
            time.sleep(BROKER_RELOAD_INTERVAL)
            try:
                new_cfgs = load_broker_configs()
            except Exception as e:
                logger.error(
                    "reload_brokers: load_broker_configs failed: %s",
                    e,
                    exc_info=True,
                )
                continue

            by_key: Dict[Tuple[str, int, str | None, str, str | None], BrokerConfig] = {
                cfg.key(): cfg for cfg in new_cfgs
            }

            # Start workers for any NEW brokers
            for key, cfg in by_key.items():
                if key not in workers:
                    if not cfg.topics:
                        continue
                    logger.info(
                        "Spawning new MQTT worker for broker %s:%s due to config reload",
                        cfg.broker_host,
                        cfg.broker_port,
                    )
                    w = MQTTWorker(cfg)
                    workers[key] = w
                    w.start()
                    continue

                # Existing broker: add/update topics + device mappings
                w = workers[key]
                for topic, dev in cfg.topic_to_device.items():
                    if topic not in w.cfg.topics:
                        logger.info(
                            "Adding new topic %s to existing broker %s:%s",
                            topic,
                            cfg.broker_host,
                            cfg.broker_port,
                        )
                    w.add_or_update_topic(topic, dev)

            # (Optional) We could log brokers that disappeared, but we do not
            # shut workers down automatically in this version.

    if BROKER_RELOAD_INTERVAL > 0:
        t = threading.Thread(
            target=reload_brokers_loop,
            name="BrokerReloader",
            daemon=True,
        )
        t.start()

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("raw-writer shutting down due to KeyboardInterrupt")


if __name__ == "__main__":
    main()
