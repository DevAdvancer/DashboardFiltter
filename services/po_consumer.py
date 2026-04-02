import json
import logging
import os
import re
import threading
import time
from datetime import datetime

import pytz
from supabase import create_client
from services.team_management import normalize_email_like

LOGGER = logging.getLogger(__name__)

_CONSUMER_LOCK = threading.Lock()
_CONSUMER_THREAD = None


def clean_text(text):
    if not text:
        return ""
    return re.sub(r"\s+", " ", str(text).replace("\xa0", " ")).strip()


def extract(pattern, text):
    match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    if match.lastindex:
        return match.group(1).strip()
    return match.group(0).strip()


def extract_int(pattern, text):
    value = extract(pattern, text)
    return int(value) if value and value.isdigit() else None


def extract_date(pattern, text):
    value = extract(pattern, text)
    if not value:
        return None
    try:
        return datetime.strptime(value, "%m/%d/%Y").date().isoformat()
    except ValueError:
        return None


def extract_rate(text):
    match = re.search(
        r"Rate:\s*(.*?)\s*(Signup Date|Sales|Lead By|Marketing|$)",
        text,
        re.IGNORECASE | re.DOTALL,
    )
    return clean_text(match.group(1)) if match else None


def convert_to_est(date_str):
    if not date_str:
        return None

    try:
        normalized = str(date_str).replace("Z", "")
        dt_utc = datetime.fromisoformat(normalized).replace(tzinfo=pytz.UTC)
        est = pytz.timezone("America/New_York")
        return dt_utc.astimezone(est).isoformat()
    except ValueError:
        return None


def fallback_kafka_time(message):
    timestamp = message.timestamp()[1]
    if timestamp:
        dt_utc = datetime.utcfromtimestamp(timestamp / 1000).replace(tzinfo=pytz.UTC)
        est = pytz.timezone("America/New_York")
        return dt_utc.astimezone(est).isoformat()
    return None


def parse_body(body):
    body = clean_text(body)
    preview = extract(r"Hello Team,(.*?)Name", body)
    preview = preview.strip() if preview else None

    return {
        "candidate_name": extract(r"Candidate:\s*(.*?)\s*SST", body),
        "email": extract(r"Email ID\s*(.*?)\s*Full Address", body),
        "phone": extract(r"Phone Number\s*(.*?)\s*Email", body),
        "location": extract(r"Location\s*(.*?)\s*PO Count", body),
        "position": extract(r"Position that Applied:\s*(.*?)\s*Job Location", body),
        "job_location": extract(r"Job Location:\s*(.*?)\s*Implementation", body),
        "client": extract(r"End Client\s*(.*?)\s*Vendor Details", body),
        "rate": extract_rate(body),
        "signup_date": extract_date(r"Signup Date:\s*(\d{2}/\d{2}/\d{4})", body),
        "po_total": extract_int("TOTAL\\s*(?:-|\\u2013)\\s*(\\d+)", body),
        "po_amd": extract_int("AMD\\s*(?:-|\\u2013)\\s*(\\d+)", body),
        "po_ggr": extract_int("GGR\\s*(?:-|\\u2013)\\s*(\\d+)", body),
        "po_lko": extract_int("LKO\\s*(?:-|\\u2013)\\s*(\\d+)", body),
        "interview_support_by": extract(r"Support by\s*(.*?)\s*Team Lead", body),
        "team_lead": extract(r"Interview Support.*?Team Lead\s*(.*?)\s*Manager", body),
        "manager": extract(r"Interview Support.*?Manager\s*(.*?)\s*Marketing", body),
        "preview_text": preview,
    }


def truthy(value):
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


class POConsumerService:
    def __init__(self):
        self.kafka_broker = os.getenv("PO_KAFKA_BROKER", "")
        self.kafka_username = os.getenv("PO_KAFKA_USERNAME", "")
        self.kafka_password = os.getenv("PO_KAFKA_PASSWORD", "")
        self.topic = os.getenv("PO_KAFKA_TOPIC", "PO")
        self.group_id = os.getenv("PO_KAFKA_GROUP_ID", "po-backfill-v1")
        self.auto_offset_reset = os.getenv("PO_KAFKA_AUTO_OFFSET_RESET", "earliest")
        self.supabase_url, self.supabase_key = self._get_supabase_config()

    @staticmethod
    def _get_supabase_config():
        return (
            os.getenv("PO_SUPABASE_URL") or os.getenv("SUPABASE_URL", ""),
            os.getenv("PO_SUPABASE_KEY") or os.getenv("SUPABASE_KEY", ""),
        )

    def is_configured(self):
        required = [
            self.kafka_broker,
            self.kafka_username,
            self.kafka_password,
            self.supabase_url,
            self.supabase_key,
        ]
        return all(required)

    def supabase_client(self):
        return create_client(self.supabase_url, self.supabase_key)

    def fetch_group_id(self):
        return os.getenv("PO_KAFKA_FETCH_GROUP_ID", f"{self.group_id}-manual")

    def consumer_config(self, group_id=None):
        return {
            "bootstrap.servers": self.kafka_broker,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": self.kafka_username,
            "sasl.password": self.kafka_password,
            "group.id": group_id or self.group_id,
            "auto.offset.reset": self.auto_offset_reset,
            "enable.auto.commit": False,
        }

    def matches_filter(self, payload):
        sender = clean_text(payload.get("sender"))
        subject = clean_text(payload.get("subject"))
        return sender.lower() == "rgahlot@silverspaceinc.com" and "po details" in subject.lower()

    def build_record(self, payload, message):
        body = payload.get("body", "")
        received_raw = payload.get("receivedDateTime", "")
        extracted = parse_body(body)
        extracted["email"] = normalize_email_like(extracted.get("email"))
        extracted["interview_support_by"] = normalize_email_like(extracted.get("interview_support_by"))
        extracted["team_lead"] = normalize_email_like(extracted.get("team_lead"))
        extracted["manager"] = normalize_email_like(extracted.get("manager"))

        return {
            "sender": normalize_email_like(payload.get("sender", "")),
            "subject": payload.get("subject", ""),
            "body": body,
            "to_field": normalize_email_like(payload.get("to")),
            "cc_field": normalize_email_like(payload.get("cc")),
            "received_at": convert_to_est(received_raw) or fallback_kafka_time(message),
            **extracted,
            "raw_json": payload,
        }

    def record_exists(self, supabase, record):
        received_at = record.get("received_at")
        candidate_name = clean_text(record.get("candidate_name"))
        sender = clean_text(record.get("sender"))
        subject = clean_text(record.get("subject"))

        if not received_at or not sender or not subject:
            return False

        query = (
            supabase.table("po_details")
            .select("id")
            .eq("sender", sender)
            .eq("subject", subject)
            .eq("received_at", received_at)
            .limit(5)
        )

        if candidate_name:
            query = query.eq("candidate_name", candidate_name)

        response = query.execute()
        return bool(response.data)

    def handle_message(self, message, supabase):
        try:
            payload = json.loads(message.value().decode("utf-8"))
        except Exception:
            LOGGER.exception("Skipping PO Kafka message because JSON decoding failed.")
            return {"status": "invalid_json", "commit": True}

        if not self.matches_filter(payload):
            return {"status": "ignored", "commit": True}

        record = self.build_record(payload, message)

        try:
            if self.record_exists(supabase, record):
                LOGGER.info(
                    "Skipping duplicate PO record for candidate '%s'.",
                    clean_text(record.get("candidate_name")) or "unknown",
                )
                return {"status": "duplicate", "commit": True}

            supabase.table("po_details").insert(record).execute()
            LOGGER.info(
                "Inserted PO record for candidate '%s'.",
                clean_text(record.get("candidate_name")) or "unknown",
            )
            return {"status": "inserted", "commit": True}
        except Exception:
            LOGGER.exception(
                "Failed to write PO record for candidate '%s'.",
                clean_text(record.get("candidate_name")) or "unknown",
            )
            return {"status": "error", "commit": False}

    def consume_batch(
        self,
        *,
        group_id=None,
        max_wait_seconds=None,
        max_messages=None,
        idle_poll_limit=None,
    ):
        from confluent_kafka import Consumer

        max_wait_seconds = max_wait_seconds or int(os.getenv("PO_FETCH_MAX_WAIT_SECONDS", "8"))
        max_messages = max_messages or int(os.getenv("PO_FETCH_MAX_MESSAGES", "200"))
        idle_poll_limit = idle_poll_limit or int(os.getenv("PO_FETCH_IDLE_POLLS", "3"))

        stats = {
            "checked": 0,
            "inserted": 0,
            "duplicate": 0,
            "ignored": 0,
            "invalid_json": 0,
            "error": 0,
            "committed": 0,
        }

        consumer = Consumer(self.consumer_config(group_id=group_id or self.fetch_group_id()))
        consumer.subscribe([self.topic])
        supabase = self.supabase_client()
        start_time = time.monotonic()
        idle_polls = 0

        try:
            while stats["checked"] < max_messages:
                if time.monotonic() - start_time >= max_wait_seconds:
                    break

                message = consumer.poll(1.0)
                if message is None:
                    idle_polls += 1
                    if idle_polls >= idle_poll_limit:
                        break
                    continue

                idle_polls = 0

                if message.error():
                    LOGGER.error("PO Kafka fetch error: %s", message.error())
                    stats["error"] += 1
                    continue

                stats["checked"] += 1
                result = self.handle_message(message, supabase)
                status = result["status"]
                stats[status] += 1

                if result["commit"]:
                    consumer.commit(message=message, asynchronous=False)
                    stats["committed"] += 1
        finally:
            consumer.close()

        return stats

    def run_forever(self):
        from confluent_kafka import Consumer

        consumer = Consumer(self.consumer_config())
        consumer.subscribe([self.topic])
        supabase = self.supabase_client()

        LOGGER.info("PO Kafka consumer started for topic '%s'.", self.topic)

        try:
            while True:
                message = consumer.poll(1.0)
                if message is None:
                    continue

                if message.error():
                    LOGGER.error("PO Kafka consumer error: %s", message.error())
                    continue

                result = self.handle_message(message, supabase)
                if result["commit"]:
                    consumer.commit(message=message, asynchronous=False)
        except Exception:
            LOGGER.exception("PO Kafka consumer stopped unexpectedly.")
        finally:
            consumer.close()


def start_po_consumer():
    global _CONSUMER_THREAD

    if not truthy(os.getenv("PO_CONSUMER_ENABLED", "true")):
        LOGGER.info("PO Kafka consumer is disabled.")
        return None

    if os.getenv("VERCEL"):
        LOGGER.info("Skipping PO Kafka consumer startup in the Vercel runtime.")
        return None

    if truthy(os.getenv("FLASK_DEBUG", "true")) and os.getenv("WERKZEUG_RUN_MAIN") != "true":
        return None

    with _CONSUMER_LOCK:
        if _CONSUMER_THREAD and _CONSUMER_THREAD.is_alive():
            return _CONSUMER_THREAD

        service = POConsumerService()
        if not service.is_configured():
            LOGGER.warning("PO Kafka consumer is missing configuration and was not started.")
            return None

        thread = threading.Thread(
            target=service.run_forever,
            name="po-kafka-consumer",
            daemon=True,
        )
        thread.start()
        _CONSUMER_THREAD = thread
        return thread
