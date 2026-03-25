from collections import Counter
from datetime import datetime, timezone
import os

from flask import Blueprint, flash, jsonify, redirect, render_template, request, url_for
from supabase import create_client
from po_security import (
    current_request_next_url,
    clear_current_po_access,
    enforce_po_filter_values,
    filter_records_for_po_access,
    find_po_pin_profile,
    get_current_po_access,
    get_po_lock,
    is_safe_next_url,
    po_access_can_sync,
    po_pin_security_enabled,
    set_current_po_access,
)
from services.po_consumer import POConsumerService

po_bp = Blueprint("po", __name__)

FIELDS = """
id,
candidate_name,
email,
phone,
location,
position,
job_location,
client,
rate,
signup_date,
interview_support_by,
team_lead,
manager,
preview_text,
received_at,
created_at
"""

MAX_FETCH_ROWS = max(int(os.getenv("PO_MAX_FETCH_ROWS", 5000)), 1)
FETCH_BATCH_SIZE = max(min(int(os.getenv("PO_FETCH_BATCH_SIZE", 1000)), MAX_FETCH_ROWS), 1)
PO_NAME_ALIASES = {
    "Anusree Vasudevan": "Anushree Vasudevan",
    "Prateek Navariya": "Prateek Narvariya",
    "Rujuwal Garag": "Rujuwal Garg",
}
PO_PLACEHOLDER_NAMES = {"n/a", "na", "not applicable", "none", "null", "nil"}


def get_supabase_client():
    supabase_url = os.getenv("PO_SUPABASE_URL") or os.getenv("SUPABASE_URL", "")
    supabase_key = os.getenv("PO_SUPABASE_KEY") or os.getenv("SUPABASE_KEY", "")

    if not supabase_url or not supabase_key:
        raise RuntimeError("Supabase configuration is missing for the PO dashboard.")
    return create_client(supabase_url, supabase_key)


def clean_text(value):
    if value is None:
        return ""
    return " ".join(str(value).replace("\xa0", " ").split())


def display_value(value, fallback="Unassigned"):
    cleaned = clean_text(value)
    return cleaned or fallback


def normalize_person_name(value):
    cleaned = clean_text(value)
    if not cleaned:
        return ""
    if cleaned.lower() in PO_PLACEHOLDER_NAMES:
        return ""
    titled = cleaned.title()
    return PO_NAME_ALIASES.get(titled, titled)


def parse_datetime(value):
    if not value:
        return None

    text = clean_text(value)
    if not text:
        return None

    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"

    try:
        return datetime.fromisoformat(text)
    except ValueError:
        pass

    for fmt in (
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue

    return None


def sort_timestamp(dt):
    if not dt:
        return 0.0
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def month_label(month_key):
    if not month_key:
        return "Unknown"
    try:
        return datetime.strptime(month_key, "%Y-%m").strftime("%B %Y")
    except ValueError:
        return month_key


def summarize_dates(date_values):
    known_dates = sorted({date for date in date_values if date and date != "Unknown"})
    has_unknown = any(not date or date == "Unknown" for date in date_values)

    if known_dates:
        earliest_date = known_dates[0]
        latest_date = known_dates[-1]
        if len(known_dates) == 1:
            label = known_dates[0]
        else:
            label = f"{earliest_date} to {latest_date}"

        if has_unknown:
            label = f"{label} + Unknown"

        return {
            "earliest_date": earliest_date,
            "latest_date": latest_date,
            "date_label": label,
            "date_count": len(known_dates) + (1 if has_unknown else 0),
        }

    return {
        "earliest_date": "Unknown",
        "latest_date": "Unknown",
        "date_label": "Unknown",
        "date_count": 1 if date_values else 0,
    }


def enrich_record(row):
    received_dt = parse_datetime(row.get("received_at"))
    created_dt = parse_datetime(row.get("created_at"))
    effective_dt = received_dt or created_dt

    enriched = dict(row)
    enriched["record_id"] = str(row.get("id", ""))
    enriched["candidate_name_display"] = display_value(row.get("candidate_name"), "N/A")
    enriched["expert_name"] = display_value(normalize_person_name(row.get("interview_support_by")))
    enriched["manager_name"] = display_value(normalize_person_name(row.get("manager")))
    enriched["team_lead_name"] = display_value(normalize_person_name(row.get("team_lead")))
    enriched["received_at_display"] = (
        effective_dt.strftime("%Y-%m-%d %I:%M %p")
        if effective_dt
        else display_value(row.get("received_at") or row.get("created_at"), "N/A")
    )
    enriched["created_at_display"] = (
        created_dt.strftime("%Y-%m-%d %I:%M %p")
        if created_dt
        else display_value(row.get("created_at"), "N/A")
    )
    enriched["mail_date"] = effective_dt.strftime("%Y-%m-%d") if effective_dt else ""
    enriched["month_key"] = effective_dt.strftime("%Y-%m") if effective_dt else ""
    enriched["month_label"] = month_label(enriched["month_key"]) if effective_dt else "Unknown"
    enriched["sort_ts"] = sort_timestamp(effective_dt)
    return enriched


def fetch_po_records(supabase):
    records = []
    start = 0

    while start < MAX_FETCH_ROWS:
        end = min(start + FETCH_BATCH_SIZE - 1, MAX_FETCH_ROWS - 1)
        response = (
            supabase.table("po_details")
            .select(FIELDS)
            .order("received_at", desc=True)
            .order("created_at", desc=True)
            .range(start, end)
            .execute()
        )

        batch = response.data or []
        if not batch:
            break

        records.extend(batch)
        if len(batch) < FETCH_BATCH_SIZE:
            break

        start += FETCH_BATCH_SIZE

    enriched_records = [enrich_record(row) for row in records]
    enriched_records.sort(key=lambda row: row["sort_ts"], reverse=True)
    return enriched_records


def normalize_month_filter(raw_month, raw_year):
    month_value = clean_text(raw_month)
    year_value = clean_text(raw_year)

    if month_value and month_value.isdigit() and year_value and year_value.isdigit():
        month_num = int(month_value)
        if 1 <= month_num <= 12:
            return f"{int(year_value):04d}-{month_num:02d}"

    return month_value


def build_summary_rows(records):
    grouped = {}

    for record in records:
        key = (
            record["expert_name"],
            record["manager_name"],
            record["team_lead_name"],
        )

        if key not in grouped:
            grouped[key] = {
                "expert_name": record["expert_name"],
                "manager_name": record["manager_name"],
                "team_lead_name": record["team_lead_name"],
                "po_count": 0,
                "candidate_preview": [],
                "latest_sort_ts": 0.0,
                "date_values": set(),
            }

        grouped[key]["po_count"] += 1

        if record["sort_ts"] >= grouped[key]["latest_sort_ts"]:
            grouped[key]["latest_sort_ts"] = record["sort_ts"]

        record_date = record["mail_date"] or "Unknown"
        grouped[key]["date_values"].add(record_date)

        candidate_name = clean_text(record.get("candidate_name"))
        if (
            candidate_name
            and candidate_name not in grouped[key]["candidate_preview"]
            and len(grouped[key]["candidate_preview"]) < 3
        ):
            grouped[key]["candidate_preview"].append(candidate_name)

    rows = list(grouped.values())

    for row in rows:
        row.update(summarize_dates(row["date_values"]))

    rows.sort(key=lambda row: row["team_lead_name"].lower())
    rows.sort(key=lambda row: row["manager_name"].lower())
    rows.sort(key=lambda row: row["expert_name"].lower())
    rows.sort(key=lambda row: row["po_count"], reverse=True)
    rows.sort(key=lambda row: row["latest_sort_ts"], reverse=True)
    return rows


def serialize_record(record):
    keys = [
        "record_id",
        "candidate_name",
        "candidate_name_display",
        "email",
        "phone",
        "location",
        "position",
        "job_location",
        "client",
        "rate",
        "signup_date",
        "interview_support_by",
        "team_lead",
        "manager",
        "preview_text",
        "received_at",
        "received_at_display",
        "created_at",
        "created_at_display",
        "mail_date",
        "month_key",
        "month_label",
        "expert_name",
        "manager_name",
        "team_lead_name",
    ]
    return {key: record.get(key) for key in keys}


def build_po_redirect_params(source):
    params = {}
    person_keys = {"expert", "manager", "team_lead", "group_expert", "group_manager", "group_team_lead"}
    for key in (
        "month",
        "date",
        "expert",
        "manager",
        "team_lead",
        "group_expert",
        "group_manager",
        "group_team_lead",
        "record",
        "view",
    ):
        raw_value = source.get(key, "")
        value = normalize_person_name(raw_value) if key in person_keys else clean_text(raw_value)
        if value:
            params[key] = value
    return params


def po_access_redirect(next_url=""):
    target = next_url if is_safe_next_url(next_url) else url_for("po.po_dashboard")
    return redirect(url_for("po.po_access", next=target))


@po_bp.route("/access", methods=["GET", "POST"])
def po_access():
    if not po_pin_security_enabled():
        return redirect(url_for("po.po_dashboard"))

    next_url = request.values.get("next", "") or url_for("po.po_dashboard")
    safe_next_url = next_url if is_safe_next_url(next_url) else url_for("po.po_dashboard")
    if safe_next_url == url_for("po.po_access"):
        safe_next_url = url_for("po.po_dashboard")

    current_access = get_current_po_access()
    if current_access:
        return redirect(safe_next_url)

    if request.method == "POST":
        profile = find_po_pin_profile(request.form.get("pin", ""))
        if profile:
            set_current_po_access(profile)
            flash(f"PO page unlocked for {profile['label']}.", "success")
            return redirect(safe_next_url)

        flash("Invalid PO PIN. Please try again.", "error")
        return redirect(url_for("po.po_access", next=safe_next_url))

    return render_template("po_access.html", next_url=safe_next_url)


@po_bp.route("/logout")
def po_logout():
    clear_current_po_access()
    flash("PO page locked.", "info")
    if po_pin_security_enabled():
        return redirect(url_for("po.po_access", next=url_for("po.po_dashboard")))
    return redirect(url_for("po.po_dashboard"))


@po_bp.route("/")
@po_bp.route("/po")
def po_dashboard():
    if po_pin_security_enabled() and not get_current_po_access():
        return po_access_redirect(current_request_next_url())

    po_access = get_current_po_access()
    po_lock = get_po_lock(po_access)
    selected_month = normalize_month_filter(
        request.args.get("month", ""),
        request.args.get("year", ""),
    )
    selected_date = clean_text(request.args.get("date", ""))
    selected_expert = normalize_person_name(request.args.get("expert", ""))
    selected_manager = normalize_person_name(request.args.get("manager", ""))
    selected_team_lead = normalize_person_name(request.args.get("team_lead", ""))

    group_expert = normalize_person_name(request.args.get("group_expert", ""))
    group_manager = normalize_person_name(request.args.get("group_manager", ""))
    group_team_lead = normalize_person_name(request.args.get("group_team_lead", ""))
    selected_record_id = clean_text(request.args.get("record", "") or request.args.get("view", ""))

    try:
        records = filter_records_for_po_access(fetch_po_records(get_supabase_client()), po_access)
        load_error = ""
    except Exception as exc:
        records = []
        load_error = str(exc)

    selected_expert, selected_manager, selected_team_lead = enforce_po_filter_values(
        selected_expert,
        selected_manager,
        selected_team_lead,
        po_access,
    )

    month_counts = Counter(record["month_key"] for record in records if record["month_key"])
    month_options = [
        {
            "value": month_key,
            "label": month_label(month_key),
            "count": month_counts[month_key],
        }
        for month_key in sorted(month_counts.keys(), reverse=True)
    ]

    temporal_records = [
        record
        for record in records
        if (not selected_month or record["month_key"] == selected_month)
        and (not selected_date or record["mail_date"] == selected_date)
    ]

    available_experts = sorted(
        {record["expert_name"] for record in temporal_records if record["expert_name"]}
    )
    available_managers = sorted(
        {record["manager_name"] for record in temporal_records if record["manager_name"]}
    )
    available_team_leads = sorted(
        {record["team_lead_name"] for record in temporal_records if record["team_lead_name"]}
    )

    filtered_records = [
        record
        for record in temporal_records
        if (not selected_expert or record["expert_name"] == selected_expert)
        and (not selected_manager or record["manager_name"] == selected_manager)
        and (not selected_team_lead or record["team_lead_name"] == selected_team_lead)
    ]

    summary_rows = build_summary_rows(filtered_records)

    selected_group_records = []
    selected_group = None
    if group_expert and group_manager and group_team_lead:
        selected_group_records = [
            record
            for record in filtered_records
            if record["expert_name"] == group_expert
            and record["manager_name"] == group_manager
            and record["team_lead_name"] == group_team_lead
        ]
        selected_group_records.sort(key=lambda record: record["sort_ts"], reverse=True)

        if selected_group_records:
            selected_group = {
                "expert_name": group_expert,
                "manager_name": group_manager,
                "team_lead_name": group_team_lead,
                "po_count": len(selected_group_records),
            }
            selected_group.update(
                summarize_dates({record["mail_date"] or "Unknown" for record in selected_group_records})
            )

    selected_record = None
    if selected_record_id:
        for record in filtered_records:
            if record["record_id"] == selected_record_id:
                selected_record = record
                break

    unique_candidates = len(
        {clean_text(record.get("candidate_name")) for record in filtered_records if clean_text(record.get("candidate_name"))}
    )
    unique_managers = len({record["manager_name"] for record in filtered_records if record["manager_name"]})
    unique_team_leads = len(
        {record["team_lead_name"] for record in filtered_records if record["team_lead_name"]}
    )

    return render_template(
        "po.html",
        load_error=load_error,
        month_options=month_options,
        selected_month=selected_month,
        selected_date=selected_date,
        selected_expert=selected_expert,
        selected_manager=selected_manager,
        selected_team_lead=selected_team_lead,
        available_experts=available_experts,
        available_managers=available_managers,
        available_team_leads=available_team_leads,
        summary_rows=summary_rows,
        selected_group=selected_group,
        selected_group_records=selected_group_records,
        selected_record=selected_record,
        total_records=len(filtered_records),
        total_groups=len(summary_rows),
        unique_candidates=unique_candidates,
        unique_managers=unique_managers,
        unique_team_leads=unique_team_leads,
        active_months=len(month_options),
        po_security_enabled=po_pin_security_enabled(),
        po_access=po_access,
        po_lock=po_lock,
        can_fetch_new=(not po_pin_security_enabled()) or po_access_can_sync(po_access),
    )


@po_bp.route("/api/records")
def po_api_records():
    if po_pin_security_enabled() and not get_current_po_access():
        return jsonify({"success": False, "error": "PO access PIN is required."}), 401

    po_access = get_current_po_access()
    selected_month = normalize_month_filter(
        request.args.get("month", ""),
        request.args.get("year", ""),
    )
    selected_date = clean_text(request.args.get("date", ""))
    selected_expert = normalize_person_name(request.args.get("expert", ""))
    selected_manager = normalize_person_name(request.args.get("manager", ""))
    selected_team_lead = normalize_person_name(request.args.get("team_lead", ""))
    limit = max(min(int(request.args.get("limit", 50)), 200), 1)
    offset = max(int(request.args.get("offset", 0)), 0)

    try:
        records = filter_records_for_po_access(fetch_po_records(get_supabase_client()), po_access)
    except Exception as exc:
        return jsonify({"success": False, "error": str(exc)}), 500

    selected_expert, selected_manager, selected_team_lead = enforce_po_filter_values(
        selected_expert,
        selected_manager,
        selected_team_lead,
        po_access,
    )

    filtered_records = [
        record
        for record in records
        if (not selected_month or record["month_key"] == selected_month)
        and (not selected_date or record["mail_date"] == selected_date)
        and (not selected_expert or record["expert_name"] == selected_expert)
        and (not selected_manager or record["manager_name"] == selected_manager)
        and (not selected_team_lead or record["team_lead_name"] == selected_team_lead)
    ]

    paged_records = filtered_records[offset : offset + limit]

    return jsonify(
        {
            "success": True,
            "count": len(filtered_records),
            "data": [serialize_record(record) for record in paged_records],
        }
    )


@po_bp.route("/fetch-new", methods=["POST"])
def po_fetch_new():
    redirect_params = build_po_redirect_params(request.form)
    redirect_target = url_for("po.po_dashboard", **redirect_params)

    if po_pin_security_enabled() and not get_current_po_access():
        return po_access_redirect(redirect_target)

    po_access = get_current_po_access()
    if po_pin_security_enabled() and not po_access_can_sync(po_access):
        flash("This PO PIN is view-only. Fetch New Data is available only for all-view access.", "error")
        return redirect(redirect_target)

    try:
        service = POConsumerService()
        if not service.is_configured():
            raise RuntimeError("PO Kafka or Supabase configuration is missing.")

        stats = service.consume_batch()
        inserted = stats["inserted"]
        duplicates = stats["duplicate"]
        checked = stats["checked"]
        backlog_note = (
            " More messages may still be waiting in Kafka, so you can click again."
            if checked >= int(os.getenv("PO_FETCH_MAX_MESSAGES", "200"))
            else ""
        )

        if inserted > 0:
            flash(
                f"Fetched {inserted} new PO mail(s). Checked {checked} Kafka message(s) and skipped "
                f"{duplicates} duplicate record(s).{backlog_note}",
                "success",
            )
        else:
            flash(
                f"No new PO data was added. Checked {checked} Kafka message(s) and skipped "
                f"{duplicates} duplicate record(s).{backlog_note}",
                "info",
            )
    except Exception as exc:
        flash(f"PO fetch failed: {exc}", "error")

    return redirect(redirect_target)
