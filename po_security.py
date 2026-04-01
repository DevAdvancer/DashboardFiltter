import json
import os
from functools import lru_cache
import re
from urllib.parse import urlparse

from flask import request, session
from services.reference_data import get_teams_reference
from services.team_management import get_team_management_directory

PO_SESSION_KEY = "po_dashboard_access"
LOCKABLE_FIELDS = {"team", "expert", "manager", "team_lead"}
LOCK_FIELD_LABELS = {
    "team": "Team",
    "expert": "Expert Name",
    "manager": "Manager Name",
    "team_lead": "Team Lead Name",
}
PO_NAME_ALIASES = {
    "Anusree Vasudevan": "Anusree Vasudevan",
    "Prateek Navariya": "Prateek Narvariya",
    "Rujuwal Garag": "Rujuwal Garg",
}
PO_PLACEHOLDER_NAMES = {"n/a", "na", "not applicable", "none", "null", "nil"}


def clean_text(value):
    if value is None:
        return ""
    return " ".join(str(value).split())


def normalize_person_name(value):
    cleaned = clean_text(value)
    if not cleaned:
        return ""
    if cleaned.lower() in PO_PLACEHOLDER_NAMES:
        return ""
    titled = cleaned.title()
    return PO_NAME_ALIASES.get(titled, titled)


def normalize_team_name(value):
    cleaned = clean_text(value)
    if not cleaned:
        return ""
    compact = re.sub(r"[\s_-]+", "", cleaned).lower()
    if compact.startswith("team"):
        compact = compact[4:]
    return compact


def resolve_team_lock_value(value):
    cleaned = clean_text(value)
    if not cleaned:
        return ""

    reference = get_teams_reference()
    teams_list = reference.get("teams_list", [])

    if cleaned in teams_list:
        return cleaned

    normalized_value = normalize_team_name(cleaned)
    for team_name in teams_list:
        if normalize_team_name(team_name) == normalized_value:
            return team_name

    normalized_person = normalize_person_name(cleaned)
    if not normalized_person:
        return cleaned

    person_tokens = set(re.findall(r"[a-z0-9]+", normalized_person.lower()))

    token_team_matches = {
        team_name
        for team_name in teams_list
        if normalize_team_name(team_name) and normalize_team_name(team_name) in person_tokens
    }
    if len(token_team_matches) == 1:
        return next(iter(token_team_matches))

    directory = get_team_management_directory()
    team_matches = {
        clean_text(entry.get("team_name"))
        for entry in directory.get("entries", [])
        if clean_text(entry.get("team_name"))
        and (
            normalize_person_name(entry.get("team_lead_name")) == normalized_person
            or normalize_person_name(entry.get("manager_name")) == normalized_person
            or normalize_person_name(entry.get("expert_name")) == normalized_person
        )
    }

    if len(team_matches) == 1:
        return next(iter(team_matches))

    return cleaned


def normalize_lock_value(field, value):
    cleaned = clean_text(value)
    if not cleaned:
        return ""
    if field == "team":
        return resolve_team_lock_value(cleaned)
    return normalize_person_name(cleaned)


@lru_cache(maxsize=1)
def load_po_pin_profiles():
    raw = os.getenv("PO_PIN_PROFILES_JSON", "").strip()
    if not raw:
        return []

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return []

    profiles = []
    for item in data:
        if not isinstance(item, dict):
            continue

        pin = clean_text(item.get("pin"))
        scope = clean_text(item.get("scope", "team")).lower()
        label = clean_text(item.get("label"))
        team_value = clean_text(item.get("team"))
        field = clean_text(item.get("field")).lower()
        value = clean_text(item.get("value")) or team_value

        if not pin:
            continue

        if scope == "all":
            profiles.append(
                {
                    "pin": pin,
                    "label": label or "PO All View",
                    "scope": "all",
                    "field": "",
                    "value": "",
                }
            )
            continue

        if scope not in {"team", "locked"}:
            continue

        if not field:
            field = "team" if team_value else ""

        if field not in LOCKABLE_FIELDS or not value:
            continue

        value = normalize_lock_value(field, value)

        profiles.append(
            {
                "pin": pin,
                "label": label or f"{LOCK_FIELD_LABELS[field]} View",
                "scope": "locked",
                "field": field,
                "value": value,
            }
        )

    return profiles


def po_pin_security_enabled():
    return bool(load_po_pin_profiles())


def find_po_pin_profile(pin_value):
    pin_value = clean_text(pin_value)
    for profile in load_po_pin_profiles():
        if profile["pin"] == pin_value:
            return {
                "label": profile["label"],
                "scope": profile["scope"],
                "field": profile["field"],
                "value": profile["value"],
            }
    return None


def get_current_po_access():
    access = session.get(PO_SESSION_KEY)
    if not isinstance(access, dict):
        return None

    scope = clean_text(access.get("scope")).lower()
    field = clean_text(access.get("field")).lower()
    value = normalize_lock_value(field, access.get("value"))

    if scope == "all":
        return {
            "label": clean_text(access.get("label")) or "PO All View",
            "scope": "all",
            "field": "",
            "value": "",
        }

    if scope == "locked" and field in LOCKABLE_FIELDS and value:
        return {
            "label": clean_text(access.get("label")) or f"{LOCK_FIELD_LABELS[field]} View",
            "scope": "locked",
            "field": field,
            "value": value,
        }

    return None


def set_current_po_access(profile):
    session[PO_SESSION_KEY] = {
        "label": clean_text(profile.get("label")),
        "scope": clean_text(profile.get("scope")).lower(),
        "field": clean_text(profile.get("field")).lower(),
        "value": normalize_lock_value(clean_text(profile.get("field")).lower(), profile.get("value")),
    }


def clear_current_po_access():
    session.pop(PO_SESSION_KEY, None)


def get_po_lock(access=None):
    access = access or get_current_po_access()
    if not access or access.get("scope") != "locked":
        return None

    field = access["field"]
    return {
        "field": field,
        "field_label": LOCK_FIELD_LABELS[field],
        "value": access["value"],
    }


def filter_records_for_po_access(records, access=None):
    access = access or get_current_po_access()
    lock = get_po_lock(access)
    if not lock:
        return records

    key = {
        "team": "team_name",
        "expert": "expert_name",
        "manager": "manager_name",
        "team_lead": "team_lead_name",
    }[lock["field"]]
    return [
        record
        for record in records
        if normalize_lock_value(lock["field"], record.get(key)) == lock["value"]
    ]


def enforce_po_dashboard_filters(selected_team, selected_expert, access=None):
    access = access or get_current_po_access()
    lock = get_po_lock(access)
    if not lock:
        return selected_team, selected_expert

    if lock["field"] == "team":
        return lock["value"], selected_expert
    if lock["field"] == "expert":
        return selected_team, lock["value"]
    return selected_team, selected_expert


def enforce_po_filter_values(selected_expert, selected_manager, selected_team_lead, access=None):
    access = access or get_current_po_access()
    lock = get_po_lock(access)
    if not lock:
        return selected_expert, selected_manager, selected_team_lead

    if lock["field"] == "expert":
        return lock["value"], selected_manager, selected_team_lead
    if lock["field"] == "manager":
        return selected_expert, lock["value"], selected_team_lead
    return selected_expert, selected_manager, lock["value"]


def po_access_can_sync(access=None):
    access = access or get_current_po_access()
    return bool(access and access.get("scope") == "all")


def is_safe_next_url(target):
    if not target:
        return False

    parsed = urlparse(target)
    if parsed.scheme or parsed.netloc:
        return False

    return target.startswith("/")


def current_request_next_url():
    full_path = request.full_path or request.path or "/po/"
    if full_path.endswith("?"):
        full_path = full_path[:-1]
    return full_path or "/po/"
