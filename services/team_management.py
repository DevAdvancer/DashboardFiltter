from difflib import SequenceMatcher
import re

from flask import current_app, has_app_context

from db import get_db, get_teams_db

PO_NAME_ALIASES = {
    "Anusree Vasudevan": "Anusree Vasudevan",
    "Prateek Navariya": "Prateek Narvariya",
    "Rujuwal Garag": "Rujuwal Garg",
    "Varsa Shahu": "Varsha Sahu",
}
PO_PLACEHOLDER_NAMES = {"n/a", "na", "not applicable", "none", "null", "nil"}
NAME_MATCH_THRESHOLD = 0.84
NAME_MATCH_MARGIN = 0.05


def clean_text(value):
    if value is None:
        return ""
    return " ".join(str(value).replace("\xa0", " ").split())


def normalize_lookup_text(value):
    return clean_text(value).casefold()


def normalize_email_like(value):
    cleaned = clean_text(value)
    return cleaned.casefold() if "@" in cleaned else cleaned


def mongo_normalized_text(field_name):
    return {
        "$toLower": {
            "$trim": {
                "input": {
                    "$ifNull": [f"${field_name}", ""],
                }
            }
        }
    }


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


def _normalize_lookup_key(value):
    cleaned = normalize_lookup_text(value)
    if not cleaned:
        return ""

    if "@" in cleaned:
        cleaned = cleaned.split("@", 1)[0]

    cleaned = re.sub(r"[._-]+", " ", cleaned)
    cleaned = re.sub(r"[^A-Za-z0-9\s]", " ", cleaned)
    cleaned = " ".join(cleaned.split())
    if not cleaned:
        return ""

    return " ".join(normalize_person_name(cleaned).lower().split())


def _sorted_token_key(value):
    normalized = _normalize_lookup_key(value)
    if not normalized:
        return ""
    return " ".join(sorted(normalized.split()))


def _name_match_score(left, right):
    if not left or not right:
        return 0.0

    if left == right:
        return 1.0

    left_sorted = " ".join(sorted(left.split()))
    right_sorted = " ".join(sorted(right.split()))

    return max(
        SequenceMatcher(None, left, right).ratio(),
        SequenceMatcher(None, left_sorted, right_sorted).ratio(),
    )


def _derive_names_from_email(email):
    local_part = clean_text(email).split("@", 1)[0]
    if not local_part:
        return []

    parts = [part for part in re.split(r"[._-]+", local_part) if part]
    if not parts:
        return []

    candidates = [" ".join(parts)]
    if len(parts) > 1:
        candidates.append(" ".join(reversed(parts)))

    return [normalize_person_name(candidate) for candidate in candidates if normalize_person_name(candidate)]


def _team_lead_from_team_name(team_name):
    cleaned = clean_text(team_name)
    if cleaned.lower().startswith("team "):
        cleaned = cleaned[5:]
    return normalize_person_name(cleaned)


def _index_entry(index, key, entry):
    if not key:
        return
    index.setdefault(key, {})
    index[key][entry["email"]] = entry


def get_team_management_directory():
    def build():
        teams_db = get_teams_db()
        main_db = get_db()

        team_docs = list(teams_db.teams.find({}, {"_id": 0, "name": 1, "members": 1}))
        users = list(
            main_db.users.find(
                {},
                {
                    "_id": 0,
                    "email": 1,
                    "manager": 1,
                    "teamLead": 1,
                    "profile.displayName": 1,
                },
            )
        )

        team_by_email = {}
        for team_doc in team_docs:
            team_name = clean_text(team_doc.get("name"))
            for member in team_doc.get("members", []):
                email = clean_text(member).lower()
                if email:
                    team_by_email[email] = team_name

        users_by_email = {}
        for user in users:
            email = clean_text(user.get("email")).lower()
            if email:
                users_by_email[email] = user

        entries = []
        exact_index = {}
        token_index = {}

        all_emails = sorted(set(team_by_email.keys()) | set(users_by_email.keys()))
        for email in all_emails:
            user = users_by_email.get(email, {})
            team_name = team_by_email.get(email, "")
            display_name = normalize_person_name((user.get("profile") or {}).get("displayName"))
            derived_names = _derive_names_from_email(email)
            expert_name = display_name or (derived_names[0] if derived_names else "")

            entry = {
                "email": email,
                "expert_name": expert_name,
                "manager_name": normalize_person_name(user.get("manager")),
                "team_lead_name": normalize_person_name(user.get("teamLead")) or _team_lead_from_team_name(team_name),
                "team_name": team_name,
                "lookup_keys": set(),
            }

            lookup_values = {email, expert_name}
            lookup_values.update(derived_names)
            for lookup_value in lookup_values:
                exact_key = _normalize_lookup_key(lookup_value)
                token_key = _sorted_token_key(lookup_value)
                if exact_key:
                    entry["lookup_keys"].add(exact_key)
                    _index_entry(exact_index, exact_key, entry)
                if token_key:
                    _index_entry(token_index, token_key, entry)

            entries.append(entry)

        return {
            "entries": entries,
            "exact_index": exact_index,
            "token_index": token_index,
        }

    if has_app_context():
        cache = getattr(current_app, "cache", None)
        if cache:
            cache_key = "team-management:directory:v1"
            cached = cache.get(cache_key)
            if cached is not None:
                return cached
            value = build()
            cache.set(cache_key, value, timeout=600)
            return value

    return build()


def resolve_expert_management(expert_value, directory=None):
    directory = directory or get_team_management_directory()
    normalized = _normalize_lookup_key(expert_value)
    if not normalized:
        return None

    exact_matches = list(directory["exact_index"].get(normalized, {}).values())
    if len(exact_matches) == 1:
        return exact_matches[0]

    token_matches = list(directory["token_index"].get(_sorted_token_key(expert_value), {}).values())
    if len(token_matches) == 1:
        return token_matches[0]

    best_entry = None
    best_score = 0.0
    runner_up = 0.0

    for entry in directory["entries"]:
        score = max((_name_match_score(normalized, key) for key in entry["lookup_keys"]), default=0.0)
        if score > best_score:
            runner_up = best_score
            best_score = score
            best_entry = entry
        elif score > runner_up:
            runner_up = score

    if best_entry and best_score >= NAME_MATCH_THRESHOLD and best_score - runner_up >= NAME_MATCH_MARGIN:
        return best_entry

    return None


def get_management_snapshot(expert_value, fallback_manager="", fallback_team_lead="", directory=None):
    resolved = resolve_expert_management(expert_value, directory=directory)

    expert_name = normalize_person_name(expert_value)
    if "@" in clean_text(expert_value):
        derived_names = _derive_names_from_email(expert_value)
        if derived_names:
            expert_name = derived_names[0]

    manager_name = normalize_person_name(fallback_manager)
    team_lead_name = normalize_person_name(fallback_team_lead)
    team_name = ""

    if resolved:
        expert_name = resolved.get("expert_name") or expert_name
        manager_name = resolved.get("manager_name") or manager_name
        team_lead_name = resolved.get("team_lead_name") or team_lead_name
        team_name = clean_text(resolved.get("team_name"))

    return {
        "expert_name": display_value(expert_name),
        "manager_name": display_value(manager_name),
        "team_lead_name": display_value(team_lead_name),
        "team_name": display_value(team_name),
        "management_source": "mongo" if resolved else "supabase",
        "email": resolved.get("email", "") if resolved else "",
    }
