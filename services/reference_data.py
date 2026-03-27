from flask import current_app, has_app_context

from db import get_db, get_teams_db

CACHE_VERSION = "v1"


def _cache_key(name, *parts):
    serialized = ":".join(str(part) for part in parts if part not in (None, ""))
    return f"ref:{CACHE_VERSION}:{name}" + (f":{serialized}" if serialized else "")


def _cache_result(key, timeout, builder):
    if has_app_context():
        cache = getattr(current_app, "cache", None)
        if cache:
            cached = cache.get(key)
            if cached is not None:
                return cached

            value = builder()
            cache.set(key, value, timeout=timeout)
            return value

    return builder()


def get_teams_reference():
    def build():
        teams_db = get_teams_db()
        teams_cursor = list(teams_db.teams.find({}, {"name": 1, "members": 1, "_id": 0}))
        teams_map = {
            team["name"]: [str(member).strip() for member in team.get("members", []) if str(member).strip()]
            for team in teams_cursor
            if team.get("name")
        }

        expert_to_team = {}
        for team_name, members in teams_map.items():
            for member in members:
                expert_to_team[member.lower()] = team_name

        all_experts = sorted({member for members in teams_map.values() for member in members})
        return {
            "teams_map": teams_map,
            "teams_list": sorted(teams_map.keys()),
            "expert_to_team": expert_to_team,
            "all_experts": all_experts,
        }

    return _cache_result(_cache_key("teams"), 600, build)


def get_active_expert_emails(manager_name="Harsh Patel"):
    def build():
        db = get_db()
        active_experts_cursor = db.users.find(
            {"manager": manager_name, "active": True},
            {"email": 1, "_id": 0},
        )
        return sorted(
            {
                str(user["email"]).strip().lower()
                for user in active_experts_cursor
                if user.get("email")
            }
        )

    return _cache_result(_cache_key("active-experts", manager_name), 600, build)


def get_active_task_experts(completed_only=True, manager_name="Harsh Patel"):
    def build():
        db = get_db()
        active_experts = set(get_active_expert_emails(manager_name))
        query = {"assignedTo": {"$type": "string", "$ne": ""}}
        if completed_only:
            query["status"] = "Completed"

        task_experts = db.taskBody.distinct("assignedTo", query)
        return sorted(
            [expert for expert in task_experts if str(expert).strip().lower() in active_experts]
        )

    return _cache_result(
        _cache_key("task-experts", "completed" if completed_only else "all", manager_name),
        600,
        build,
    )


def get_candidate_lookup_names(limit=500):
    def build():
        db = get_db()
        return sorted(
            db.taskBody.distinct(
                "Candidate Name",
                {"Candidate Name": {"$type": "string", "$ne": ""}},
            )
        )[:limit]

    return _cache_result(_cache_key("candidate-lookup", limit), 300, build)


def get_export_filter_options():
    def build():
        db = get_db()
        technologies = sorted(
            [value for value in db.candidateDetails.distinct("Technology") if value not in (None, "")]
        )
        workflow_statuses = sorted(
            [value for value in db.candidateDetails.distinct("workflowStatus") if value not in (None, "")]
        )
        return {
            "technologies": technologies,
            "workflow_statuses": workflow_statuses,
        }

    return _cache_result(_cache_key("export-options"), 600, build)


def get_kpi_round_titles():
    def build():
        db = get_db()
        rounds = db.taskBody.distinct("actualRound", {"status": "Completed"})
        return sorted([value for value in rounds if value and isinstance(value, str)])

    return _cache_result(_cache_key("kpi-rounds"), 600, build)
