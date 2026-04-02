from flask import Blueprint, render_template, request, jsonify, current_app, send_file, redirect, url_for
from db import get_db
from datetime import datetime
from collections import Counter, defaultdict
from functools import lru_cache
import pandas as pd
import re
import json
from io import BytesIO
from po_security import (
    filter_records_for_po_access,
    get_current_po_access,
    po_pin_security_enabled,
)
from routes.po import fetch_po_records, get_supabase_client
from services.reference_data import (
    get_active_expert_emails,
    get_active_task_experts,
    get_export_filter_options,
    get_teams_reference,
)
from services.team_management import normalize_lookup_text

analytics_bp = Blueprint('analytics', __name__)
ANALYTICS_CACHE_VERSION = "v4"

# Round mapping from actualRound to funnel stages
ROUND_BUCKETS = {
    "screening": "Screening",
    "1st round": "1st",
    "first round": "1st",
    "2nd round": "2nd",
    "second round": "2nd",
    "3rd round": "3rd/Technical",
    "third round": "3rd/Technical",
    "tech round": "3rd/Technical",
    "technical": "3rd/Technical",
    "technical round": "3rd/Technical",
    "loop": "Loop Round",
    "loop round": "Loop Round",
    "final": "Final",
    "final round": "Final",
}

ROUND_BUCKET_PATTERNS = (
    (re.compile(r"\bscreen(?:ing)?\b"), "Screening"),
    (re.compile(r"\b(1st|first)\b"), "1st"),
    (re.compile(r"\b(2nd|second)\b"), "2nd"),
    (re.compile(r"\b(3rd|third)\b|\btech(?:nical)?\b"), "3rd/Technical"),
    (re.compile(r"\bloop\b"), "Loop Round"),
    (re.compile(r"\bfinal\b"), "Final"),
)

PIPELINE_ORDER = ["Screening", "1st", "2nd", "3rd/Technical", "Loop Round", "Final"]


def normalize_round(r):
    """Normalize actualRound string to funnel stage."""
    if not r:
        return None
    key = re.sub(r"[^a-z0-9]+", " ", str(r).strip().lower()).strip()
    if not key:
        return None

    mapped = ROUND_BUCKETS.get(key)
    if mapped:
        return mapped

    for pattern, stage in ROUND_BUCKET_PATTERNS:
        if pattern.search(key):
            return stage

    return None


def build_funnel_metrics(stages):
    """Build consistent funnel metrics from normalized stage counts."""
    scr = stages.get("Screening", 0)
    r1 = stages.get("1st", 0)
    r2 = stages.get("2nd", 0)
    r3 = stages.get("3rd/Technical", 0)
    loop = stages.get("Loop Round", 0)
    fin = stages.get("Final", 0)

    return {
        'interview_count': r1 + r2 + r3 + loop + fin,
        'screening': scr,
        'first': r1,
        'second': r2,
        'third_tech': r3,
        'loop_round': loop,
        'final': fin,
        'screening_to_1st': pct(r1, scr),
        'first_to_2nd': pct(r2, r1),
        'second_to_3rd': pct(r3, r2),
        'third_to_loop': pct(loop, r3),
        'loop_to_final': pct(fin, loop),
        'third_to_final': pct(fin, loop or r3),
    }


def pct(num, den):
    """Calculate percentage."""
    if den and den > 0:
        return round((num / den) * 100, 1)
    return 0.0


def get_date_filter_strings():
    """
    Get date filter as strings for taskBody collection.
    Defaults to current month if not provided.
    """
    start_date = request.args.get('start_date', '')
    end_date = request.args.get('end_date', '')

    if not start_date and not end_date:
        today = datetime.now()
        start_date = today.replace(day=1).strftime('%Y-%m-%d')
        end_date = today.strftime('%Y-%m-%d')

    return start_date, end_date


def get_active_experts(db):
    return set(get_active_expert_emails())


def get_expert_team_map(db=None):
    reference = get_teams_reference()
    return reference["expert_to_team"], reference["teams_map"]


def get_analytics_filter_options(completed_only=True):
    reference = get_teams_reference()
    return reference["teams_list"], get_active_task_experts(completed_only=completed_only), reference["teams_map"]


def analytics_cache_key(name, *parts):
    serialized = ":".join(str(part) if part not in (None, "") else "all" for part in parts)
    return f"analytics:{ANALYTICS_CACHE_VERSION}:{name}:{serialized}"


def get_po_access_cache_token():
    access = get_current_po_access()
    if po_pin_security_enabled() and not access:
        return "locked"
    if not access:
        return "public"
    return json.dumps(access, sort_keys=True, default=str)


def get_po_count_maps(start_date="", end_date=""):
    access = get_current_po_access()
    cache_key = analytics_cache_key(
        "po-count-maps",
        start_date,
        end_date,
        get_po_access_cache_token(),
    )
    cached = current_app.cache.get(cache_key)
    if cached is not None:
        return cached

    if po_pin_security_enabled() and not access:
        value = {
            "state": "locked",
            "team_counts": {},
            "expert_counts": {},
            "total": None,
        }
        current_app.cache.set(cache_key, value, timeout=300)
        return value

    try:
        records = filter_records_for_po_access(fetch_po_records(get_supabase_client()), access)
        start_value = start_date[:10] if start_date else ""
        end_value = end_date[:10] if end_date else ""

        filtered_records = [
            record
            for record in records
            if (
                not start_value
                or (
                    record.get("mail_date")
                    and str(record.get("mail_date")) >= start_value
                )
            )
            and (
                not end_value
                or (
                    record.get("mail_date")
                    and str(record.get("mail_date")) <= end_value
                )
            )
        ]

        team_counts = Counter(
            record.get("team_name")
            for record in filtered_records
            if record.get("team_name")
        )
        expert_counts = Counter(
            (record.get("expert_email") or "").lower()
            for record in filtered_records
            if record.get("expert_email")
        )

        value = {
            "state": "ready",
            "team_counts": dict(team_counts),
            "expert_counts": dict(expert_counts),
            "total": len(filtered_records),
        }
    except Exception:
        value = {
            "state": "unavailable",
            "team_counts": {},
            "expert_counts": {},
            "total": None,
        }

    current_app.cache.set(cache_key, value, timeout=300)
    return value


def build_task_query(start_date='', end_date=''):
    """Build base query for taskBody collection."""
    match_filters = {
        "status": "Completed",
        "assignedTo": {"$type": "string", "$ne": ""},
        "actualRound": {"$nin": ["On demand", "On Demand or AI Interview"]},
    }

    if start_date or end_date:
        date_filter = {}
        if start_date:
            # Convert date string to ISO format if needed
            if 'T' not in start_date:
                start_date = f"{start_date}T00:00:00"
            date_filter["$gte"] = start_date
        if end_date:
            if 'T' not in end_date:
                end_date = f"{end_date}T23:59:59"
            date_filter["$lte"] = end_date
        if date_filter:
            match_filters["receivedDateTime"] = date_filter

    return match_filters


def get_expert_funnel_data(db, start_date='', end_date='', filter_team=None, filter_expert=None):
    """
    Get expert funnel data from taskBody collection.

    Expert-Level Filtration Rules:
    0. ONLY include experts who are active (active=true) and manager="Harsh Patel" from users collection
    1. Determine expert's team using case-insensitive email-to-team mapping
    2. Apply Team Filter first:
       - If filter_team is not null, include only experts from that team
       - Exclude all experts from other teams
    3. Apply Expert Filter:
       - If filter_expert is not null, include only that specific expert
       - Exclude all other experts
    4. If BOTH filters are provided:
       - Expert must satisfy BOTH conditions
       - Must belong to filter_team AND email must match filter_expert

    Experts that do not meet active filters will NOT appear in output.
    """
    cache = current_app.cache
    cache_key = analytics_cache_key("expert-funnel", start_date, end_date, filter_team, filter_expert)
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    filter_expert_key = normalize_lookup_text(filter_expert)

    # Get active experts first
    active_experts = get_active_experts(db)

    expert_team_map, teams_map = get_expert_team_map(db)

    # Build query for taskBody collection
    match_filters = build_task_query(start_date, end_date)

    # Aggregate by expert and raw round in Mongo first so we process far fewer rows in Python.
    round_rows = list(
        db.taskBody.aggregate(
            [
                {"$match": match_filters},
                {
                    "$group": {
                        "_id": {
                            "expert": "$assignedTo",
                            "round": "$actualRound",
                        },
                        "count": {"$sum": 1},
                    }
                },
            ],
            allowDiskUse=True,
        )
    )

    # Normalize raw rounds into funnel stages per expert.
    expert_stage_counts = defaultdict(lambda: Counter())

    for row in round_rows:
        row_id = row.get("_id") or {}
        expert = normalize_lookup_text(row_id.get("expert"))
        if not expert:
            continue

        stage = normalize_round(row_id.get("round"))
        if not stage:
            continue

        expert_stage_counts[expert][stage] += row.get("count", 0)

    # Build expert stats with filtration
    expert_stats = []
    for expert, stages in expert_stage_counts.items():
        if expert not in active_experts:
            continue

        team_name = expert_team_map.get(expert, "Unmapped")
        if filter_team is not None and team_name != filter_team:
            continue
        if filter_expert_key and expert != filter_expert_key:
            continue

        expert_stats.append({
            'expert': expert,
            'team': team_name,
            **build_funnel_metrics(stages),
        })

    expert_stats.sort(key=lambda x: (x['screening_to_1st'], x['interview_count']), reverse=True)

    for idx, stat in enumerate(expert_stats):
        stat['rank'] = idx + 1

    value = (expert_stats, teams_map)
    cache.set(cache_key, value, timeout=300)
    return value


def get_team_funnel_data(db, start_date='', end_date='', filter_team=None, filter_expert=None):
    """
    Get team funnel data from taskBody collection.

    Filtration Rules:
    1. If filter_team is set: only include that team
    2. If filter_expert is set: only include that expert's data within teams
    3. If both are set: expert must belong to filter_team AND match filter_expert
    4. Team aggregations only include data from experts that pass all filters
    """
    cache = current_app.cache
    cache_key = analytics_cache_key("team-funnel", start_date, end_date, filter_team, filter_expert)
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    _, teams_map = get_expert_team_map(db)
    expert_stats, _ = get_expert_funnel_data(db, start_date, end_date, filter_team, filter_expert)
    expert_stats_map = {stat['expert']: stat for stat in expert_stats}

    team_stats = []
    for team_name, members in teams_map.items():
        if filter_team and team_name != filter_team:
            continue

        effective_members = members
        if filter_expert:
            effective_members = [member for member in members if member == filter_expert]
            if not effective_members:
                continue

        agg = Counter()
        contributing_members = 0
        for member in effective_members:
            stats = expert_stats_map.get(member)
            if stats:
                contributing_members += 1
                agg['Screening'] += stats['screening']
                agg['1st'] += stats['first']
                agg['2nd'] += stats['second']
                agg['3rd/Technical'] += stats['third_tech']
                agg['Loop Round'] += stats['loop_round']
                agg['Final'] += stats['final']

        if contributing_members == 0:
            continue

        team_stats.append({
            'team': team_name,
            'member_count': len(members),
            'active_member_count': contributing_members,
            **build_funnel_metrics(agg),
        })

    team_stats.sort(key=lambda x: (x['screening_to_1st'], x['interview_count']), reverse=True)

    for idx, stat in enumerate(team_stats):
        stat['rank'] = idx + 1

    value = (team_stats, teams_map)
    cache.set(cache_key, value, timeout=300)
    return value


@analytics_bp.route('/experts')
def expert_analytics():
    db = get_db()
    start_date, end_date = get_date_filter_strings()

    # Get filter parameters
    filter_team = request.args.get('team', '') or None
    filter_expert = normalize_lookup_text(request.args.get('expert', '')) or None

    teams_list, all_experts, teams_map = get_analytics_filter_options(completed_only=True)
    # Get expert funnel data
    expert_stats, _ = get_expert_funnel_data(db, start_date, end_date, filter_team, filter_expert)

    # Get selected expert detail
    selected_expert = normalize_lookup_text(request.args.get('view_expert', ''))
    expert_detail = None
    expert_tasks = []

    if selected_expert:
        # Find expert stats
        for stat in expert_stats:
            if stat['expert'] == selected_expert:
                expert_detail = stat
                break

        if not expert_detail:
            # Get data for this specific expert
            single_stats, _ = get_expert_funnel_data(db, start_date, end_date, None, selected_expert)
            if single_stats:
                expert_detail = single_stats[0]

        if expert_detail:
            detail_cache_key = analytics_cache_key("expert-detail", start_date, end_date, selected_expert)
            cached_detail = current_app.cache.get(detail_cache_key)

            if cached_detail is None:
                # Get recent tasks for this expert
                task_query = build_task_query(start_date, end_date)
                task_query['assignedTo'] = {"$regex": f"^{re.escape(selected_expert)}$", "$options": "i"}

                expert_tasks = list(db.taskBody.find(task_query).sort('receivedDateTime', -1).limit(25))

                # Get round distribution for charts
                round_pipeline = [
                    {'$match': task_query},
                    {'$group': {'_id': '$actualRound', 'count': {'$sum': 1}}},
                    {'$sort': {'count': -1}}
                ]
                round_dist = list(db.taskBody.aggregate(round_pipeline))
                cached_detail = {
                    'expert_tasks': expert_tasks,
                    'round_distribution': {
                        (item['_id'] or 'Unknown'): item['count']
                        for item in round_dist
                    }
                }
                current_app.cache.set(detail_cache_key, cached_detail, timeout=300)

            expert_tasks = cached_detail.get('expert_tasks', [])
            expert_detail['round_distribution'] = cached_detail.get('round_distribution', {})

    return render_template(
        'expert_analytics.html',
        expert_stats=expert_stats,
        teams=teams_list,
        experts=all_experts,
        teams_map=teams_map,
        selected_team=filter_team or '',
        selected_expert=filter_expert or '',
        view_expert=selected_expert,
        expert_detail=expert_detail,
        expert_tasks=expert_tasks,
        start_date=start_date,
        end_date=end_date,
        total_experts=len(expert_stats)
    )


@analytics_bp.route('/teams')
def team_analytics():
    db = get_db()
    start_date, end_date = get_date_filter_strings()

    # Get filter parameters
    filter_team = request.args.get('team', '') or None
    filter_expert = normalize_lookup_text(request.args.get('expert', '')) or None

    teams_list, all_experts, teams_map = get_analytics_filter_options(completed_only=True)
    # Get team funnel data (with filters applied for alignment)
    team_stats, _ = get_team_funnel_data(db, start_date, end_date, filter_team, filter_expert)

    # Get selected team detail
    selected_team = request.args.get('view_team', '')
    team_detail = None
    member_stats = []

    if selected_team:
        # Find team stats from filtered results
        for stat in team_stats:
            if stat['team'] == selected_team:
                team_detail = stat
                break

        if team_detail:
            # Get member-level breakdown with same filters applied
            # This ensures alignment: if filter_expert is set, only that expert shows
            expert_stats, _ = get_expert_funnel_data(
                db, start_date, end_date,
                selected_team,  # Force team filter to the viewed team
                filter_expert   # Maintain expert filter for alignment
            )

            # All returned experts should be from this team already
            members = teams_map.get(selected_team, [])
            member_stats = [s for s in expert_stats if s['expert'] in members]

    return render_template(
        'team_analytics.html',
        team_stats=team_stats,
        teams=teams_list,
        experts=all_experts,
        teams_map=teams_map,
        selected_team=filter_team or '',
        selected_expert=filter_expert or '',
        view_team=selected_team,
        team_detail=team_detail,
        member_stats=member_stats,
        start_date=start_date,
        end_date=end_date,
        total_teams=len(team_stats)
    )


@analytics_bp.route('/funnel')
def funnel_analytics():
    return redirect(url_for('dashboard.index'))


@analytics_bp.route('/interview-stats')
def interview_stats():
    """
    Interview Statistics page showing Completed, Cancelled, Rescheduled, Not Done counts
    per expert/team with date filtering.
    """
    db = get_db()
    start_date, end_date = get_date_filter_strings()

    # Get filter parameters
    filter_team = request.args.get('team', '') or None
    filter_expert = normalize_lookup_text(request.args.get('expert', '')) or None

    active_experts = get_active_experts(db)
    _, teams_map = get_expert_team_map(db)
    teams_list, all_experts, _ = get_analytics_filter_options(completed_only=False)
    po_counts = get_po_count_maps(start_date, end_date)

    cache_key = analytics_cache_key(
        "interview-stats-page",
        start_date,
        end_date,
        filter_team,
        filter_expert,
        get_po_access_cache_token(),
    )
    cached = current_app.cache.get(cache_key)
    if cached is None:
        date_match = {}
        if start_date or end_date:
            date_filter = {}
            if start_date:
                date_filter["$gte"] = f"{start_date}T00:00:00" if 'T' not in start_date else start_date
            if end_date:
                date_filter["$lte"] = f"{end_date}T23:59:59" if 'T' not in end_date else end_date
            if date_filter:
                date_match["receivedDateTime"] = date_filter

        pipeline = [
            {
                "$match": {
                    **date_match,
                    "actualRound": {"$nin": ["Screening", "On Demand or AI Interview"]},
                    "assignedTo": {"$type": "string", "$ne": ""}
                }
            },
            {
                "$group": {
                    "_id": "$assignedTo",
                    "CompletedCount": {
                        "$sum": {"$cond": [{"$eq": ["$status", "Completed"]}, 1, 0]}
                    },
                    "CancelledCount": {
                        "$sum": {"$cond": [{"$eq": ["$status", "Cancelled"]}, 1, 0]}
                    },
                    "RescheduledCount": {
                        "$sum": {"$cond": [{"$eq": ["$status", "Rescheduled"]}, 1, 0]}
                    },
                    "NotDoneCount": {
                        "$sum": {"$cond": [{"$eq": ["$status", "Not Done"]}, 1, 0]}
                    },
                    "TotalInterviews": {"$sum": 1}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "Expert": "$_id",
                    "CompletedCount": 1,
                    "CancelledCount": 1,
                    "RescheduledCount": 1,
                    "NotDoneCount": 1,
                    "TotalInterviews": 1
                }
            }
        ]

        results = list(db.taskBody.aggregate(pipeline))
        expert_stats_map = {}
        for r in results:
            expert_key = normalize_lookup_text(r.get("Expert"))
            if not expert_key:
                continue

            expert_stats_map.setdefault(
                expert_key,
                {
                    "Expert": expert_key,
                    "CompletedCount": 0,
                    "CancelledCount": 0,
                    "RescheduledCount": 0,
                    "NotDoneCount": 0,
                    "TotalInterviews": 0,
                },
            )
            expert_stats_map[expert_key]["CompletedCount"] += r.get("CompletedCount", 0)
            expert_stats_map[expert_key]["CancelledCount"] += r.get("CancelledCount", 0)
            expert_stats_map[expert_key]["RescheduledCount"] += r.get("RescheduledCount", 0)
            expert_stats_map[expert_key]["NotDoneCount"] += r.get("NotDoneCount", 0)
            expert_stats_map[expert_key]["TotalInterviews"] += r.get("TotalInterviews", 0)
        po_team_counts = po_counts["team_counts"]
        po_expert_counts = po_counts["expert_counts"]

        team_data = []
        expert_data = []

        for team_name, members in teams_map.items():
            if filter_team and team_name != filter_team:
                continue

            effective_members = members
            if filter_expert:
                effective_members = [m for m in members if m == filter_expert]
                if not effective_members:
                    continue

            team_completed = team_cancelled = team_rescheduled = team_notdone = team_total = 0
            member_stats = []

            for expert in effective_members:
                expert_key = normalize_lookup_text(expert)
                if expert_key not in active_experts:
                    continue

                data = expert_stats_map.get(expert_key)
                if data:
                    c = data["CompletedCount"]
                    x = data["CancelledCount"]
                    r = data["RescheduledCount"]
                    nd = data["NotDoneCount"]
                    t = data["TotalInterviews"]
                else:
                    c = x = r = nd = t = 0

                team_completed += c
                team_cancelled += x
                team_rescheduled += r
                team_notdone += nd
                team_total += t

                member_stats.append({
                    'expert': expert_key,
                    'completed': c,
                    'cancelled': x,
                    'rescheduled': r,
                    'notdone': nd,
                    'total': t,
                    'po_count': po_expert_counts.get(expert_key, 0),
                })

                expert_data.append({
                    'team': team_name,
                    'expert': expert_key,
                    'completed': c,
                    'cancelled': x,
                    'rescheduled': r,
                    'notdone': nd,
                    'total': t,
                    'po_count': po_expert_counts.get(expert_key, 0),
                })

            team_data.append({
                'team': team_name,
                'member_count': len(members),
                'active_members': len([m for m in member_stats if m['total'] > 0]),
                'completed': team_completed,
                'cancelled': team_cancelled,
                'rescheduled': team_rescheduled,
                'notdone': team_notdone,
                'total': team_total,
                'po_count': po_team_counts.get(team_name, 0),
                'members': sorted(member_stats, key=lambda x: x['total'], reverse=True)
            })

        team_data.sort(key=lambda x: x['total'], reverse=True)
        expert_data.sort(key=lambda x: x['total'], reverse=True)

        cached = {
            'team_data': team_data,
            'expert_data': expert_data,
            'overall_completed': sum(t['completed'] for t in team_data),
            'overall_cancelled': sum(t['cancelled'] for t in team_data),
            'overall_rescheduled': sum(t['rescheduled'] for t in team_data),
            'overall_notdone': sum(t['notdone'] for t in team_data),
            'overall_total': sum(t['total'] for t in team_data),
            'po_counts_state': po_counts['state'],
        }
        current_app.cache.set(cache_key, cached, timeout=300)

    return render_template(
        'interview_stats.html',
        teams=teams_list,
        experts=all_experts,
        teams_map=teams_map,
        team_data=cached['team_data'],
        expert_data=cached['expert_data'],
        selected_team=filter_team or '',
        selected_expert=filter_expert or '',
        start_date=start_date,
        end_date=end_date,
        overall_completed=cached['overall_completed'],
        overall_cancelled=cached['overall_cancelled'],
        overall_rescheduled=cached['overall_rescheduled'],
        overall_notdone=cached['overall_notdone'],
        overall_total=cached['overall_total'],
        po_counts_state=cached['po_counts_state'],
    )

@lru_cache(maxsize=4096)
def parse_interview_date_from_subject(subject):
    """
    Extract interview date from subject line.
    Example subjects:
    - "Interview Support - Revanth Vatturi - Data Analyst - Feb 2, 2026 at 03:00 PM EST"
    - "Interview Support - Nagaraju Nagam - Devops Engineer - Jan 30, 2026 at 03:00 PM EST"
    Returns date string in YYYY-MM-DD format or None if not found.
    """
    if not subject:
        return None

    # Pattern to match dates like "Feb 2, 2026", "Jan 30, 2026", etc.
    date_pattern = r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2}),?\s+(\d{4})'
    match = re.search(date_pattern, subject, re.IGNORECASE)

    if match:
        month_str, day, year = match.groups()
        month_map = {
            'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
            'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
            'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'
        }
        month = month_map.get(month_str.lower(), '01')
        return f"{year}-{month}-{int(day):02d}"

    return None


@analytics_bp.route('/interview-records')
def interview_records():
    """
    Interview Records page showing detailed interview records with subjects
    per expert/team with date filtering.
    Uses interview date from subject line for filtering (not receivedDateTime).
    """
    db = get_db()
    start_date, end_date = get_date_filter_strings()

    # Get filter parameters
    filter_team = request.args.get('team', '') or None
    filter_expert = normalize_lookup_text(request.args.get('expert', '')) or None

    active_experts = get_active_experts(db)
    expert_team_map, teams_map = get_expert_team_map(db)
    teams_list, all_experts, _ = get_analytics_filter_options(completed_only=False)

    cache_key = analytics_cache_key("interview-records-page", start_date, end_date, filter_team, filter_expert)
    cached = current_app.cache.get(cache_key)
    if cached is None:
        filtered_teams_map = {}
        selected_members = []
        for team_name, members in teams_map.items():
            if filter_team and team_name != filter_team:
                continue

            effective_members = members
            if filter_expert:
                effective_members = [m for m in members if m == filter_expert]
                if not effective_members:
                    continue

            filtered_teams_map[team_name] = effective_members
            selected_members.extend(effective_members)

        expert_patterns = [
            {"assignedTo": {"$regex": f"^{re.escape(member)}$", "$options": "i"}}
            for member in selected_members
            if member
        ]
        query = {
            "actualRound": {"$nin": ["Screening", "On demand", "On Demand or AI Interview"]},
            "status": "Completed",
        }
        if expert_patterns:
            query["$or"] = expert_patterns
        else:
            query["assignedTo"] = "__no_match__"

        records = list(db.taskBody.find(query, {
            "assignedTo": 1,
            "subject": 1,
            "receivedDateTime": 1,
            "actualRound": 1,
            "Candidate Name": 1,
            "_id": 0,
        }).limit(20000))

        expert_records = defaultdict(list)
        all_records = []

        for r in records:
            expert = r.get("assignedTo")
            expert_key = normalize_lookup_text(expert)
            if expert_key not in active_experts:
                continue

            team_name = expert_team_map.get(expert_key)
            if not team_name or team_name not in filtered_teams_map:
                continue

            subject = r.get('subject', '')
            interview_date = parse_interview_date_from_subject(subject)

            if (start_date or end_date) and not interview_date:
                continue
            if interview_date:
                if start_date and interview_date < start_date:
                    continue
                if end_date and interview_date > end_date:
                    continue

            display_date = interview_date if interview_date else (
                r.get('receivedDateTime', '')[:10] if r.get('receivedDateTime') else 'N/A'
            )

            sort_date = interview_date or r.get('receivedDateTime', '') or ''

            expert_records[(team_name, expert_key)].append({
                'subject': subject or 'N/A',
                'candidate': r.get('Candidate Name', 'N/A'),
                'round': r.get('actualRound', 'N/A'),
                'date': display_date,
                'sort_date': sort_date,
            })

            all_records.append({
                'team': team_name,
                'expert': expert_key,
                'subject': subject or 'N/A',
                'candidate': r.get('Candidate Name', 'N/A'),
                'round': r.get('actualRound', 'N/A'),
                'date': sort_date,
            })

        team_data = []
        overall_total = 0
        for team_name, members in filtered_teams_map.items():
            expert_list = []
            team_total = 0
            for expert in members:
                recs = expert_records.get((team_name, expert), [])
                recs.sort(key=lambda item: item['sort_date'], reverse=True)
                for item in recs:
                    item.pop('sort_date', None)
                team_total += len(recs)
                expert_list.append({
                    'expert': expert,
                    'count': len(recs),
                    'records': recs
                })

            expert_list.sort(key=lambda x: x['count'], reverse=True)
            overall_total += team_total
            team_data.append({
                'team': team_name,
                'total': team_total,
                'member_count': len(teams_map.get(team_name, [])),
                'active_count': len([e for e in expert_list if e['count'] > 0]),
                'experts': expert_list
            })

        team_data.sort(key=lambda x: x['total'], reverse=True)
        all_records.sort(key=lambda item: item['date'], reverse=True)
        cached = {
            'team_data': team_data,
            'all_records': all_records,
            'overall_total': overall_total,
            'total_records': len(all_records),
        }
        current_app.cache.set(cache_key, cached, timeout=300)

    return render_template(
        'interview_records.html',
        teams=teams_list,
        experts=all_experts,
        teams_map=teams_map,
        team_data=cached['team_data'],
        all_records=cached['all_records'][:500],
        selected_team=filter_team or '',
        selected_expert=filter_expert or '',
        start_date=start_date,
        end_date=end_date,
        overall_total=cached['overall_total'],
        total_records=cached['total_records']
    )


@analytics_bp.route('/export')
def export_center():
    db = get_db()
    start_date, end_date = get_date_filter_strings()

    teams_list, experts, teams_map = get_analytics_filter_options(completed_only=True)
    export_options = get_export_filter_options()

    return render_template(
        'export_center.html',
        teams=teams_list,
        experts=experts,
        technologies=export_options['technologies'],
        workflow_statuses=export_options['workflow_statuses'],
        teams_map=teams_map,
        start_date=start_date,
        end_date=end_date
    )


@analytics_bp.route('/export/preview', methods=['POST'])
def export_preview():
    """Return preview data for the export (first 20 records)."""
    db = get_db()

    start_date = request.form.get('start_date', '')
    end_date = request.form.get('end_date', '')
    export_type = request.form.get('export_type', 'interview_records')
    filter_team = request.form.get('team', '') or None
    filter_expert = normalize_lookup_text(request.form.get('expert', '')) or None

    # Get active experts
    active_experts = get_active_experts(db)
    expert_team_map, teams_map = get_expert_team_map(db)

    preview_data = []
    total_count = 0
    fields = []

    if export_type == 'interview_records':
        # Get interview records preview
        match_filters = build_task_query(start_date, end_date)
        records = list(db.taskBody.find(match_filters, {
            "assignedTo": 1, "subject": 1, "Candidate Name": 1,
            "actualRound": 1, "receivedDateTime": 1, "status": 1, "_id": 0
        }).limit(500))

        for r in records:
            expert = r.get('assignedTo', '')
            expert_key = normalize_lookup_text(expert)
            if expert_key not in active_experts:
                continue
            team = expert_team_map.get(expert_key, "Unmapped")
            if filter_team and team != filter_team:
                continue
            if filter_expert and expert_key != filter_expert:
                continue
            preview_data.append({
                'Team': team,
                'Expert': expert_key.split('@')[0] if '@' in expert_key else expert_key,
                'Subject': (r.get('subject', '') or '')[:50] + '...' if len(r.get('subject', '') or '') > 50 else r.get('subject', ''),
                'Round': r.get('actualRound', ''),
                'Date': (r.get('receivedDateTime', '') or '')[:10],
                'Status': r.get('status', '')
            })
        fields = ['Team', 'Expert', 'Subject', 'Round', 'Date', 'Status']

    elif export_type == 'team_summary':
        expert_stats, _ = get_expert_funnel_data(db, start_date, end_date, filter_team, filter_expert)
        for exp in expert_stats:
            preview_data.append({
                'Team': exp.get('team', ''),
                'Expert': exp.get('expert', '').split('@')[0],
                'Interviews': exp.get('interview_count', 0),
                'Screening': exp.get('screening', 0),
                'Final': exp.get('final', 0)
            })
        fields = ['Team', 'Expert', 'Interviews', 'Screening', 'Final']

    elif export_type == 'funnel_combined':
        expert_stats, _ = get_expert_funnel_data(db, start_date, end_date, filter_team, filter_expert)
        for exp in expert_stats:
            preview_data.append({
                'Expert': exp.get('expert', '').split('@')[0],
                'Team': exp.get('team', ''),
                'Screening': exp.get('screening', 0),
                '1st': exp.get('first', 0),
                '2nd': exp.get('second', 0),
                '3rd/Tech': exp.get('third_tech', 0),
                'Loop Round': exp.get('loop_round', 0),
                'Final': exp.get('final', 0),
                'Conv%': exp.get('screening_to_1st', 0)
            })
        fields = ['Expert', 'Team', 'Screening', '1st', '2nd', '3rd/Tech', 'Loop Round', 'Final', 'Conv%']

    elif export_type == 'experts':
        expert_stats, _ = get_expert_funnel_data(db, start_date, end_date, filter_team, filter_expert)
        for exp in expert_stats:
            preview_data.append({
                'Rank': exp.get('rank', 0),
                'Expert': exp.get('expert', '').split('@')[0],
                'Team': exp.get('team', ''),
                'Screening': exp.get('screening', 0),
                '1st': exp.get('first', 0),
                '2nd': exp.get('second', 0),
                '3rd/Tech': exp.get('third_tech', 0),
                'Loop Round': exp.get('loop_round', 0),
                'Final': exp.get('final', 0),
                'Total': exp.get('interview_count', 0),
                'Conv%': exp.get('screening_to_1st', 0)
            })
        fields = ['Rank', 'Expert', 'Team', 'Screening', '1st', '2nd', '3rd/Tech', 'Loop Round', 'Final', 'Total', 'Conv%']

    elif export_type == 'interview_stats':
        # Interview stats: Completed, Cancelled, Rescheduled counts per expert
        date_match = {}
        if start_date or end_date:
            date_filter = {}
            if start_date:
                date_filter["$gte"] = f"{start_date}T00:00:00" if 'T' not in start_date else start_date
            if end_date:
                date_filter["$lte"] = f"{end_date}T23:59:59" if 'T' not in end_date else end_date
            if date_filter:
                date_match["receivedDateTime"] = date_filter

        pipeline = [
            {"$match": {**date_match, "assignedTo": {"$type": "string", "$ne": ""}}},
            {"$group": {
                "_id": "$assignedTo",
                "Completed": {"$sum": {"$cond": [{"$eq": ["$status", "Completed"]}, 1, 0]}},
                "Cancelled": {"$sum": {"$cond": [{"$eq": ["$status", "Cancelled"]}, 1, 0]}},
                "Rescheduled": {"$sum": {"$cond": [{"$eq": ["$status", "Rescheduled"]}, 1, 0]}},
                "Total": {"$sum": 1}
            }}
        ]
        results = list(db.taskBody.aggregate(pipeline))

        preview_map = {}
        for r in results:
            expert = r['_id']
            expert_key = normalize_lookup_text(expert)
            if expert_key not in active_experts:
                continue
            team = expert_team_map.get(expert_key, "Unmapped")
            if filter_team and team != filter_team:
                continue
            if filter_expert and expert_key != filter_expert:
                continue
            preview_map.setdefault(
                expert_key,
                {
                    'Team': team,
                    'Expert': expert_key.split('@')[0] if '@' in expert_key else expert_key,
                    'Completed': 0,
                    'Cancelled': 0,
                    'Rescheduled': 0,
                    'Total': 0,
                },
            )
            preview_map[expert_key]['Completed'] += r.get('Completed', 0)
            preview_map[expert_key]['Cancelled'] += r.get('Cancelled', 0)
            preview_map[expert_key]['Rescheduled'] += r.get('Rescheduled', 0)
            preview_map[expert_key]['Total'] += r.get('Total', 0)
        preview_data.extend(preview_map.values())
        fields = ['Team', 'Expert', 'Completed', 'Cancelled', 'Rescheduled', 'Total']

    total_count = len(preview_data)
    preview_data = preview_data[:20]  # Return only first 20 for preview

    return jsonify({
        'success': True,
        'data': preview_data,
        'total_count': total_count,
        'fields': fields,
        'field_count': len(fields)
    })


@analytics_bp.route('/export/download', methods=['POST'])
def export_download():
    """Handle export downloads with Excel, CSV, and JSON support."""
    db = get_db()

    start_date = request.form.get('start_date', '')
    end_date = request.form.get('end_date', '')
    export_type = request.form.get('export_type', 'experts')
    export_format = request.form.get('format', 'excel')
    filter_team = request.form.get('team', '') or None
    filter_expert = normalize_lookup_text(request.form.get('expert', '')) or None

    # Get active experts
    active_experts = get_active_experts(db)
    expert_team_map, teams_map = get_expert_team_map(db)

    if export_type == 'interview_records':
        return export_interview_records_excel(db, start_date, end_date, filter_team, filter_expert,
                                             active_experts, expert_team_map, teams_map, export_format)

    elif export_type == 'team_summary':
        return export_team_summary_excel(db, start_date, end_date, filter_team, filter_expert,
                                        active_experts, expert_team_map, teams_map, export_format)

    elif export_type == 'funnel_combined':
        return export_funnel_combined_excel(db, start_date, end_date, filter_team, filter_expert, export_format)

    elif export_type == 'experts':
        expert_stats, _ = get_expert_funnel_data(db, start_date, end_date, filter_team, filter_expert)
        return export_experts_excel(expert_stats, start_date, end_date, export_format)

    elif export_type == 'teams':
        team_stats, _ = get_team_funnel_data(db, start_date, end_date, filter_team, filter_expert)
        return export_teams_excel(team_stats, start_date, end_date, export_format)

    elif export_type == 'interview_stats':
        return export_interview_stats_excel(db, start_date, end_date, filter_team, filter_expert,
                                           active_experts, expert_team_map, export_format)

    else:
        return jsonify({'success': False, 'error': 'Invalid export type'})


def export_interview_records_excel(db, start_date, end_date, filter_team, filter_expert,
                                   active_experts, expert_team_map, teams_map, export_format='excel'):
    """
    EXPORT TYPE 1: Interview Records
    Columns: Team, Expert, Subject, Candidate, Round, ReceivedDateTime, Status
    """
    # Build query
    match_filters = build_task_query(start_date, end_date)

    # Get interview records
    records = list(db.taskBody.find(
        match_filters,
        {
            "assignedTo": 1,
            "subject": 1,
            "Candidate Name": 1,
            "actualRound": 1,
            "receivedDateTime": 1,
            "status": 1,
            "_id": 0
        }
    ).limit(10000))

    excel_rows = []
    for r in records:
        expert = r.get('assignedTo', '')
        expert_key = normalize_lookup_text(expert)

        # Skip inactive experts
        if expert_key not in active_experts:
            continue

        # Get team
        team = expert_team_map.get(expert_key, "Unmapped")

        # Apply filters
        if filter_team and team != filter_team:
            continue
        if filter_expert and expert_key != filter_expert:
            continue

        excel_rows.append({
            'Team': team,
            'Expert': expert_key,
            'Subject': r.get('subject', ''),
            'Candidate': r.get('Candidate Name', ''),
            'Round': r.get('actualRound', ''),
            'ReceivedDateTime': r.get('receivedDateTime', ''),
            'Status': r.get('status', '')
        })

    if not excel_rows:
        return jsonify({'success': False, 'error': 'No records found for the given filters'})

    # Create DataFrame and sort
    df = pd.DataFrame(excel_rows)
    df = df.sort_values(by=["Team", "Expert", "ReceivedDateTime"], ascending=[True, True, True])

    # Generate filename base
    start_str = start_date[:10] if start_date else 'all'
    end_str = end_date[:10] if end_date else 'all'

    if export_format == 'csv':
        output = BytesIO()
        df.to_csv(output, index=False)
        output.seek(0)
        return send_file(
            output,
            mimetype='text/csv',
            as_attachment=True,
            download_name=f"interviews_{start_str}_to_{end_str}.csv"
        )
    else:
        # Excel format (default)
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Interview_Records', index=False)
        output.seek(0)
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=f"interviews_{start_str}_to_{end_str}.xlsx"
        )


def export_team_summary_excel(db, start_date, end_date, filter_team, filter_expert,
                              active_experts, expert_team_map, teams_map, export_format='excel'):
    """
    EXPORT TYPE 2: Team Summary
    Columns: Team, Expert, Total_Interviews, Completed, Cancelled, Rescheduled
    """
    # Build query
    match_filters = build_task_query(start_date, end_date)
    match_filters["status"] = {"$in": ["Completed", "Cancelled", "Rescheduled"]}

    # Get interview stats by expert
    pipeline = [
        {"$match": match_filters},
        {
            "$group": {
                "_id": {
                    "expert": "$assignedTo",
                    "status": "$status"
                },
                "count": {"$sum": 1}
            }
        }
    ]

    results = list(db.taskBody.aggregate(pipeline))

    # Process results
    expert_stats = {}
    for r in results:
        expert = r['_id']['expert']
        expert_key = normalize_lookup_text(expert)
        status = r['_id']['status']
        count = r['count']

        # Skip inactive experts
        if expert_key not in active_experts:
            continue

        if expert_key not in expert_stats:
            expert_stats[expert_key] = {'Completed': 0, 'Cancelled': 0, 'Rescheduled': 0}

        expert_stats[expert_key][status] += count

    excel_rows = []
    for expert, stats in expert_stats.items():
        team = expert_team_map.get(expert, "Unmapped")

        # Apply filters
        if filter_team and team != filter_team:
            continue
        if filter_expert and expert != filter_expert:
            continue

        total = stats['Completed'] + stats['Cancelled'] + stats['Rescheduled']

        excel_rows.append({
            'Team': team,
            'Expert': expert,
            'Total_Interviews': total,
            'Completed': stats['Completed'],
            'Cancelled': stats['Cancelled'],
            'Rescheduled': stats['Rescheduled']
        })

    if not excel_rows:
        return jsonify({'success': False, 'error': 'No data to export for the given filters'})

    # Create DataFrame and sort
    df = pd.DataFrame(excel_rows)
    df = df.sort_values(by=["Team", "Expert"], ascending=[True, True])

    start_str = start_date[:10] if start_date else 'all'
    end_str = end_date[:10] if end_date else 'all'

    if export_format == 'csv':
        output = BytesIO()
        df.to_csv(output, index=False)
        output.seek(0)
        return send_file(
            output,
            mimetype='text/csv',
            as_attachment=True,
            download_name=f"team_summary_{start_str}_to_{end_str}.csv"
        )
    else:
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Team_Summary', index=False)
        output.seek(0)
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=f"team_summary_{start_str}_to_{end_str}.xlsx"
        )


def export_funnel_combined_excel(db, start_date, end_date, filter_team, filter_expert, export_format='excel'):
    """
    EXPORT TYPE 3 & 4: Expert & Team Funnel Combined
    Two sheets: Expert_Funnel and Team_Funnel
    """
    # Get expert and team funnel data
    expert_stats, _ = get_expert_funnel_data(db, start_date, end_date, filter_team, filter_expert)
    team_stats, _ = get_team_funnel_data(db, start_date, end_date, filter_team, filter_expert)

    # Create expert DataFrame
    expert_rows = []
    for exp in expert_stats:
        expert_rows.append({
            'Rank': exp.get('rank', 0),
            'Expert': exp.get('expert', ''),
            'Team': exp.get('team', ''),
            'Screening': exp.get('screening', 0),
            '1st': exp.get('first', 0),
            '2nd': exp.get('second', 0),
            '3rd/Technical': exp.get('third_tech', 0),
            'Loop Round': exp.get('loop_round', 0),
            'Final': exp.get('final', 0),
            'Total_Interviews': exp.get('interview_count', 0),
            'Screening_to_1st_%': exp.get('screening_to_1st', 0),
            '1st_to_2nd_%': exp.get('first_to_2nd', 0),
            '2nd_to_3rd_%': exp.get('second_to_3rd', 0),
            '3rd_to_Loop_%': exp.get('third_to_loop', 0),
            'Loop_to_Final_%': exp.get('loop_to_final', 0),
        })

    # Create team DataFrame
    team_rows = []
    for team in team_stats:
        team_rows.append({
            'Rank': team.get('rank', 0),
            'Team': team.get('team', ''),
            'Members': team.get('member_count', 0),
            'Screening': team.get('screening', 0),
            '1st': team.get('first', 0),
            '2nd': team.get('second', 0),
            '3rd/Technical': team.get('third_tech', 0),
            'Loop Round': team.get('loop_round', 0),
            'Final': team.get('final', 0),
            'Total_Interviews': team.get('interview_count', 0),
            'Screening_to_1st_%': team.get('screening_to_1st', 0),
            '1st_to_2nd_%': team.get('first_to_2nd', 0),
            '2nd_to_3rd_%': team.get('second_to_3rd', 0),
            '3rd_to_Loop_%': team.get('third_to_loop', 0),
            'Loop_to_Final_%': team.get('loop_to_final', 0),
        })

    if not expert_rows and not team_rows:
        return jsonify({'success': False, 'error': 'No funnel data available'})

    expert_df = pd.DataFrame(expert_rows)
    team_df = pd.DataFrame(team_rows)

    if export_format == 'csv':
        # For CSV, combine both dataframes with a separator
        output = BytesIO()
        if not expert_df.empty:
            expert_df.to_csv(output, index=False)
            output.write(b'\n\n--- Team Funnel ---\n\n')
        if not team_df.empty:
            team_df.to_csv(output, index=False, header=True)
        output.seek(0)
        return send_file(
            output,
            mimetype='text/csv',
            as_attachment=True,
            download_name="expert_team_conversion_funnel.csv"
        )
    else:
        # Create Excel file with multiple sheets
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            if not expert_df.empty:
                expert_df.to_excel(writer, sheet_name='Expert_Funnel', index=False)
            if not team_df.empty:
                team_df.to_excel(writer, sheet_name='Team_Funnel', index=False)
        output.seek(0)
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name="expert_team_conversion_funnel.xlsx"
        )


def export_experts_excel(expert_stats, start_date, end_date, export_format='excel'):
    """Simple expert funnel export."""
    rows = []
    for exp in expert_stats:
        rows.append({
            'Rank': exp.get('rank', 0),
            'Expert': exp.get('expert', ''),
            'Team': exp.get('team', ''),
            'Screening': exp.get('screening', 0),
            '1st': exp.get('first', 0),
            '2nd': exp.get('second', 0),
            '3rd/Technical': exp.get('third_tech', 0),
            'Loop Round': exp.get('loop_round', 0),
            'Final': exp.get('final', 0),
            'Total': exp.get('interview_count', 0),
            'Conversion_%': exp.get('screening_to_1st', 0)
        })

    df = pd.DataFrame(rows)
    start_str = start_date[:10] if start_date else 'all'
    end_str = end_date[:10] if end_date else 'all'

    if export_format == 'csv':
        output = BytesIO()
        df.to_csv(output, index=False)
        output.seek(0)
        return send_file(
            output,
            mimetype='text/csv',
            as_attachment=True,
            download_name=f"expert_analytics_{start_str}_to_{end_str}.csv"
        )
    else:
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Experts', index=False)
        output.seek(0)
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=f"expert_analytics_{start_str}_to_{end_str}.xlsx"
        )


def export_teams_excel(team_stats, start_date, end_date, export_format='excel'):
    """Simple team funnel export."""
    rows = []
    for team in team_stats:
        rows.append({
            'Rank': team.get('rank', 0),
            'Team': team.get('team', ''),
            'Members': team.get('member_count', 0),
            'Screening': team.get('screening', 0),
            '1st': team.get('first', 0),
            '2nd': team.get('second', 0),
            '3rd/Technical': team.get('third_tech', 0),
            'Loop Round': team.get('loop_round', 0),
            'Final': team.get('final', 0),
            'Total': team.get('interview_count', 0),
            'Conversion_%': team.get('screening_to_1st', 0)
        })

    df = pd.DataFrame(rows)
    start_str = start_date[:10] if start_date else 'all'
    end_str = end_date[:10] if end_date else 'all'

    if export_format == 'csv':
        output = BytesIO()
        df.to_csv(output, index=False)
        output.seek(0)
        return send_file(
            output,
            mimetype='text/csv',
            as_attachment=True,
            download_name=f"team_analytics_{start_str}_to_{end_str}.csv"
        )
    else:
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Teams', index=False)
        output.seek(0)
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=f"team_analytics_{start_str}_to_{end_str}.xlsx"
        )


def export_interview_stats_excel(db, start_date, end_date, filter_team, filter_expert,
                                 active_experts, expert_team_map, export_format='excel'):
    """
    Export interview stats: Completed, Cancelled, Rescheduled counts per expert.
    """
    date_match = {}
    if start_date or end_date:
        date_filter = {}
        if start_date:
            date_filter["$gte"] = f"{start_date}T00:00:00" if 'T' not in start_date else start_date
        if end_date:
            date_filter["$lte"] = f"{end_date}T23:59:59" if 'T' not in end_date else end_date
        if date_filter:
            date_match["receivedDateTime"] = date_filter

    pipeline = [
        {"$match": {**date_match, "assignedTo": {"$type": "string", "$ne": ""}}},
        {"$group": {
            "_id": "$assignedTo",
            "Completed": {"$sum": {"$cond": [{"$eq": ["$status", "Completed"]}, 1, 0]}},
            "Cancelled": {"$sum": {"$cond": [{"$eq": ["$status", "Cancelled"]}, 1, 0]}},
            "Rescheduled": {"$sum": {"$cond": [{"$eq": ["$status", "Rescheduled"]}, 1, 0]}},
            "Total": {"$sum": 1}
        }}
    ]
    results = list(db.taskBody.aggregate(pipeline))

    merged_rows = {}
    for r in results:
        expert = r['_id']
        expert_key = normalize_lookup_text(expert)
        if expert_key not in active_experts:
            continue
        team = expert_team_map.get(expert_key, "Unmapped")
        if filter_team and team != filter_team:
            continue
        if filter_expert and expert_key != filter_expert:
            continue
        merged_rows.setdefault(
            expert_key,
            {
                'Team': team,
                'Expert': expert_key,
                'Completed': 0,
                'Cancelled': 0,
                'Rescheduled': 0,
                'Total': 0,
            },
        )
        merged_rows[expert_key]['Completed'] += r.get('Completed', 0)
        merged_rows[expert_key]['Cancelled'] += r.get('Cancelled', 0)
        merged_rows[expert_key]['Rescheduled'] += r.get('Rescheduled', 0)
        merged_rows[expert_key]['Total'] += r.get('Total', 0)

    excel_rows = list(merged_rows.values())

    if not excel_rows:
        return jsonify({'success': False, 'error': 'No data to export for the given filters'})

    df = pd.DataFrame(excel_rows)
    df = df.sort_values(by=["Team", "Expert"], ascending=[True, True])

    start_str = start_date[:10] if start_date else 'all'
    end_str = end_date[:10] if end_date else 'all'

    if export_format == 'csv':
        output = BytesIO()
        df.to_csv(output, index=False)
        output.seek(0)
        return send_file(
            output,
            mimetype='text/csv',
            as_attachment=True,
            download_name=f"interview_stats_{start_str}_to_{end_str}.csv"
        )
    else:
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Interview_Stats', index=False)
        output.seek(0)
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=f"interview_stats_{start_str}_to_{end_str}.xlsx"
        )
