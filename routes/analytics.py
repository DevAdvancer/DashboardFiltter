from flask import Blueprint, render_template, request, jsonify
from db import get_db
from datetime import datetime
from collections import Counter, defaultdict

analytics_bp = Blueprint('analytics', __name__)

# Round mapping from actualRound to funnel stages
ROUND_BUCKETS = {
    "screening": "Screening",
    "1st round": "1st",
    "first round": "1st",
    "2nd round": "2nd",
    "second round": "2nd",
    "3rd round": "3rd/Technical",
    "third round": "3rd/Technical",
    "technical": "3rd/Technical",
    "technical round": "3rd/Technical",
    "final": "Final",
    "final round": "Final",
    "loop round": "Final",
}

PIPELINE_ORDER = ["Screening", "1st", "2nd", "3rd/Technical", "Final"]


def normalize_round(r):
    """Normalize actualRound string to funnel stage."""
    if not r:
        return None
    key = str(r).strip().lower()
    return ROUND_BUCKETS.get(key)


def pct(num, den):
    """Calculate percentage."""
    if den and den > 0:
        return round((num / den) * 100, 1)
    return 0.0


def get_date_filter_strings():
    """Get date filter as strings for taskBody collection."""
    start_date = request.args.get('start_date', '')
    end_date = request.args.get('end_date', '')
    return start_date, end_date


def get_expert_team_map(db):
    """Build expert -> team mapping."""
    teams_cursor = db.teams.find({})
    teams_map = {t['name']: t.get('members', []) for t in teams_cursor}

    expert_team_map = {}
    for team_name, members in teams_map.items():
        for member in members:
            expert_team_map[str(member).lower()] = team_name

    return expert_team_map, teams_map


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
    """Get expert funnel data from taskBody collection."""
    expert_team_map, teams_map = get_expert_team_map(db)

    # Build query
    match_filters = build_task_query(start_date, end_date)

    # Get all completed tasks
    docs = list(db.taskBody.find(match_filters, {
        "assignedTo": 1,
        "Candidate Name": 1,
        "actualRound": 1,
        "receivedDateTime": 1,
    }))

    # Aggregate by expert and stage
    expert_stage_counts = defaultdict(lambda: Counter())

    for doc in docs:
        expert = doc.get("assignedTo")
        if not expert:
            continue

        stage = normalize_round(doc.get("actualRound"))
        if not stage:
            continue

        expert_stage_counts[expert][stage] += 1

    # Build expert stats
    expert_stats = []
    for expert, stages in expert_stage_counts.items():
        team_name = expert_team_map.get(str(expert).lower(), "Unmapped")

        # Apply filters
        if filter_team and team_name != filter_team:
            continue
        if filter_expert and expert != filter_expert:
            continue

        scr = stages.get("Screening", 0)
        r1 = stages.get("1st", 0)
        r2 = stages.get("2nd", 0)
        r3 = stages.get("3rd/Technical", 0)
        fin = stages.get("Final", 0)

        # Interview count = 1st + 2nd + 3rd + Final (excludes Screening)
        total_interviews = r1 + r2 + r3 + fin

        # Conversion rates
        scr_to_1st = pct(r1, scr)
        first_to_second = pct(r2, r1)
        second_to_third = pct(r3, r2)
        third_to_final = pct(fin, r3)

        expert_stats.append({
            'expert': expert,
            'team': team_name,
            'interview_count': total_interviews,
            'screening': scr,
            'first': r1,
            'second': r2,
            'third_tech': r3,
            'final': fin,
            'screening_to_1st': scr_to_1st,
            'first_to_2nd': first_to_second,
            'second_to_3rd': second_to_third,
            'third_to_final': third_to_final,
        })

    # Sort by ScreeningTO1st conversion, then interview volume
    expert_stats.sort(key=lambda x: (x['screening_to_1st'], x['interview_count']), reverse=True)

    # Add rank
    for idx, stat in enumerate(expert_stats):
        stat['rank'] = idx + 1

    return expert_stats, teams_map


def get_team_funnel_data(db, start_date='', end_date='', filter_team=None, filter_expert=None):
    """Get team funnel data from taskBody collection."""
    expert_team_map, teams_map = get_expert_team_map(db)

    # Get expert funnel data first
    expert_stats, _ = get_expert_funnel_data(db, start_date, end_date)

    # Create expert -> stats map
    expert_stats_map = {stat['expert']: stat for stat in expert_stats}

    # Aggregate by team
    team_stats = []
    for team_name, members in teams_map.items():
        # Apply team filter
        if filter_team and team_name != filter_team:
            continue

        effective_members = members

        # If filtering by expert, only include that expert
        if filter_expert:
            effective_members = [m for m in members if m == filter_expert]
            if not effective_members:
                continue

        # Aggregate stages
        agg = Counter()
        for member in effective_members:
            stats = expert_stats_map.get(member)
            if stats:
                agg['Screening'] += stats['screening']
                agg['1st'] += stats['first']
                agg['2nd'] += stats['second']
                agg['3rd/Technical'] += stats['third_tech']
                agg['Final'] += stats['final']

        scr = agg.get("Screening", 0)
        r1 = agg.get("1st", 0)
        r2 = agg.get("2nd", 0)
        r3 = agg.get("3rd/Technical", 0)
        fin = agg.get("Final", 0)

        total_interviews = r1 + r2 + r3 + fin

        team_stats.append({
            'team': team_name,
            'member_count': len(members),
            'interview_count': total_interviews,
            'screening': scr,
            'first': r1,
            'second': r2,
            'third_tech': r3,
            'final': fin,
            'screening_to_1st': pct(r1, scr),
            'first_to_2nd': pct(r2, r1),
            'second_to_3rd': pct(r3, r2),
            'third_to_final': pct(fin, r3),
        })

    # Sort by ScreeningTO1st conversion, then interview volume
    team_stats.sort(key=lambda x: (x['screening_to_1st'], x['interview_count']), reverse=True)

    # Add rank
    for idx, stat in enumerate(team_stats):
        stat['rank'] = idx + 1

    return team_stats, teams_map


@analytics_bp.route('/experts')
def expert_analytics():
    db = get_db()
    start_date, end_date = get_date_filter_strings()

    # Get filter parameters
    filter_team = request.args.get('team', '') or None
    filter_expert = request.args.get('expert', '') or None

    # Get teams for filter dropdown
    expert_team_map, teams_map = get_expert_team_map(db)
    teams_list = list(teams_map.keys())

    # Get all experts for filter dropdown
    all_experts = sorted(db.taskBody.distinct('assignedTo', {
        "status": "Completed",
        "assignedTo": {"$type": "string", "$ne": ""},
    }))

    # Get expert funnel data
    expert_stats, _ = get_expert_funnel_data(db, start_date, end_date, filter_team, filter_expert)

    # Get selected expert detail
    selected_expert = request.args.get('view_expert', '')
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
            # Get recent tasks for this expert
            task_query = build_task_query(start_date, end_date)
            task_query['assignedTo'] = selected_expert

            expert_tasks = list(db.taskBody.find(task_query).sort('receivedDateTime', -1).limit(25))

            # Get round distribution for charts
            round_pipeline = [
                {'$match': task_query},
                {'$group': {'_id': '$actualRound', 'count': {'$sum': 1}}},
                {'$sort': {'count': -1}}
            ]
            round_dist = list(db.taskBody.aggregate(round_pipeline))
            expert_detail['round_distribution'] = {
                (item['_id'] or 'Unknown'): item['count']
                for item in round_dist
            }

    return render_template(
        'expert_analytics.html',
        expert_stats=expert_stats,
        teams=teams_list,
        experts=all_experts,
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
    filter_expert = request.args.get('expert', '') or None

    # Get teams for filter dropdown
    expert_team_map, teams_map = get_expert_team_map(db)
    teams_list = list(teams_map.keys())

    # Get team funnel data
    team_stats, _ = get_team_funnel_data(db, start_date, end_date, filter_team, filter_expert)

    # Get selected team detail
    selected_team = request.args.get('view_team', '')
    team_detail = None
    member_stats = []

    if selected_team:
        # Find team stats
        for stat in team_stats:
            if stat['team'] == selected_team:
                team_detail = stat
                break

        if team_detail:
            # Get member-level breakdown
            members = teams_map.get(selected_team, [])
            expert_stats, _ = get_expert_funnel_data(db, start_date, end_date, selected_team, None)

            # Filter to only this team's members
            member_stats = [s for s in expert_stats if s['expert'] in members]

    return render_template(
        'team_analytics.html',
        team_stats=team_stats,
        teams=teams_list,
        selected_team=filter_team or '',
        view_team=selected_team,
        team_detail=team_detail,
        member_stats=member_stats,
        start_date=start_date,
        end_date=end_date,
        total_teams=len(team_stats)
    )


@analytics_bp.route('/funnel')
def funnel_analytics():
    db = get_db()
    start_date, end_date = get_date_filter_strings()

    # Get filter parameters
    filter_team = request.args.get('team', '') or None

    # Get teams for filter dropdown
    expert_team_map, teams_map = get_expert_team_map(db)
    teams_list = list(teams_map.keys())

    # Get overall funnel data
    expert_stats, _ = get_expert_funnel_data(db, start_date, end_date, filter_team, None)

    # Aggregate totals
    total_screening = sum(s['screening'] for s in expert_stats)
    total_first = sum(s['first'] for s in expert_stats)
    total_second = sum(s['second'] for s in expert_stats)
    total_third = sum(s['third_tech'] for s in expert_stats)
    total_final = sum(s['final'] for s in expert_stats)

    total_interviews = total_first + total_second + total_third + total_final

    funnel_totals = {
        'screening': total_screening,
        'first': total_first,
        'second': total_second,
        'third_tech': total_third,
        'final': total_final,
        'total_interviews': total_interviews,
        'screening_to_1st': pct(total_first, total_screening),
        'first_to_2nd': pct(total_second, total_first),
        'second_to_3rd': pct(total_third, total_second),
        'third_to_final': pct(total_final, total_third),
    }

    # Get team-level funnel
    team_stats, _ = get_team_funnel_data(db, start_date, end_date, filter_team, None)

    return render_template(
        'funnel_analytics.html',
        funnel_totals=funnel_totals,
        expert_stats=expert_stats[:20],
        team_stats=team_stats,
        teams=teams_list,
        selected_team=filter_team or '',
        start_date=start_date,
        end_date=end_date
    )


@analytics_bp.route('/export')
def export_center():
    db = get_db()
    start_date, end_date = get_date_filter_strings()

    # Get filter options for export
    expert_team_map, teams_map = get_expert_team_map(db)
    teams_list = list(teams_map.keys())

    experts = sorted(db.taskBody.distinct('assignedTo', {
        "status": "Completed",
        "assignedTo": {"$type": "string", "$ne": ""},
    }))

    return render_template(
        'export_center.html',
        teams=teams_list,
        experts=experts,
        start_date=start_date,
        end_date=end_date
    )


@analytics_bp.route('/export/download', methods=['POST'])
def export_download():
    """Handle export downloads."""
    db = get_db()

    start_date = request.form.get('start_date', '')
    end_date = request.form.get('end_date', '')
    export_type = request.form.get('export_type', 'experts')
    filter_team = request.form.get('team', '') or None
    filter_expert = request.form.get('expert', '') or None

    if export_type == 'experts':
        expert_stats, _ = get_expert_funnel_data(db, start_date, end_date, filter_team, filter_expert)
        return jsonify({
            'success': True,
            'type': 'experts',
            'count': len(expert_stats),
            'data': expert_stats
        })
    elif export_type == 'teams':
        team_stats, _ = get_team_funnel_data(db, start_date, end_date, filter_team, filter_expert)
        return jsonify({
            'success': True,
            'type': 'teams',
            'count': len(team_stats),
            'data': team_stats
        })
    else:
        return jsonify({'success': False, 'error': 'Invalid export type'})
