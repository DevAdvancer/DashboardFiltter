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
    """
    Get expert funnel data from taskBody collection.

    Expert-Level Filtration Rules:
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
    expert_team_map, teams_map = get_expert_team_map(db)

    # Build query for taskBody collection
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

    # Build expert stats with filtration
    expert_stats = []
    for expert, stages in expert_stage_counts.items():
        # Step 1: Determine expert's team (case-insensitive)
        team_name = expert_team_map.get(str(expert).lower(), "Unmapped")

        # Step 2: Apply Team Filter
        # If filter_team is set, expert must belong to that team
        if filter_team is not None and team_name != filter_team:
            continue  # Skip experts not in the filtered team

        # Step 3: Apply Expert Filter
        # If filter_expert is set, expert email must match exactly
        if filter_expert is not None and expert != filter_expert:
            continue  # Skip experts that don't match the filter

        # Expert passed all filters - include in results
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

    # Add rank (based on filtered results)
    for idx, stat in enumerate(expert_stats):
        stat['rank'] = idx + 1

    return expert_stats, teams_map


def get_team_funnel_data(db, start_date='', end_date='', filter_team=None, filter_expert=None):
    """
    Get team funnel data from taskBody collection.

    Filtration Rules:
    1. If filter_team is set: only include that team
    2. If filter_expert is set: only include that expert's data within teams
    3. If both are set: expert must belong to filter_team AND match filter_expert
    4. Team aggregations only include data from experts that pass all filters
    """
    expert_team_map, teams_map = get_expert_team_map(db)

    # IMPORTANT: Get expert funnel data WITH the same filters applied
    # This ensures alignment - filtered-out experts won't contribute to team totals
    expert_stats, _ = get_expert_funnel_data(db, start_date, end_date, filter_team, filter_expert)

    # Create expert -> stats map (only contains filtered experts)
    expert_stats_map = {stat['expert']: stat for stat in expert_stats}

    # Aggregate by team
    team_stats = []
    for team_name, members in teams_map.items():
        # Apply team filter: skip teams that don't match
        if filter_team and team_name != filter_team:
            continue

        # Determine effective members based on filters
        effective_members = members

        # If filtering by expert, only include that expert in this team
        if filter_expert:
            effective_members = [m for m in members if m == filter_expert]
            # If this team doesn't contain the filtered expert, skip the team entirely
            if not effective_members:
                continue

        # Aggregate stages ONLY from filtered experts
        # (expert_stats_map only contains experts that passed all filters)
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
                agg['Final'] += stats['final']

        # Skip teams with no contributing members (all filtered out)
        if contributing_members == 0:
            continue

        scr = agg.get("Screening", 0)
        r1 = agg.get("1st", 0)
        r2 = agg.get("2nd", 0)
        r3 = agg.get("3rd/Technical", 0)
        fin = agg.get("Final", 0)

        total_interviews = r1 + r2 + r3 + fin

        team_stats.append({
            'team': team_name,
            'member_count': len(members),  # Total team members
            'active_member_count': contributing_members,  # Members with data after filtering
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

    # Get all experts for filter dropdown
    all_experts = sorted(db.taskBody.distinct('assignedTo', {
        "status": "Completed",
        "assignedTo": {"$type": "string", "$ne": ""},
    }))

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

    # Get overall funnel data (with filters for alignment)
    expert_stats, _ = get_expert_funnel_data(db, start_date, end_date, filter_team, filter_expert)

    # Aggregate totals from filtered experts only
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

    # Get team-level funnel (with same filters for alignment)
    team_stats, _ = get_team_funnel_data(db, start_date, end_date, filter_team, filter_expert)

    return render_template(
        'funnel_analytics.html',
        funnel_totals=funnel_totals,
        expert_stats=expert_stats[:20],
        team_stats=team_stats,
        teams=teams_list,
        experts=all_experts,
        selected_team=filter_team or '',
        selected_expert=filter_expert or '',
        start_date=start_date,
        end_date=end_date
    )


@analytics_bp.route('/interview-stats')
def interview_stats():
    """
    Interview Statistics page showing Completed, Cancelled, Rescheduled counts
    per expert/team with date filtering.
    """
    db = get_db()
    start_date, end_date = get_date_filter_strings()

    # Get filter parameters
    filter_team = request.args.get('team', '') or None
    filter_expert = request.args.get('expert', '') or None

    # Get expert-team mapping
    expert_team_map, teams_map = get_expert_team_map(db)
    teams_list = sorted(teams_map.keys())

    # Get all experts for filter dropdown
    all_experts = sorted(db.taskBody.distinct('assignedTo', {
        "assignedTo": {"$type": "string", "$ne": ""},
    }))

    # Build date filter for pipeline
    date_match = {}
    if start_date or end_date:
        date_filter = {}
        if start_date:
            if 'T' not in start_date:
                start_date_val = f"{start_date}T00:00:00"
            else:
                start_date_val = start_date
            date_filter["$gte"] = start_date_val
        if end_date:
            if 'T' not in end_date:
                end_date_val = f"{end_date}T23:59:59"
            else:
                end_date_val = end_date
            date_filter["$lte"] = end_date_val
        if date_filter:
            date_match["receivedDateTime"] = date_filter

    # MongoDB aggregation pipeline for interview stats
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
                "TotalInterviews": 1
            }
        }
    ]

    results = list(db.taskBody.aggregate(pipeline))

    # Map: expert email -> their stats
    expert_stats_map = {r["Expert"]: r for r in results}

    # Build team stats with members
    team_data = []
    expert_data = []

    for team_name, members in teams_map.items():
        # Filter by team
        if filter_team and team_name != filter_team:
            continue

        # Filter by expert within team
        effective_members = members
        if filter_expert:
            effective_members = [m for m in members if m == filter_expert]
            if not effective_members:
                continue

        # Compute team totals
        team_completed = team_cancelled = team_rescheduled = team_total = 0
        member_stats = []

        for expert in effective_members:
            data = expert_stats_map.get(expert)
            if data:
                c = data["CompletedCount"]
                x = data["CancelledCount"]
                r = data["RescheduledCount"]
                t = data["TotalInterviews"]
            else:
                c = x = r = t = 0

            team_completed += c
            team_cancelled += x
            team_rescheduled += r
            team_total += t

            member_stats.append({
                'expert': expert,
                'completed': c,
                'cancelled': x,
                'rescheduled': r,
                'total': t
            })

            # Add to flat expert list
            expert_data.append({
                'team': team_name,
                'expert': expert,
                'completed': c,
                'cancelled': x,
                'rescheduled': r,
                'total': t
            })

        team_data.append({
            'team': team_name,
            'member_count': len(members),
            'active_members': len([m for m in member_stats if m['total'] > 0]),
            'completed': team_completed,
            'cancelled': team_cancelled,
            'rescheduled': team_rescheduled,
            'total': team_total,
            'members': sorted(member_stats, key=lambda x: x['total'], reverse=True)
        })

    # Sort teams by total interviews
    team_data.sort(key=lambda x: x['total'], reverse=True)
    expert_data.sort(key=lambda x: x['total'], reverse=True)

    # Calculate overall totals
    overall_completed = sum(t['completed'] for t in team_data)
    overall_cancelled = sum(t['cancelled'] for t in team_data)
    overall_rescheduled = sum(t['rescheduled'] for t in team_data)
    overall_total = sum(t['total'] for t in team_data)

    return render_template(
        'interview_stats.html',
        teams=teams_list,
        experts=all_experts,
        team_data=team_data,
        expert_data=expert_data,
        selected_team=filter_team or '',
        selected_expert=filter_expert or '',
        start_date=start_date,
        end_date=end_date,
        overall_completed=overall_completed,
        overall_cancelled=overall_cancelled,
        overall_rescheduled=overall_rescheduled,
        overall_total=overall_total
    )


@analytics_bp.route('/interview-records')
def interview_records():
    """
    Interview Records page showing detailed interview records with subjects
    per expert/team with date filtering.
    """
    db = get_db()
    start_date, end_date = get_date_filter_strings()

    # Get filter parameters
    filter_team = request.args.get('team', '') or None
    filter_expert = request.args.get('expert', '') or None

    # Get expert-team mapping
    expert_team_map, teams_map = get_expert_team_map(db)
    teams_list = sorted(teams_map.keys())

    # Get all experts for filter dropdown
    all_experts = sorted(db.taskBody.distinct('assignedTo', {
        "assignedTo": {"$type": "string", "$ne": ""},
    }))

    # Build date filter
    date_match = {}
    if start_date or end_date:
        date_filter = {}
        if start_date:
            if 'T' not in start_date:
                start_date_val = f"{start_date}T00:00:00"
            else:
                start_date_val = start_date
            date_filter["$gte"] = start_date_val
        if end_date:
            if 'T' not in end_date:
                end_date_val = f"{end_date}T23:59:59"
            else:
                end_date_val = end_date
            date_filter["$lte"] = end_date_val
        if date_filter:
            date_match["receivedDateTime"] = date_filter

    # Build team data with interview records
    team_data = []
    all_records = []
    overall_total = 0

    for team_name, members in teams_map.items():
        # Filter by team
        if filter_team and team_name != filter_team:
            continue

        # Filter by expert within team
        effective_members = members
        if filter_expert:
            effective_members = [m for m in members if m == filter_expert]
            if not effective_members:
                continue

        # Query for completed interviews (excluding Screening and On Demand)
        query = {
            **date_match,
            "assignedTo": {"$in": effective_members},
            "actualRound": {"$nin": ["Screening", "On demand", "On Demand or AI Interview"]},
            "status": "Completed",
        }

        records = list(db.taskBody.find(query, {
            "assignedTo": 1,
            "subject": 1,
            "receivedDateTime": 1,
            "actualRound": 1,
            "Candidate Name": 1,
        }).sort("receivedDateTime", -1))

        # Group by expert
        expert_records = {}
        for r in records:
            expert = r.get("assignedTo")
            if expert not in expert_records:
                expert_records[expert] = []
            expert_records[expert].append({
                'subject': r.get('subject', 'N/A'),
                'candidate': r.get('Candidate Name', 'N/A'),
                'round': r.get('actualRound', 'N/A'),
                'date': r.get('receivedDateTime', '')[:10] if r.get('receivedDateTime') else 'N/A',
            })

            # Add to all records for export
            all_records.append({
                'team': team_name,
                'expert': expert,
                'subject': r.get('subject', 'N/A'),
                'candidate': r.get('Candidate Name', 'N/A'),
                'round': r.get('actualRound', 'N/A'),
                'date': r.get('receivedDateTime', ''),
            })

        team_total = sum(len(recs) for recs in expert_records.values())
        overall_total += team_total

        # Build expert data for this team
        expert_list = []
        for expert in effective_members:
            recs = expert_records.get(expert, [])
            expert_list.append({
                'expert': expert,
                'count': len(recs),
                'records': recs
            })

        # Sort experts by count
        expert_list.sort(key=lambda x: x['count'], reverse=True)

        team_data.append({
            'team': team_name,
            'total': team_total,
            'member_count': len(members),
            'active_count': len([e for e in expert_list if e['count'] > 0]),
            'experts': expert_list
        })

    # Sort teams by total
    team_data.sort(key=lambda x: x['total'], reverse=True)

    return render_template(
        'interview_records.html',
        teams=teams_list,
        experts=all_experts,
        team_data=team_data,
        all_records=all_records[:500],  # Limit for performance
        selected_team=filter_team or '',
        selected_expert=filter_expert or '',
        start_date=start_date,
        end_date=end_date,
        overall_total=overall_total,
        total_records=len(all_records)
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
