
from flask import Blueprint, render_template, request, current_app, send_file
from db import get_db, get_teams_db
from datetime import datetime, timedelta
from collections import Counter, defaultdict
import pandas as pd
from io import BytesIO

candidates_bp = Blueprint('candidates', __name__)

@candidates_bp.route('/', methods=['GET'])
def search():
    query_name = request.args.get('q', '')
    results = []

    if query_name:
        db = get_db()
        # Case insensitive regex match for Candidate Name
        mongo_query = {
            "Candidate Name": {"$regex": query_name, "$options": 'i'}
        }

        # Querying candidateDetails collection with projection
        cursor = db.candidateDetails.find(
            mongo_query,
            {
                "Candidate Name": 1,
                "workflowStatus": 1,
                "Technology": 1,
                "_id": 1
            }
        ).limit(50)
        results = list(cursor)

    return render_template('search.html', query=query_name, results=results)


@candidates_bp.route('/lookup', methods=['GET'])
def candidate_lookup():
    """
    Candidate interview lookup - shows detailed interview statistics
    for a specific candidate from the taskBody collection.
    """
    db = get_db()
    candidate_name = request.args.get('name', '').strip()

    # Get list of all unique candidate names for autocomplete/dropdown - OPTIMIZED with limit
    all_candidates = sorted(db.taskBody.distinct('Candidate Name', {
        "Candidate Name": {"$type": "string", "$ne": ""}
    }))[:500]  # Limit to 500 for dropdown performance

    candidate_data = None
    interview_records = []

    if candidate_name:
        # Run the aggregation pipeline for the candidate
        pipeline = [
            {
                "$match": {
                    "Candidate Name": candidate_name
                }
            },
            {
                "$facet": {
                    # Total number of interview records for this candidate
                    "totalInterviews": [
                        {"$count": "count"}
                    ],
                    # How many times each round occurred
                    "byRound": [
                        {"$group": {"_id": "$actualRound", "count": {"$sum": 1}}},
                        {"$sort": {"count": -1}}
                    ],
                    # How many times each status occurred
                    "byStatus": [
                        {"$group": {"_id": "$status", "count": {"$sum": 1}}},
                        {"$sort": {"count": -1}}
                    ],
                    # How many times each expert interviewed
                    "byExpert": [
                        {"$group": {"_id": "$assignedTo", "count": {"$sum": 1}}},
                        {"$sort": {"count": -1}}
                    ],
                    # Timeline of interviews (most recent first)
                    "timeline": [
                        {"$sort": {"receivedDateTime": -1}},
                        {"$limit": 50},
                        {"$project": {
                            "subject": 1,
                            "actualRound": 1,
                            "status": 1,
                            "assignedTo": 1,
                            "receivedDateTime": 1,
                            "scheduledDateTime": 1
                        }}
                    ]
                }
            }
        ]

        results = list(db.taskBody.aggregate(pipeline))

        if results:
            data = results[0]
            total = data["totalInterviews"][0]["count"] if data["totalInterviews"] else 0

            # Process round data
            by_round = []
            for r in data["byRound"]:
                round_name = r['_id'] if r['_id'] else 'Unknown'
                by_round.append({'round': round_name, 'count': r['count']})

            # Process status data
            by_status = []
            completed_count = 0
            cancelled_count = 0
            rescheduled_count = 0
            for s in data["byStatus"]:
                status_name = s['_id'] if s['_id'] else 'Unknown'
                by_status.append({'status': status_name, 'count': s['count']})
                if status_name == 'Completed':
                    completed_count = s['count']
                elif status_name == 'Cancelled':
                    cancelled_count = s['count']
                elif status_name == 'Rescheduled':
                    rescheduled_count = s['count']

            # Process expert data
            by_expert = []
            for e in data["byExpert"]:
                expert_name = e['_id'] if e['_id'] else 'Unknown'
                by_expert.append({'expert': expert_name, 'count': e['count']})

            # Process timeline
            interview_records = data["timeline"]

            # Calculate completion rate
            completion_rate = round((completed_count / total) * 100, 1) if total > 0 else 0

            candidate_data = {
                'name': candidate_name,
                'total_interviews': total,
                'completed_count': completed_count,
                'cancelled_count': cancelled_count,
                'rescheduled_count': rescheduled_count,
                'completion_rate': completion_rate,
                'by_round': by_round,
                'by_status': by_status,
                'by_expert': by_expert,
                'unique_rounds': len(by_round),
                'unique_experts': len(by_expert)
            }

    return render_template(
        'candidate_lookup.html',
        all_candidates=all_candidates,
        candidate_name=candidate_name,
        candidate_data=candidate_data,
        interview_records=interview_records
    )


@candidates_bp.route('/expert-activity', methods=['GET'])
def expert_candidate_activity():
    """
    Expert Candidate Activity Dashboard
    Shows which experts have active candidates based on interview counts in a date range.

    Filters:
    - min_interviews: Minimum number of interviews to be considered "active" (default: 1, which means > 0)
    - months: Number of months to look back (default: 3)
    """
    cache = current_app.cache

    # Get filter parameters
    try:
        min_interviews = int(request.args.get('min_interviews', 1))
    except ValueError:
        min_interviews = 1

    try:
        months = int(request.args.get('months', 3))
    except ValueError:
        months = 3

    # Cache the data
    @cache.memoize(timeout=300)
    def get_expert_activity_data(min_interviews, months):
        db = get_db()
        teams_db = get_teams_db()

        # Get teams
        teams_cursor = teams_db.teams.find({}, {"name": 1, "members": 1, "_id": 0})
        teams_map = {t['name']: t.get('members', []) for t in teams_cursor}

        # Create expert to team mapping
        expert_to_team = {}
        for team_name, members in teams_map.items():
            for member in members:
                expert_to_team[str(member).lower()] = team_name

        # Calculate date range
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=months * 30)
        start_date_str = start_date.strftime('%Y-%m-%dT%H:%M:%S')
        end_date_str = end_date.strftime('%Y-%m-%dT%H:%M:%S')

        # OPTIMIZED: Query taskBody first to get interview counts by candidate
        interview_pipeline = [
            {
                "$match": {
                    "Candidate Name": {"$type": "string", "$ne": ""},
                    "receivedDateTime": {"$gte": start_date_str, "$lte": end_date_str}
                }
            },
            {
                "$group": {
                    "_id": "$Candidate Name",
                    "InterviewCount": {"$sum": 1}
                }
            }
        ]

        interview_results = list(db.taskBody.aggregate(interview_pipeline))
        candidate_interview_map = {r['_id']: r['InterviewCount'] for r in interview_results}

        # Get all candidates with their experts
        candidates = list(db.candidateDetails.find(
            {"Candidate Name": {"$type": "string", "$ne": ""}},
            {"Candidate Name": 1, "Expert": 1, "_id": 0}
        ))

        # Build per_candidate and per_expert lists
        per_candidate = []
        expert_stats = defaultdict(lambda: {"ActiveCandidates": 0, "InactiveCandidates": 0, "TotalCandidates": 0})
        active_candidates_by_expert = defaultdict(list)

        for cand in candidates:
            cand_name = cand.get("Candidate Name", "")
            expert = cand.get("Expert", "")
            expert_lower = expert.lower() if expert else ""

            interview_count = candidate_interview_map.get(cand_name, 0)
            is_active = interview_count >= min_interviews

            per_candidate.append({
                "CandidateName": cand_name,
                "Expert": expert,
                "ExpertLower": expert_lower,
                "InterviewCount": interview_count,
                "isActive": is_active
            })

            # Update expert stats
            expert_stats[expert_lower]["TotalCandidates"] += 1
            if is_active:
                expert_stats[expert_lower]["ActiveCandidates"] += 1
                active_candidates_by_expert[expert_lower].append({
                    "CandidateName": cand_name,
                    "InterviewCount": interview_count
                })
            else:
                expert_stats[expert_lower]["InactiveCandidates"] += 1

        # Sort per_candidate
        per_candidate.sort(key=lambda x: (x["InterviewCount"], x["CandidateName"]), reverse=True)

        # Convert expert_stats to list format
        per_expert = [
            {"_id": expert, **stats}
            for expert, stats in expert_stats.items()
        ]
        per_expert.sort(key=lambda x: (x["ActiveCandidates"], x["TotalCandidates"]), reverse=True)

        # Map expert summaries (already created above)
        expert_summary = {(e.get("_id") or "").lower(): e for e in per_expert}

        # Build team data
        team_data = []
        for team_name, members in teams_map.items():
            team_active = team_inactive = team_total = 0
            expert_list = []

            for expert in members:
                key = expert.lower()
                e = expert_summary.get(key, {})
                active_cnt = e.get("ActiveCandidates", 0)
                inactive_cnt = e.get("InactiveCandidates", 0)
                total_cnt = e.get("TotalCandidates", 0)

                team_active += active_cnt
                team_inactive += inactive_cnt
                team_total += total_cnt

                # Get active candidate list
                active_list = active_candidates_by_expert.get(key, [])
                active_list = sorted(
                    active_list,
                    key=lambda x: x.get("InterviewCount", 0),
                    reverse=True
                )

                expert_list.append({
                    'expert': expert,
                    'total': total_cnt,
                    'active': active_cnt,
                    'inactive': inactive_cnt,
                    'active_candidates': active_list
                })

            # Sort experts by active candidates
            expert_list.sort(key=lambda x: x['active'], reverse=True)

            team_data.append({
                'team': team_name,
                'total': team_total,
                'active': team_active,
                'inactive': team_inactive,
                'experts': expert_list
            })

        # Sort teams by active candidates
        team_data.sort(key=lambda x: x['active'], reverse=True)

        # Calculate overall stats
        overall_total = sum(t['total'] for t in team_data)
        overall_active = sum(t['active'] for t in team_data)
        overall_inactive = sum(t['inactive'] for t in team_data)

        summary = {
            'total_candidates': overall_total,
            'active_candidates': overall_active,
            'inactive_candidates': overall_inactive,
            'teams_count': len(team_data),
            'date_range': f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
        }

        return team_data, summary, start_date, end_date

    team_data, summary, start_date, end_date = get_expert_activity_data(min_interviews, months)

    return render_template(
        'expert_activity.html',
        team_data=team_data,
        summary=summary,
        min_interviews=min_interviews,
        months=months,
        start_date=start_date.strftime('%Y-%m-%d'),
        end_date=end_date.strftime('%Y-%m-%d')
    )


@candidates_bp.route('/expert-activity/export', methods=['GET'])
def export_expert_activity():
    """Export expert candidate activity to Excel."""
    db = get_db()
    teams_db = get_teams_db()

    # Get filter parameters
    try:
        min_interviews = int(request.args.get('min_interviews', 1))
    except ValueError:
        min_interviews = 1

    try:
        months = int(request.args.get('months', 3))
    except ValueError:
        months = 3

    # Get teams
    teams_cursor = teams_db.teams.find({}, {"name": 1, "members": 1, "_id": 0})
    teams_map = {t['name']: t.get('members', []) for t in teams_cursor}

    # Create expert to team mapping
    expert_to_team = {}
    for team_name, members in teams_map.items():
        for member in members:
            expert_to_team[str(member).lower()] = team_name

    # Calculate date range
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=months * 30)
    start_date_str = start_date.strftime('%Y-%m-%dT%H:%M:%S')
    end_date_str = end_date.strftime('%Y-%m-%dT%H:%M:%S')

    # OPTIMIZED: Same as the view function
    interview_pipeline = [
        {
            "$match": {
                "Candidate Name": {"$type": "string", "$ne": ""},
                "receivedDateTime": {"$gte": start_date_str, "$lte": end_date_str}
            }
        },
        {
            "$group": {
                "_id": "$Candidate Name",
                "InterviewCount": {"$sum": 1}
            }
        }
    ]

    interview_results = list(db.taskBody.aggregate(interview_pipeline))
    candidate_interview_map = {r['_id']: r['InterviewCount'] for r in interview_results}

    # Get all candidates with their experts
    candidates = list(db.candidateDetails.find(
        {"Candidate Name": {"$type": "string", "$ne": ""}},
        {"Candidate Name": 1, "Expert": 1, "_id": 0}
    ))

    # Build data structures
    expert_stats = defaultdict(lambda: {"ActiveCandidates": 0, "InactiveCandidates": 0, "TotalCandidates": 0})
    active_candidates_by_expert = defaultdict(list)

    for cand in candidates:
        cand_name = cand.get("Candidate Name", "")
        expert = cand.get("Expert", "")
        expert_lower = expert.lower() if expert else ""

        interview_count = candidate_interview_map.get(cand_name, 0)
        is_active = interview_count >= min_interviews

        expert_stats[expert_lower]["TotalCandidates"] += 1
        if is_active:
            expert_stats[expert_lower]["ActiveCandidates"] += 1
            active_candidates_by_expert[expert_lower].append({
                "CandidateName": cand_name,
                "InterviewCount": interview_count
            })
        else:
            expert_stats[expert_lower]["InactiveCandidates"] += 1

    # Map expert summaries
    expert_summary = {expert: stats for expert, stats in expert_stats.items()}

    # Build Excel data
    summary_rows = []
    active_rows = []

    for team_name, members in teams_map.items():
        for expert in members:
            key = expert.lower()
            e = expert_summary.get(key, {})
            active_cnt = e.get("ActiveCandidates", 0)
            inactive_cnt = e.get("InactiveCandidates", 0)
            total_cnt = e.get("TotalCandidates", 0)

            summary_rows.append({
                "Team": team_name,
                "Expert": expert,
                "TotalCandidates": total_cnt,
                "ActiveCandidates": active_cnt,
                "InactiveCandidates": inactive_cnt,
            })

            # Active candidates for this expert
            active_list = active_candidates_by_expert.get(key, [])
            active_list = sorted(
                active_list,
                key=lambda x: x.get("InterviewCount", 0),
                reverse=True
            )

            for c in active_list:
                active_rows.append({
                    "Team": team_name,
                    "Expert": expert,
                    "CandidateName": c.get("CandidateName", ""),
                    "InterviewCount": c.get("InterviewCount", 0),
                })

    # Handle experts without team mapping
    other_experts = [k for k in expert_summary.keys() if k not in expert_to_team]
    for key in other_experts:
        e = expert_summary[key]
        active_cnt = e["ActiveCandidates"]
        inactive_cnt = e["InactiveCandidates"]
        total_cnt = e["TotalCandidates"]

        summary_rows.append({
            "Team": "NO TEAM",
            "Expert": key,
            "TotalCandidates": total_cnt,
            "ActiveCandidates": active_cnt,
            "InactiveCandidates": inactive_cnt,
        })

        active_list = active_candidates_by_expert.get(key, [])
        active_list = sorted(
            active_list,
            key=lambda x: x.get("InterviewCount", 0),
            reverse=True
        )

        for c in active_list:
            active_rows.append({
                "Team": "NO TEAM",
                "Expert": key,
                "CandidateName": c.get("CandidateName", ""),
                "InterviewCount": c.get("InterviewCount", 0),
            })

    # Create DataFrames
    summary_df = pd.DataFrame(summary_rows)
    active_df = pd.DataFrame(active_rows)

    # Create Excel file in memory
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        summary_df.to_excel(writer, sheet_name='Summary', index=False)
        active_df.to_excel(writer, sheet_name='ActiveCandidates', index=False)
    output.seek(0)

    filename = f"expert_candidate_activity_{start_date.strftime('%Y-%m-%d')}_to_{end_date.strftime('%Y-%m-%d')}.xlsx"

    return send_file(
        output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=filename
    )


@candidates_bp.route('/active', methods=['GET'])
def active_candidates():
    """
    Active Candidates Dashboard - shows candidates with multiple interviews
    within a specified time period - OPTIMIZED with caching.

    Filters:
    - min_interviews: Minimum number of interviews (default: 2, which means > 1)
    - months: Number of months to look back (default: 3)
    """
    cache = current_app.cache

    # Get filter parameters
    try:
        min_interviews = int(request.args.get('min_interviews', 2))
    except ValueError:
        min_interviews = 2

    try:
        months = int(request.args.get('months', 3))
    except ValueError:
        months = 3

    # Cache the data with filters as part of the key
    @cache.memoize(timeout=300)  # Cache for 5 minutes
    def get_active_candidates_data(min_interviews, months):
        db = get_db()

        # Calculate date range
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=months * 30)  # Approximate months

        # Convert to ISO string format for MongoDB query
        start_date_str = start_date.strftime('%Y-%m-%dT%H:%M:%S')
        end_date_str = end_date.strftime('%Y-%m-%dT%H:%M:%S')

        # OPTIMIZED: Build aggregation pipeline with early filtering and limit
        pipeline = [
            {
                "$match": {
                    "Candidate Name": {"$type": "string", "$ne": ""},
                    "receivedDateTime": {
                        "$gte": start_date_str,
                        "$lte": end_date_str
                    }
                }
            },
            {
                "$group": {
                    "_id": "$Candidate Name",
                    "totalInterviews": {"$sum": 1},
                    "completedCount": {
                        "$sum": {"$cond": [{"$eq": ["$status", "Completed"]}, 1, 0]}
                    },
                    "cancelledCount": {
                        "$sum": {"$cond": [{"$eq": ["$status", "Cancelled"]}, 1, 0]}
                    },
                    "rescheduledCount": {
                        "$sum": {"$cond": [{"$eq": ["$status", "Rescheduled"]}, 1, 0]}
                    },
                    "rounds": {"$addToSet": "$actualRound"},
                    "experts": {"$addToSet": "$assignedTo"},
                    "lastInterviewDate": {"$max": "$receivedDateTime"},
                    "firstInterviewDate": {"$min": "$receivedDateTime"}
                }
            },
            {
                "$match": {
                    "totalInterviews": {"$gte": min_interviews}
                }
            },
            {
                "$sort": {"totalInterviews": -1, "lastInterviewDate": -1}
            },
            {
                "$limit": 200  # Reduced limit for better performance
            }
        ]

        results = list(db.taskBody.aggregate(pipeline, allowDiskUse=True))

        # Process results
        active_candidates_list = []
        for result in results:
            candidate_name = result['_id']
            total = result['totalInterviews']
            completed = result['completedCount']
            cancelled = result['cancelledCount']
            rescheduled = result['rescheduledCount']

            # Calculate completion rate
            completion_rate = round((completed / total) * 100, 1) if total > 0 else 0

            # Filter out None/empty values from rounds and experts
            rounds = [r for r in result.get('rounds', []) if r]
            experts = [e for e in result.get('experts', []) if e]

            active_candidates_list.append({
                'name': candidate_name,
                'total_interviews': total,
                'completed': completed,
                'cancelled': cancelled,
                'rescheduled': rescheduled,
                'completion_rate': completion_rate,
                'unique_rounds': len(rounds),
                'unique_experts': len(experts),
                'last_interview': result.get('lastInterviewDate', '')[:10] if result.get('lastInterviewDate') else 'N/A',
                'first_interview': result.get('firstInterviewDate', '')[:10] if result.get('firstInterviewDate') else 'N/A',
                'rounds_list': rounds,
                'experts_list': experts
            })

        # Calculate summary statistics
        total_active_candidates = len(active_candidates_list)
        total_interviews = sum(c['total_interviews'] for c in active_candidates_list)
        avg_interviews = round(total_interviews / total_active_candidates, 1) if total_active_candidates > 0 else 0

        summary = {
            'total_candidates': total_active_candidates,
            'total_interviews': total_interviews,
            'avg_interviews': avg_interviews,
            'date_range': f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
        }

        return active_candidates_list, summary, start_date, end_date

    # Get cached data
    active_candidates_list, summary, start_date, end_date = get_active_candidates_data(min_interviews, months)

    return render_template(
        'active_candidates.html',
        candidates=active_candidates_list,
        summary=summary,
        min_interviews=min_interviews,
        months=months,
        start_date=start_date.strftime('%Y-%m-%d'),
        end_date=end_date.strftime('%Y-%m-%d')
    )
