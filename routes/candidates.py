
from flask import Blueprint, render_template, request
from db import get_db
from datetime import datetime, timedelta
from collections import Counter

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

        # Querying candidateDetails collection
        cursor = db.candidateDetails.find(mongo_query).limit(50)
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

    # Get list of all unique candidate names for autocomplete/dropdown
    all_candidates = sorted(db.taskBody.distinct('Candidate Name', {
        "Candidate Name": {"$type": "string", "$ne": ""}
    }))

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


@candidates_bp.route('/active', methods=['GET'])
def active_candidates():
    """
    Active Candidates Dashboard - shows candidates with multiple interviews
    within a specified time period.

    Filters:
    - min_interviews: Minimum number of interviews (default: 2, which means > 1)
    - months: Number of months to look back (default: 3)
    """
    db = get_db()

    # Get filter parameters
    try:
        min_interviews = int(request.args.get('min_interviews', 2))
    except ValueError:
        min_interviews = 2

    try:
        months = int(request.args.get('months', 3))
    except ValueError:
        months = 3

    # Calculate date range
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=months * 30)  # Approximate months

    # Convert to ISO string format for MongoDB query
    start_date_str = start_date.strftime('%Y-%m-%dT%H:%M:%S')
    end_date_str = end_date.strftime('%Y-%m-%dT%H:%M:%S')

    # Build aggregation pipeline to find active candidates
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
            "$limit": 500  # Limit for performance
        }
    ]

    results = list(db.taskBody.aggregate(pipeline))

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

    return render_template(
        'active_candidates.html',
        candidates=active_candidates_list,
        summary=summary,
        min_interviews=min_interviews,
        months=months,
        start_date=start_date.strftime('%Y-%m-%d'),
        end_date=end_date.strftime('%Y-%m-%d')
    )
