
from flask import Blueprint, render_template, request
from db import get_db
from datetime import datetime
from collections import Counter

dashboard_bp = Blueprint('dashboard', __name__)

@dashboard_bp.route('/')
def index():
    db = get_db()

    # ==========================================
    # COMPREHENSIVE DASHBOARD OVERVIEW (NO FILTERS)
    # ==========================================

    # 1. Overall Candidate Stats
    total_candidates = db.candidateDetails.count_documents({})

    # Status breakdown
    status_pipeline = [
        {"$group": {"_id": "$workflowStatus", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    status_data = list(db.candidateDetails.aggregate(status_pipeline))
    status_breakdown = {(s.get("_id") or "Unknown"): s["count"] for s in status_data}

    active_count = status_breakdown.get("active", 0)
    scheduled_count = status_breakdown.get("scheduled", 0)
    rejected_count = status_breakdown.get("rejected", 0)
    completed_count = status_breakdown.get("completed", 0)

    # 2. Interview Statistics from taskBody
    total_interviews = db.taskBody.count_documents({})
    completed_interviews = db.taskBody.count_documents({"status": "Completed"})
    cancelled_interviews = db.taskBody.count_documents({"status": "Cancelled"})
    rescheduled_interviews = db.taskBody.count_documents({"status": "Rescheduled"})

    # 3. Top Experts by Interview Count
    expert_pipeline = [
        {"$match": {"status": "Completed", "assignedTo": {"$type": "string", "$ne": ""}}},
        {"$group": {"_id": "$assignedTo", "interview_count": {"$sum": 1}}},
        {"$sort": {"interview_count": -1}},
        {"$limit": 10}
    ]
    top_experts_data = list(db.taskBody.aggregate(expert_pipeline))
    top_experts = [
        {"name": e["_id"], "count": e["interview_count"]}
        for e in top_experts_data
    ]

    # 4. Top Teams by Interview Count
    teams_cursor = db.teams.find({})
    teams_map = {t["name"]: t["members"] for t in teams_cursor}

    team_interview_counts = {}
    for team_name, members in teams_map.items():
        team_count = db.taskBody.count_documents({
            "status": "Completed",
            "assignedTo": {"$in": members}
        })
        team_interview_counts[team_name] = team_count

    top_teams = sorted(
        [{"name": k, "count": v} for k, v in team_interview_counts.items()],
        key=lambda x: x["count"],
        reverse=True
    )[:10]

    # 5. Technology Distribution
    tech_pipeline = [
        {"$match": {"Technology": {"$ne": None, "$ne": ""}}},
        {"$group": {"_id": "$Technology", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    tech_data = list(db.candidateDetails.aggregate(tech_pipeline))
    technology_distribution = [
        {"name": t["_id"], "count": t["count"]}
        for t in tech_data
    ]

    # 6. Funnel Overview (from taskBody)
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

    def normalize_round(r):
        if not r:
            return None
        key = str(r).strip().lower()
        return ROUND_BUCKETS.get(key)

    # Get all completed interviews
    completed_tasks = list(db.taskBody.find({
        "status": "Completed",
        "actualRound": {"$nin": ["On demand", "On Demand or AI Interview", None, ""]}
    }, {"actualRound": 1}))

    stage_counts = Counter()
    for task in completed_tasks:
        stage = normalize_round(task.get("actualRound"))
        if stage:
            stage_counts[stage] += 1

    funnel_stages = {
        "Screening": stage_counts.get("Screening", 0),
        "1st": stage_counts.get("1st", 0),
        "2nd": stage_counts.get("2nd", 0),
        "3rd/Technical": stage_counts.get("3rd/Technical", 0),
        "Final": stage_counts.get("Final", 0)
    }

    # Calculate conversion rates
    def calc_conversion(current, previous):
        return round((current / previous * 100), 1) if previous > 0 else 0

    funnel_data = {
        "stages": funnel_stages,
        "conversions": {
            "screening_to_1st": calc_conversion(funnel_stages["1st"], funnel_stages["Screening"]),
            "1st_to_2nd": calc_conversion(funnel_stages["2nd"], funnel_stages["1st"]),
            "2nd_to_3rd": calc_conversion(funnel_stages["3rd/Technical"], funnel_stages["2nd"]),
            "3rd_to_final": calc_conversion(funnel_stages["Final"], funnel_stages["3rd/Technical"])
        }
    }

    # 7. Recent Activity (Last 10 candidates)
    recent_candidates = list(
        db.candidateDetails.find({})
        .sort("updated_at", -1)
        .limit(10)
    )

    # 8. Branch & Brand Distribution
    branch_pipeline = [
        {"$match": {"Branch": {"$ne": None, "$ne": ""}}},
        {"$group": {"_id": "$Branch", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 8}
    ]
    branch_data = list(db.candidateDetails.aggregate(branch_pipeline))
    branch_distribution = [{"name": b["_id"], "count": b["count"]} for b in branch_data]

    # 9. Monthly Trend (simplified - last 6 months simulation)
    # For demo purposes, we'll create a simple trend
    monthly_data = []
    from datetime import timedelta
    today = datetime.now()

    for i in range(5, -1, -1):
        month_start = today - timedelta(days=30 * (i + 1))
        month_end = today - timedelta(days=30 * i)

        month_count = db.candidateDetails.count_documents({
            "updated_at": {"$gte": month_start, "$lt": month_end}
        })

        month_interviews = db.taskBody.count_documents({
            "receivedDateTime": {
                "$gte": month_start.isoformat(),
                "$lt": month_end.isoformat()
            }
        })

        monthly_data.append({
            "month": month_start.strftime("%b %Y"),
            "candidates": month_count,
            "interviews": month_interviews
        })

    return render_template(
        "index.html",
        # Overall Stats
        total_candidates=total_candidates,
        total_interviews=total_interviews,
        completed_interviews=completed_interviews,
        cancelled_interviews=cancelled_interviews,
        rescheduled_interviews=rescheduled_interviews,
        active_count=active_count,
        scheduled_count=scheduled_count,
        rejected_count=rejected_count,
        completed_count=completed_count,
        # Status Breakdown
        status_breakdown=status_breakdown,
        # Top Performers
        top_experts=top_experts,
        top_teams=top_teams,
        # Distributions
        technology_distribution=technology_distribution,
        branch_distribution=branch_distribution,
        # Funnel
        funnel_data=funnel_data,
        # Recent Activity
        recent_candidates=recent_candidates,
        # Trends
        monthly_data=monthly_data,
    )
