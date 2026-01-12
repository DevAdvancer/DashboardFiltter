
from flask import Blueprint, render_template, request, current_app
from db import get_db, get_teams_db
from datetime import datetime, timedelta
from collections import Counter

dashboard_bp = Blueprint('dashboard', __name__)

@dashboard_bp.route('/')
def index():
    """Optimized dashboard with caching and combined queries."""
    cache = current_app.cache

    # Cache the entire dashboard data for 5 minutes
    @cache.memoize(timeout=300)
    def get_dashboard_data():
        db = get_db()

        # ==========================================
        # OPTIMIZATION 1: Combine multiple candidate queries into ONE aggregation
        # ==========================================
        candidate_stats_pipeline = [
            {
                "$facet": {
                    "total": [{"$count": "count"}],
                    "by_status": [
                        {"$group": {"_id": "$workflowStatus", "count": {"$sum": 1}}},
                        {"$sort": {"count": -1}}
                    ],
                    "by_tech": [
                        {"$match": {"Technology": {"$ne": None, "$ne": ""}}},
                        {"$group": {"_id": "$Technology", "count": {"$sum": 1}}},
                        {"$sort": {"count": -1}},
                        {"$limit": 10}
                    ],
                    "by_branch": [
                        {"$match": {"Branch": {"$ne": None, "$ne": ""}}},
                        {"$group": {"_id": "$Branch", "count": {"$sum": 1}}},
                        {"$sort": {"count": -1}},
                        {"$limit": 8}
                    ],
                    "recent": [
                        {"$sort": {"updated_at": -1}},
                        {"$limit": 10},
                        {"$project": {
                            "_id": 1,
                            "Candidate Name": 1,
                            "workflowStatus": 1,
                            "updated_at": 1
                        }}
                    ]
                }
            }
        ]

        candidate_stats = list(db.candidateDetails.aggregate(candidate_stats_pipeline))[0]

        total_candidates = candidate_stats["total"][0]["count"] if candidate_stats["total"] else 0
        status_breakdown = {(s.get("_id") or "Unknown"): s["count"] for s in candidate_stats["by_status"]}
        technology_distribution = [{"name": t["_id"], "count": t["count"]} for t in candidate_stats["by_tech"]]
        branch_distribution = [{"name": b["_id"], "count": b["count"]} for b in candidate_stats["by_branch"]]
        recent_candidates = candidate_stats["recent"]

        active_count = status_breakdown.get("active", 0)
        scheduled_count = status_breakdown.get("scheduled", 0)
        rejected_count = status_breakdown.get("rejected", 0)
        completed_count = status_breakdown.get("completed", 0)

        # ==========================================
        # OPTIMIZATION 2: Combine multiple interview queries into ONE aggregation
        # ==========================================
        interview_stats_pipeline = [
            {
                "$facet": {
                    "total": [{"$count": "count"}],
                    "by_status": [
                        {"$group": {"_id": "$status", "count": {"$sum": 1}}}
                    ],
                    "top_experts": [
                        {"$match": {"status": "Completed", "assignedTo": {"$type": "string", "$ne": ""}}},
                        {"$group": {"_id": "$assignedTo", "interview_count": {"$sum": 1}}},
                        {"$sort": {"interview_count": -1}},
                        {"$limit": 10}
                    ],
                    "funnel_rounds": [
                        {"$match": {
                            "status": "Completed",
                            "actualRound": {"$nin": ["On demand", "On Demand or AI Interview", None, ""]}
                        }},
                        {"$project": {"actualRound": 1}}
                    ]
                }
            }
        ]

        interview_stats = list(db.taskBody.aggregate(interview_stats_pipeline))[0]

        total_interviews = interview_stats["total"][0]["count"] if interview_stats["total"] else 0
        interview_status = {s["_id"]: s["count"] for s in interview_stats["by_status"]}
        completed_interviews = interview_status.get("Completed", 0)
        cancelled_interviews = interview_status.get("Cancelled", 0)
        rescheduled_interviews = interview_status.get("Rescheduled", 0)

        top_experts = [{"name": e["_id"], "count": e["interview_count"]} for e in interview_stats["top_experts"]]

        # ==========================================
        # OPTIMIZATION 3: Process funnel data from already loaded data
        # ==========================================
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

        stage_counts = Counter()
        for task in interview_stats["funnel_rounds"]:
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

        # ==========================================
        # OPTIMIZATION 4: Get teams and calculate counts efficiently (single aggregation)
        # ==========================================
        teams_db = get_teams_db()
        teams_cursor = teams_db.teams.find({}, {"name": 1, "members": 1, "_id": 0})
        teams_map = {t["name"]: t["members"] for t in teams_cursor}

        # Build expert -> team mapping
        expert_to_team = {}
        for team_name, members in teams_map.items():
            for member in members:
                expert_to_team[member] = team_name

        # Single aggregation for all team members
        if expert_to_team:
            team_counts_pipeline = [
                {"$match": {"status": "Completed", "assignedTo": {"$in": list(expert_to_team.keys())}}},
                {"$group": {"_id": "$assignedTo", "count": {"$sum": 1}}}
            ]
            expert_counts = list(db.taskBody.aggregate(team_counts_pipeline))

            # Roll up to teams
            team_interview_counts = {team: 0 for team in teams_map.keys()}
            for expert_count in expert_counts:
                expert = expert_count["_id"]
                team = expert_to_team.get(expert)
                if team:
                    team_interview_counts[team] += expert_count["count"]

            top_teams = sorted(
                [{"name": k, "count": v} for k, v in team_interview_counts.items()],
                key=lambda x: x["count"],
                reverse=True
            )[:10]
        else:
            top_teams = []

        # ==========================================
        # OPTIMIZATION 5: Monthly trends - simplified
        # ==========================================
        monthly_data = []
        today = datetime.now()

        for i in range(5, -1, -1):
            month_start = today - timedelta(days=30 * (i + 1))
            month_end = today - timedelta(days=30 * i)

            # Use count with limit for performance
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

        return {
            "total_candidates": total_candidates,
            "total_interviews": total_interviews,
            "completed_interviews": completed_interviews,
            "cancelled_interviews": cancelled_interviews,
            "rescheduled_interviews": rescheduled_interviews,
            "active_count": active_count,
            "scheduled_count": scheduled_count,
            "rejected_count": rejected_count,
            "completed_count": completed_count,
            "status_breakdown": status_breakdown,
            "top_experts": top_experts,
            "top_teams": top_teams,
            "technology_distribution": technology_distribution,
            "branch_distribution": branch_distribution,
            "funnel_data": funnel_data,
            "recent_candidates": recent_candidates,
            "monthly_data": monthly_data,
        }

    data = get_dashboard_data()
    return render_template("index.html", **data)
