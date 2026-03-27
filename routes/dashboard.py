
from flask import Blueprint, render_template, current_app
from db import get_db
from datetime import datetime
from collections import Counter
from services.reference_data import get_teams_reference

dashboard_bp = Blueprint('dashboard', __name__)

@dashboard_bp.route('/')
def index():
    """Optimized dashboard with caching and combined queries."""
    cache = current_app.cache

    # Cache the entire dashboard data for 5 minutes
    @cache.memoize(timeout=300)
    def get_dashboard_data():
        db = get_db()

        # Focused queries are faster than a large $facet on this dataset.
        candidate_status_rows = list(db.candidateDetails.aggregate([
            {"$group": {"_id": "$workflowStatus", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
        ]))
        status_breakdown = {(row.get("_id") or "Unknown"): row["count"] for row in candidate_status_rows}
        total_candidates = sum(row["count"] for row in candidate_status_rows)

        technology_distribution = [
            {"name": row["_id"], "count": row["count"]}
            for row in db.candidateDetails.aggregate([
                {"$match": {"Technology": {"$nin": [None, ""]}}},
                {"$group": {"_id": "$Technology", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 10},
            ])
        ]

        branch_distribution = [
            {"name": row["_id"], "count": row["count"]}
            for row in db.candidateDetails.aggregate([
                {"$match": {"Branch": {"$nin": [None, ""]}}},
                {"$group": {"_id": "$Branch", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 8},
            ])
        ]

        recent_candidates = list(db.candidateDetails.find({}, {
            "_id": 1,
            "Candidate Name": 1,
            "Technology": 1,
            "workflowStatus": 1,
            "updated_at": 1,
        }).sort("updated_at", -1).limit(10))

        active_count = status_breakdown.get("active", 0)
        scheduled_count = status_breakdown.get("scheduled", 0)
        rejected_count = status_breakdown.get("rejected", 0)
        completed_count = status_breakdown.get("completed", 0)

        interview_status_rows = list(db.taskBody.aggregate([
            {"$group": {"_id": "$status", "count": {"$sum": 1}}},
        ]))
        interview_status = {row.get("_id") or "Unknown": row["count"] for row in interview_status_rows}
        total_interviews = sum(row["count"] for row in interview_status_rows)
        completed_interviews = interview_status.get("Completed", 0)
        cancelled_interviews = interview_status.get("Cancelled", 0)
        rescheduled_interviews = interview_status.get("Rescheduled", 0)

        top_experts = [
            {"name": row["_id"], "count": row["interview_count"]}
            for row in db.taskBody.aggregate([
                {"$match": {"status": "Completed", "assignedTo": {"$type": "string", "$ne": ""}}},
                {"$group": {"_id": "$assignedTo", "interview_count": {"$sum": 1}}},
                {"$sort": {"interview_count": -1}},
                {"$limit": 10},
            ])
        ]

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
        for row in db.taskBody.aggregate([
            {
                "$match": {
                    "status": "Completed",
                    "actualRound": {"$nin": ["On demand", "On Demand or AI Interview", None, ""]},
                }
            },
            {"$group": {"_id": "$actualRound", "count": {"$sum": 1}}},
        ]):
            stage = normalize_round(row.get("_id"))
            if stage:
                stage_counts[stage] += row["count"]

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

        teams_reference = get_teams_reference()
        teams_map = teams_reference["teams_map"]

        # Build expert -> team mapping
        expert_to_team = teams_reference["expert_to_team"]

        # Single aggregation for all team members
        if expert_to_team:
            team_counts_pipeline = [
                {"$match": {"status": "Completed", "assignedTo": {"$in": teams_reference["all_experts"]}}},
                {"$group": {"_id": "$assignedTo", "count": {"$sum": 1}}}
            ]
            expert_counts = list(db.taskBody.aggregate(team_counts_pipeline))

            # Roll up to teams
            team_interview_counts = {team: 0 for team in teams_map.keys()}
            for expert_count in expert_counts:
                expert = expert_count["_id"]
                team = expert_to_team.get(str(expert).lower())
                if team:
                    team_interview_counts[team] += expert_count["count"]

            top_teams = sorted(
                [{"name": k, "count": v} for k, v in team_interview_counts.items()],
                key=lambda x: x["count"],
                reverse=True
            )[:10]
        else:
            top_teams = []

        monthly_data = []
        today = datetime.now()
        current_month_start = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        def shift_month(dt, delta):
            month_index = (dt.year * 12 + dt.month - 1) + delta
            year = month_index // 12
            month = month_index % 12 + 1
            return dt.replace(year=year, month=month, day=1)

        month_starts = [shift_month(current_month_start, offset) for offset in range(-5, 1)]
        next_month_start = shift_month(current_month_start, 1)
        first_month_start = month_starts[0]

        candidate_month_counts = {
            row["_id"]: row["count"]
            for row in db.candidateDetails.aggregate([
                {
                    "$match": {
                        "updated_at": {"$gte": first_month_start, "$lt": next_month_start}
                    }
                },
                {
                    "$group": {
                        "_id": {"$dateToString": {"format": "%Y-%m", "date": "$updated_at"}},
                        "count": {"$sum": 1}
                    }
                }
            ])
        }

        interview_month_counts = {
            row["_id"]: row["count"]
            for row in db.taskBody.aggregate([
                {
                    "$match": {
                        "receivedDateTime": {
                            "$gte": first_month_start.isoformat(),
                            "$lt": next_month_start.isoformat()
                        }
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "month": {"$substr": ["$receivedDateTime", 0, 7]}
                    }
                },
                {
                    "$group": {
                        "_id": "$month",
                        "count": {"$sum": 1}
                    }
                }
            ])
        }

        for month_start in month_starts:
            month_key = month_start.strftime("%Y-%m")
            monthly_data.append({
                "month": month_start.strftime("%b %Y"),
                "candidates": candidate_month_counts.get(month_key, 0),
                "interviews": interview_month_counts.get(month_key, 0)
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
