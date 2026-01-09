
from flask import Blueprint, render_template, request
from db import get_db
from datetime import datetime

dashboard_bp = Blueprint('dashboard', __name__)

@dashboard_bp.route('/')
def index():
    db = get_db()

    # Distinct lists for filters
    technologies = sorted([t for t in db.candidateDetails.distinct("Technology") if t])
    recruiters = sorted([r for r in db.candidateDetails.distinct("Recruiter") if r])
    workflow_statuses = sorted([s for s in db.candidateDetails.distinct("workflowStatus") if s])
    branches = sorted([b for b in db.candidateDetails.distinct("Branch") if b])
    brands = sorted([b for b in db.candidateDetails.distinct("Brand") if b])
    resume_statuses = sorted([r for r in db.candidateDetails.distinct("resumeUnderstandingStatus") if r])

    # --- 2. Filter Parameters ---
    start_date = request.args.get('start_date', '')
    end_date = request.args.get('end_date', '')

    # Multi-selects
    selected_teams = request.args.getlist("team")
    selected_tech = request.args.getlist("technology")
    selected_recruiter = request.args.getlist("recruiter")
    selected_status = request.args.getlist("status")
    selected_experts = request.args.getlist("expert")
    selected_branches = request.args.getlist("branch")
    selected_brands = request.args.getlist("brand")
    selected_resume_statuses = request.args.getlist("resume_status")

    # Team Logic to filter Experts
    teams_cursor = db.teams.find({})
    teams_map = {t["name"]: t["members"] for t in teams_cursor}
    all_teams_list = list(teams_map.keys())

    valid_experts_from_teams = set()
    if selected_teams:
        for team_name in selected_teams:
            if team_name in teams_map:
                valid_experts_from_teams.update([member for member in teams_map[team_name]])

    # Experts are either scoped by team or pulled from the collection
    if selected_teams and valid_experts_from_teams:
        experts = sorted(valid_experts_from_teams)
    else:
        experts = sorted([e for e in db.candidateDetails.distinct("Expert") if e])

    # --- 3. Build Query ---
    query = {}

    if start_date or end_date:
        date_query = {}
        if start_date:
            try:
                date_query["$gte"] = datetime.fromisoformat(start_date)
            except ValueError:
                pass
        if end_date:
            try:
                date_query["$lte"] = datetime.fromisoformat(end_date)
            except ValueError:
                pass

        if date_query:
            query["updated_at"] = date_query

    # Field Filters
    if selected_tech:
        query["Technology"] = {"$in": selected_tech}

    if selected_recruiter:
        query["Recruiter"] = {"$in": selected_recruiter}

    if selected_status:
        query["workflowStatus"] = {"$in": selected_status}

    if selected_branches:
        query["Branch"] = {"$in": selected_branches}

    if selected_brands:
        query["Brand"] = {"$in": selected_brands}

    if selected_resume_statuses:
        query["resumeUnderstandingStatus"] = {"$in": selected_resume_statuses}

    if selected_teams:
        query["Expert"] = {"$in": list(valid_experts_from_teams)}

    if selected_experts:
        query["Expert"] = {"$in": selected_experts}

    # --- 4. Fetch Results (Grid) ---
    # Pagination
    page = int(request.args.get("page", 1))
    per_page = 50
    skip = (page - 1) * per_page

    total_count = db.candidateDetails.count_documents(query)
    cursor = (
        db.candidateDetails.find(query)
        .sort("updated_at", -1)
        .skip(skip)
        .limit(per_page)
    )
    candidates = list(cursor)

    # Simple aggregates based on filters
    active_count = db.candidateDetails.count_documents(
        {**query, "workflowStatus": "active"}
    )
    scheduled_count = db.candidateDetails.count_documents(
        {**query, "workflowStatus": "scheduled"}
    )
    rejected_count = db.candidateDetails.count_documents(
        {**query, "workflowStatus": "rejected"}
    )

    return render_template(
        "index.html",
        candidates=candidates,
        total_count=total_count,
        page=page,
        per_page=per_page,
        technologies=technologies,
        recruiters=recruiters,
        workflow_statuses=workflow_statuses,
        all_teams=all_teams_list,
        teams_map=teams_map,
        experts=experts,
        branches=branches,
        brands=brands,
        resume_statuses=resume_statuses,
        selected_teams=selected_teams,
        selected_tech=selected_tech,
        selected_recruiter=selected_recruiter,
        selected_status=selected_status,
        selected_experts=selected_experts,
        selected_branches=selected_branches,
        selected_brands=selected_brands,
        selected_resume_statuses=selected_resume_statuses,
        start_date=start_date,
        end_date=end_date,
        active_count=active_count,
        scheduled_count=scheduled_count,
        rejected_count=rejected_count,
    )
