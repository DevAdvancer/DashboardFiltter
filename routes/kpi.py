"""
KPI Sidebar Route - Expert First Assignment Analytics

Calculates expert KPIs based on first-assigned expert (from taskBody.replies)
and validates against candidateDetails.Expert field.
"""

import re
from datetime import datetime
from flask import Blueprint, jsonify, request, render_template
from db import get_db

kpi_bp = Blueprint('kpi', __name__)


def extract_first_assigned_expert(replies):
    """
    Extract the first assigned expert email from taskBody.replies array.
    
    The replies array contains assignment history. First expert is determined
    by the earliest entry containing an "assigned to" pattern.
    
    Args:
        replies: List of reply strings from taskBody document
        
    Returns:
        Email string of first assigned expert, or None if not found
    """
    if not replies or not isinstance(replies, list):
        return None
    
    # Pattern to match email addresses in assignment context
    email_pattern = re.compile(r'[\w\.-]+@[\w\.-]+\.\w+', re.IGNORECASE)
    
    for reply in replies:
        if not isinstance(reply, str):
            continue
        
        # Look for assignment indicators
        lower_reply = reply.lower()
        if any(keyword in lower_reply for keyword in ['assigned to', 'assigning to', 'assigned:', 'asigned']):
            match = email_pattern.search(reply)
            if match:
                return match.group(0).lower()
    
    # Fallback: return first email found in any reply
    for reply in replies:
        if isinstance(reply, str):
            match = email_pattern.search(reply)
            if match:
                return match.group(0).lower()
    
    return None


def build_date_filter(start_date, end_date):
    """Build MongoDB date filter from string dates."""
    filters = {}
    
    if start_date:
        filters['$gte'] = start_date
    if end_date:
        filters['$lte'] = end_date
    
    return {'receivedDateTime': filters} if filters else {}


def get_active_experts(db):
    """Get list of active experts from teams database.
    
    Experts are stored in teams collection as members array.
    Returns {email: {email, team}} map.
    """
    try:
        from db import get_teams_db
        teams_db = get_teams_db()
        teams_cursor = teams_db.teams.find({}, {'name': 1, 'members': 1, '_id': 0})
        
        experts = {}
        for team in teams_cursor:
            team_name = team.get('name', 'Unknown')
            members = team.get('members', [])
            for email in members:
                if email and isinstance(email, str):
                    experts[email.lower()] = {'email': email.lower(), 'team': team_name}
        return experts
    except Exception:
        return {}


def get_expert_team_map(db):
    """Get expert email to team name mapping.
    
    Experts are stored in teams collection as members array.
    Returns {email: team_name} map.
    """
    try:
        from db import get_teams_db
        teams_db = get_teams_db()
        teams_cursor = teams_db.teams.find({}, {'name': 1, 'members': 1, '_id': 0})
        
        expert_team_map = {}
        for team in teams_cursor:
            team_name = team.get('name', 'Unknown')
            members = team.get('members', [])
            for email in members:
                if email and isinstance(email, str):
                    expert_team_map[email.lower()] = team_name
        return expert_team_map
    except Exception:
        return {}


@kpi_bp.route('/sidebar')
def kpi_sidebar():
    """Render KPI sidebar page."""
    db = get_db()
    
    # Get filter values
    start_date = request.args.get('start_date', '')
    end_date = request.args.get('end_date', '')
    filter_team = request.args.get('team', '')
    filter_expert = request.args.get('expert', '')
    filter_round = request.args.get('round', '')
    exclude_rounds = request.args.getlist('exclude_rounds')  # Get multiple exclude values
    
    # Get distinct interview titles (actualRound) for filter dropdown
    try:
        rounds = db.taskBody.distinct('actualRound', {'status': 'Completed'})
        # Filter out empty/None values and sort
        round_titles = sorted([r for r in rounds if r and isinstance(r, str)])
    except Exception:
        round_titles = []
    
    # Get teams for filter dropdown
    try:
        from db import get_teams_db
        teams_db = get_teams_db()
        teams = list(teams_db.teams.find({}, {'name': 1, '_id': 0}))
        team_names = sorted([t.get('name') for t in teams if t.get('name')])
    except Exception:
        team_names = []
    
    # Get experts for filter dropdown (grouped by team)
    active_experts = get_active_experts(db)
    expert_emails = sorted(active_experts.keys())
    
    # Build experts by team mapping for JavaScript filtering
    experts_by_team = {}
    for email, info in active_experts.items():
        team = info.get('team', 'Unknown')
        if team not in experts_by_team:
            experts_by_team[team] = []
        experts_by_team[team].append(email)
    
    # Sort experts within each team
    for team in experts_by_team:
        experts_by_team[team] = sorted(experts_by_team[team])
    
    # Fetch KPI data
    kpi_data = calculate_kpi_data(db, start_date, end_date, filter_team, filter_expert, filter_round, exclude_rounds)
    
    return render_template('kpi_sidebar.html',
                         kpi_data=kpi_data,
                         teams=team_names,
                         experts=expert_emails,
                         experts_by_team=experts_by_team,
                         round_titles=round_titles,
                         start_date=start_date,
                         end_date=end_date,
                         selected_team=filter_team,
                         selected_expert=filter_expert,
                         selected_round=filter_round,
                         exclude_rounds=exclude_rounds)


@kpi_bp.route('/api/kpi-sidebar')
def api_kpi_sidebar():
    """API endpoint for KPI sidebar data."""
    db = get_db()
    
    start_date = request.args.get('start_date', '')
    end_date = request.args.get('end_date', '')
    filter_team = request.args.get('team', '')
    filter_expert = request.args.get('expert', '')
    filter_round = request.args.get('round', '')
    exclude_rounds = request.args.getlist('exclude_rounds[]') if 'exclude_rounds[]' in request.args else request.args.getlist('exclude_rounds')
    
    try:
        kpi_data = calculate_kpi_data(db, start_date, end_date, filter_team, filter_expert, filter_round, exclude_rounds)
        return jsonify({
            'success': True,
            'data': kpi_data
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@kpi_bp.route('/api/matched-candidates')
def api_matched_candidates():
    """API endpoint to get matched candidates for a specific expert."""
    db = get_db()
    
    expert_email = request.args.get('expert', '').lower()
    start_date = request.args.get('start_date', '')
    end_date = request.args.get('end_date', '')
    filter_round = request.args.get('round', '')
    exclude_rounds = request.args.getlist('exclude_rounds[]') if 'exclude_rounds[]' in request.args else request.args.getlist('exclude_rounds')
    
    if not expert_email:
        return jsonify({'success': False, 'error': 'Expert email required'}), 400
    
    try:
        # Build query filters
        match_filters = {'status': 'Completed'}
        if filter_round:
            if exclude_rounds and filter_round in exclude_rounds:
                # Conflict: User selected a round but also excluded it -> Show nothing
                match_filters['actualRound'] = "___NON_EXISTENT_ROUND___"
            else:
                match_filters['actualRound'] = filter_round
        elif exclude_rounds:
            match_filters['actualRound'] = {'$nin': exclude_rounds}
        if start_date or end_date:
            date_filter = {}
            if start_date:
                date_filter['$gte'] = start_date
            if end_date:
                date_filter['$lte'] = end_date
            match_filters['receivedDateTime'] = date_filter
        
        # Fetch tasks with this first-assigned expert
        projection = {'Candidate Name': 1, 'actualRound': 1, 'receivedDateTime': 1, 'replies': 1, 'assignedTo': 1, 'subject': 1}
        task_docs = list(db.taskBody.find(match_filters, projection).limit(10000))
        
        # Filter to those where first-assigned expert matches - collect ALL interviews per candidate
        candidates = {}
        for doc in task_docs:
            first_expert = extract_first_assigned_expert(doc.get('replies', []))
            if not first_expert:
                first_expert = (doc.get('assignedTo') or '').lower()
            
            if first_expert == expert_email:
                candidate_name = doc.get('Candidate Name', '')
                subject = doc.get('subject', '') or ''
                # Exclude On Demand/AI Interview and Screening
                subject_lower = subject.lower()
                if 'on demand' in subject_lower or 'ondemand' in subject_lower or 'ai interview' in subject_lower or 'screening' in subject_lower:
                    continue
                if candidate_name:
                    if candidate_name not in candidates:
                        candidates[candidate_name] = {
                            'name': candidate_name,
                            'subjects': []
                        }
                    if subject:
                        candidates[candidate_name]['subjects'].append(subject)
        
        # Get candidateDetails to check which are matched
        candidate_names = list(candidates.keys())
        matched = []
        unmatched = []
        
        if candidate_names:
            candidate_docs = list(db.candidateDetails.find(
                {'Candidate Name': {'$in': candidate_names}},
                {'Candidate Name': 1, 'Expert': 1, '_id': 0}
            ))
            candidate_expert_map = {doc['Candidate Name']: (doc.get('Expert') or '').lower() for doc in candidate_docs}
            
            for name, info in candidates.items():
                if name in candidate_expert_map:
                    if candidate_expert_map[name] == expert_email:
                        matched.append({**info, 'status': 'matched'})
                    else:
                        unmatched.append({**info, 'status': 'unmatched', 'actual_expert': candidate_expert_map[name]})
                else:
                    unmatched.append({**info, 'status': 'not_found'})
        
        return jsonify({
            'success': True,
            'expert': expert_email,
            'matched': matched,
            'unmatched': unmatched,
            'total_matched': len(matched),
            'total_unmatched': len(unmatched)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


def calculate_kpi_data(db, start_date='', end_date='', filter_team=None, filter_expert=None, filter_round=None, exclude_rounds=None):
    """
    Calculate KPI data for experts based on first-assigned attribution.
    
    Args:
        filter_round: Optional interview title (actualRound) filter
        exclude_rounds: List of interview titles to exclude
    
    Returns:
        dict with 'experts' list and 'summary' totals
    """
    # Get expert team mapping for filtering
    expert_team_map = get_expert_team_map(db)
    
    # Build base query for taskBody
    match_filters = {'status': 'Completed'}  # Only count completed interviews
    
    # Apply interview title filter
    if filter_round:
        if exclude_rounds and filter_round in exclude_rounds:
             # Conflict: User selected a round but also excluded it -> Show nothing
            match_filters['actualRound'] = "___NON_EXISTENT_ROUND___"
        else:
            match_filters['actualRound'] = filter_round
    elif exclude_rounds:
        match_filters['actualRound'] = {'$nin': exclude_rounds}
    
    if start_date or end_date:
        date_filter = {}
        if start_date:
            date_filter['$gte'] = start_date
        if end_date:
            date_filter['$lte'] = end_date
        match_filters['receivedDateTime'] = date_filter
    
    # Fetch taskBody documents with replies
    projection = {
        '_id': 1,
        'Candidate Name': 1,
        'assignedTo': 1,
        'replies': 1,
        'receivedDateTime': 1,
        'actualRound': 1
    }
    
    task_docs = list(db.taskBody.find(match_filters, projection).limit(50000))
    
    # Process documents to extract first-assigned expert
    expert_interviews = {}  # {expert_email: [interview_ids]}
    expert_candidates = {}  # {expert_email: set(candidate_names)}
    
    for doc in task_docs:
        first_expert = extract_first_assigned_expert(doc.get('replies', []))
        
        if not first_expert:
            # Fallback to assignedTo if no first assignment found
            first_expert = (doc.get('assignedTo') or '').lower()
        
        if not first_expert:
            continue
        
        # Skip experts not in teams database
        if first_expert not in expert_team_map:
            continue
        
        # Apply team filter
        if filter_team and expert_team_map.get(first_expert) != filter_team:
            continue
        
        # Apply expert filter
        if filter_expert and first_expert != filter_expert.lower():
            continue
        
        # Track interviews
        if first_expert not in expert_interviews:
            expert_interviews[first_expert] = []
            expert_candidates[first_expert] = set()
        
        expert_interviews[first_expert].append({
            'id': str(doc.get('_id')),
            'name': doc.get('Candidate Name', '')
        })
        
        candidate_name = doc.get('Candidate Name', '')
        if candidate_name:
            expert_candidates[first_expert].add(candidate_name)
    
    # Get candidateDetails for match rate calculation
    all_candidates = set()
    for candidates in expert_candidates.values():
        all_candidates.update(candidates)
    
    # Fetch candidateDetails for validation
    candidate_expert_map = {}  # {candidate_name: expert_in_candidateDetails}
    if all_candidates:
        candidate_docs = list(db.candidateDetails.find(
            {'Candidate Name': {'$in': list(all_candidates)}},
            {'Candidate Name': 1, 'Expert': 1, '_id': 0}
        ))
        for doc in candidate_docs:
            name = doc.get('Candidate Name', '')
            expert = (doc.get('Expert') or '').lower()
            if name and expert:
                candidate_expert_map[name] = expert
    
    # Calculate KPIs per expert
    results = []
    total_matches = 0
    total_validatable = 0
    
    for expert_email in sorted(expert_interviews.keys()):
        interview_count = len(expert_interviews[expert_email])
        candidate_names = expert_candidates[expert_email]
        candidate_count = len(candidate_names)
        
        # Calculate match rate
        matches = 0
        validatable = 0
        matched_interviews = 0
        for candidate in candidate_names:
            if candidate in candidate_expert_map:
                validatable += 1
                if candidate_expert_map[candidate] == expert_email:
                    matches += 1
                    # Count interviews for this matched candidate
                    matched_interviews += sum(1 for i in expert_interviews[expert_email] if i['name'] == candidate)
        
        match_rate = (matches / validatable * 100) if validatable > 0 else 0
        
        total_matches += matches
        total_validatable += validatable
        
        results.append({
            'expert': expert_email,
            'team': expert_team_map.get(expert_email, 'Unknown'),
            'total_candidates': candidate_count,
            'total_interviews': interview_count,
            'validated_candidates': validatable,
            'validated_candidates': validatable,
            'matched_candidates': matches,
            'matched_interviews': matched_interviews,
            'match_rate': round(match_rate, 1)
        })
    
    # Sort by interview count descending
    results.sort(key=lambda x: x['total_interviews'], reverse=True)
    
    # Calculate summary
    avg_match_rate = (total_matches / total_validatable * 100) if total_validatable > 0 else 0
    
    summary = {
        'total_experts': len(results),
        'total_candidates': sum(r['total_candidates'] for r in results),
        'total_interviews': sum(r['total_interviews'] for r in results),
        'avg_match_rate': round(avg_match_rate, 1)
    }
    
    return {
        'experts': results,
        'summary': summary
    }
