
from flask import Blueprint, render_template, request, redirect, url_for, flash
from db import get_teams_db
from services.team_management import clean_text, normalize_lookup_text

teams_bp = Blueprint('teams', __name__)


def normalize_team_members(members):
    normalized = []
    seen = set()

    for member in members or []:
        email = normalize_lookup_text(member)
        if email and email not in seen:
            normalized.append(email)
            seen.add(email)

    return normalized

@teams_bp.route('/', methods=['GET'])
def manage():
    teams_db = get_teams_db()
    teams = []
    for team in teams_db.teams.find({}):
        team_copy = dict(team)
        team_copy["members"] = normalize_team_members(team.get("members", []))
        teams.append(team_copy)
    return render_template('teams.html', teams=teams)

@teams_bp.route('/add_team', methods=['POST'])
def add_team():
    teams_db = get_teams_db()
    team_name = clean_text(request.form.get('team_name'))
    if team_name:
        teams_db.teams.insert_one({"name": team_name, "members": []})
    return redirect(url_for('teams.manage'))

@teams_bp.route('/delete_team/<team_name>', methods=['POST'])
def delete_team(team_name):
    teams_db = get_teams_db()
    teams_db.teams.delete_one({"name": team_name})
    return redirect(url_for('teams.manage'))

@teams_bp.route('/add_member', methods=['POST'])
def add_member():
    teams_db = get_teams_db()
    team_name = clean_text(request.form.get('team_name'))
    email = normalize_lookup_text(request.form.get('email'))

    if team_name and email:
        team = teams_db.teams.find_one({"name": team_name}, {"members": 1, "_id": 0}) or {}
        members = normalize_team_members(team.get("members", []))
        if email not in members:
            members.append(email)
        teams_db.teams.update_one(
            {"name": team_name},
            {"$set": {"members": members}}
        )
    return redirect(url_for('teams.manage'))

@teams_bp.route('/remove_member', methods=['POST'])
def remove_member():
    teams_db = get_teams_db()
    team_name = clean_text(request.form.get('team_name'))
    email = normalize_lookup_text(request.form.get('email'))

    if team_name and email:
        team = teams_db.teams.find_one({"name": team_name}, {"members": 1, "_id": 0}) or {}
        members = [
            member
            for member in normalize_team_members(team.get("members", []))
            if member != email
        ]
        teams_db.teams.update_one(
            {"name": team_name},
            {"$set": {"members": members}}
        )
    return redirect(url_for('teams.manage'))
