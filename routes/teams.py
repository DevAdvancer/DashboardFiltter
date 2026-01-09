
from flask import Blueprint, render_template, request, redirect, url_for, flash
from db import get_db

teams_bp = Blueprint('teams', __name__)

@teams_bp.route('/', methods=['GET'])
def manage():
    db = get_db()
    teams = list(db.teams.find({}))
    return render_template('teams.html', teams=teams)

@teams_bp.route('/add_team', methods=['POST'])
def add_team():
    db = get_db()
    team_name = request.form.get('team_name')
    if team_name:
        db.teams.insert_one({"name": team_name, "members": []})
    return redirect(url_for('teams.manage'))

@teams_bp.route('/delete_team/<team_name>', methods=['POST'])
def delete_team(team_name):
    db = get_db()
    db.teams.delete_one({"name": team_name})
    return redirect(url_for('teams.manage'))

@teams_bp.route('/add_member', methods=['POST'])
def add_member():
    db = get_db()
    team_name = request.form.get('team_name')
    email = request.form.get('email')

    if team_name and email:
        db.teams.update_one(
            {"name": team_name},
            {"$addToSet": {"members": email}}
        )
    return redirect(url_for('teams.manage'))

@teams_bp.route('/remove_member', methods=['POST'])
def remove_member():
    db = get_db()
    team_name = request.form.get('team_name')
    email = request.form.get('email')

    if team_name and email:
        db.teams.update_one(
            {"name": team_name},
            {"$pull": {"members": email}}
        )
    return redirect(url_for('teams.manage'))
