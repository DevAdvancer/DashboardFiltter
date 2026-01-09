
from flask import Blueprint, render_template, request
from db import get_db

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
