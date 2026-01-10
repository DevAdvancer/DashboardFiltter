import os
from dotenv import load_dotenv

# Load environment variables from .env file BEFORE importing anything else
load_dotenv()

from flask import Flask, render_template, jsonify
from routes.dashboard import dashboard_bp
from routes.teams import teams_bp
from routes.candidates import candidates_bp
from routes.analytics import analytics_bp

app = Flask(__name__)

# Register Blueprints
app.register_blueprint(dashboard_bp)
app.register_blueprint(teams_bp, url_prefix='/teams')
app.register_blueprint(candidates_bp, url_prefix='/candidates')
app.register_blueprint(analytics_bp, url_prefix='/analytics')

# Health check endpoint
@app.route('/health')
def health_check():
    """Health check endpoint for monitoring."""
    try:
        from db import get_db
        db = get_db()
        # Quick ping to verify DB connection
        db.command('ping')
        return jsonify({
            "status": "healthy",
            "database": "connected"
        }), 200
    except Exception as e:
        return jsonify({
            "status": "unhealthy",
            "error": str(e)
        }), 500

# Error handlers
@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors gracefully."""
    return render_template('error.html',
                         error_code=500,
                         error_message="Internal server error. Please contact support."), 500

@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors."""
    return render_template('error.html',
                         error_code=404,
                         error_message="Page not found."), 404

# For local development
if __name__ == "__main__":
    debug = os.getenv("FLASK_DEBUG", "True").lower() == "true"
    port = int(os.getenv("FLASK_PORT", 5000))
    app.run(debug=debug, port=port)

# Vercel serverless handler
application = app
