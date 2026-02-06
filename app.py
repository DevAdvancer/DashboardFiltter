import os
from dotenv import load_dotenv

# Load environment variables from .env file BEFORE importing anything else
load_dotenv()

from flask import Flask, render_template, jsonify
from flask_caching import Cache
from routes.dashboard import dashboard_bp
from routes.teams import teams_bp
from routes.candidates import candidates_bp
from routes.analytics import analytics_bp
from routes.kpi import kpi_bp

app = Flask(__name__)

# Configure caching for performance optimization
cache = Cache(app, config={
    'CACHE_TYPE': 'SimpleCache',  # In-memory cache for single-worker deployments
    'CACHE_DEFAULT_TIMEOUT': 300,  # 5 minutes default cache timeout
    'CACHE_THRESHOLD': 500  # Maximum number of items in cache
})

# Make cache available to blueprints
app.cache = cache

# Register Blueprints
app.register_blueprint(dashboard_bp)
app.register_blueprint(teams_bp, url_prefix='/teams')
app.register_blueprint(candidates_bp, url_prefix='/candidates')
app.register_blueprint(analytics_bp, url_prefix='/analytics')
app.register_blueprint(kpi_bp, url_prefix='/kpi')

# Health check endpoint
@app.route('/health')
def health_check():
    """Health check endpoint for monitoring."""
    import sys
    try:
        from db import get_db
        db = get_db()
        # Quick ping to verify DB connection
        result = db.command('ping')
        return jsonify({
            "status": "healthy",
            "database": "connected",
            "python_version": sys.version,
            "ping_response": result
        }), 200
    except Exception as e:
        import traceback
        return jsonify({
            "status": "unhealthy",
            "error": str(e),
            "error_type": type(e).__name__,
            "traceback": traceback.format_exc(),
            "python_version": sys.version
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
