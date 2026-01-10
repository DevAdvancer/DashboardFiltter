import os
from dotenv import load_dotenv
from flask import Flask, render_template
from routes.dashboard import dashboard_bp
from routes.teams import teams_bp
from routes.candidates import candidates_bp
from routes.analytics import analytics_bp

# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)

# Register Blueprints
app.register_blueprint(dashboard_bp)
app.register_blueprint(teams_bp, url_prefix='/teams')
app.register_blueprint(candidates_bp, url_prefix='/candidates')
app.register_blueprint(analytics_bp, url_prefix='/analytics')

# For local development
if __name__ == "__main__":
    debug = os.getenv("FLASK_DEBUG", "True").lower() == "true"
    port = int(os.getenv("FLASK_PORT", 5000))
    app.run(debug=debug, port=port)

# Vercel serverless handler
application = app
