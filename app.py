from flask import Flask, render_template
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

if __name__ == "__main__":
    app.run(debug=True, port=5000)
