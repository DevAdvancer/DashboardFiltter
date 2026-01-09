
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pymongo import MongoClient
from collections import defaultdict, Counter
from datetime import datetime, time

# -----------------------------------
# 0. PAGE CONFIG & STYLING
# -----------------------------------
st.set_page_config(
    page_title="Interview Insights Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for "eye-catching" look
st.markdown("""
<style>
    .main .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
    .metric-card {
        background-color: #0e1117;
        border: 1px solid #262730;
        padding: 15px;
        border-radius: 10px;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .metric-value {
        font-size: 2rem;
        font-weight: bold;
        color: #4CAF50;
    }
    .metric-label {
        font-size: 1rem;
        color: #b0b0b0;
    }
    div[data-testid="stExpander"] div[role="button"] p {
        font-size: 1.1rem;
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)

# -----------------------------------
# 1. DATA & CONSTANTS
# -----------------------------------

TEAMS = {
    "Team Darshan": [
        "Darshan.Singh@vizvainc.com",
        "vaibhav.kaushik@vizvainc.com",
        "harshit@vizvainc.com",
        "jayshree.rana@vizvainc.com",
    ],
    "Team Anushree": [
        "ajay.krishna@vizvainc.com",
        "shraavana@silverspaceinc.com",
        "anusree.vasudevan@vizvainc.com",
        "Hridhya.KK@silverspaceinc.com",
    ],
    "Team Prateek": [
        "Aakash.sharma@vizvainc.com",
        "varsha.sahu@vizvainc.com",
        "Prateek.Narvariya@silverspaceinc.com",
    ],
    "Team Rujuwal": [
        "rahul.agarwal@vizvainc.com",
        "aditya.sharma@vizvainc.com",
        "amartya.kumar@vizvainc.com",
        "aman.agnihotri@vizvainc.com",
        "Rujuwal.Garg@silverspaceinc.com",
    ],
    "Team Bhavya": [
        "Bhavya.Dutt@vizvainc.com",
        "ravikant.raj@silverspaceinc.com",
        "astha.singh@silverspaceinc.com",
        "Sandhya@silverspaceinc.com",
        "satyam.singh@silverspaceinc.com",
        "Patel.vidhi@silverspaceinc.com",
        "Vaibhav.Kumar@silverspaceinc.com",
        "sonali.das@silverspaceinc.com",
    ],
}

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

PIPELINE_ORDER = ["Screening", "1st", "2nd", "3rd/Technical", "Final"]

def normalize_round(r):
    if not r:
        return None
    key = str(r).strip().lower()
    return ROUND_BUCKETS.get(key)

def pct(num, den):
    if den and den > 0:
        return (num / den) * 100.0
    return 0.0

# Build expert -> team map
expert_to_team = {}
for tname, members in TEAMS.items():
    for m in members:
        expert_to_team[m.lower()] = tname

# -----------------------------------
# 2. MONGODB CONNECTION
# -----------------------------------
@st.cache_resource
def init_connection():
    return MongoClient("mongodb://localhost:27017")

try:
    client = init_connection()
    db = client["interviewSupport"]
    task_coll = db["taskBody"]
except Exception as e:
    st.error(f"Failed to connect to MongoDB: {e}")
    st.stop()

# -----------------------------------
# 3. SIDEBAR FILTERS
# -----------------------------------
st.sidebar.header("🎛️ Filters")

# Date Filter
st.sidebar.subheader("Date Range")
today = datetime.now()
default_start = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0) # First day of current month

# Custom Text Input for Start Date as requested
start_date_str = st.sidebar.text_input("Start Date (ISO Format)", value=default_start.isoformat())

d_end = st.sidebar.date_input("End Date", today)
# Convert end date to string (end of day)
end_date_str = datetime.combine(d_end, time.max).isoformat()

# Team Filter
st.sidebar.subheader("Teams & Experts")
all_teams = list(TEAMS.keys())
selected_teams = st.sidebar.multiselect("Select Teams", all_teams, default=all_teams)

# Filter Experts based on Selected Teams
available_experts = []
for t in selected_teams:
    available_experts.extend(TEAMS[t])

selected_experts = st.sidebar.multiselect(
    "Select Experts",
    available_experts,
    default=available_experts,
    help="Leave empty to select all displayed experts"
)

# Logic to handle "All" if user deselects everything or just explicit selection
if not selected_experts:
    current_filter_experts = available_experts
else:
    current_filter_experts = selected_experts

# -----------------------------------
# 4. DATA FETCHING
# -----------------------------------
@st.cache_data(ttl=60)
def fetch_data(start_str, end_str):
    match_filters = {
        "status": "Completed",
        "assignedTo": {"$type": "string", "$ne": ""},
        "actualRound": {"$nin": ["On demand", "On Demand or AI Interview"]},
        "receivedDateTime": {"$gte": start_str, "$lte": end_str}
    }

    projection = {
        "assignedTo": 1,
        "Candidate Name": 1,
        "actualRound": 1,
        "receivedDateTime": 1,
        "status": 1
    }

    return list(task_coll.find(match_filters, projection))

raw_docs = fetch_data(start_date_str, end_date_str)

# -----------------------------------
# 5. DATA PROCESSING
# -----------------------------------
expert_stage_counts = defaultdict(lambda: Counter())
filtered_docs_count = 0

for doc in raw_docs:
    expert = doc.get("assignedTo")
    if not expert:
        continue

    # Filter by Expert Selection (which implies Team selection)
    if expert not in current_filter_experts:
        continue

    stage = normalize_round(doc.get("actualRound"))
    if not stage:
        continue

    expert_stage_counts[expert][stage] += 1
    filtered_docs_count += 1

# --- Aggregation for Tables ---
rows_expert = []
team_agg = defaultdict(lambda: Counter())

for expert, stages in expert_stage_counts.items():
    team_name = expert_to_team.get(str(expert).lower(), "Unmapped")

    # Update Team Aggregation
    team_agg[team_name].update(stages)

    scr = stages.get("Screening", 0)
    r1  = stages.get("1st", 0)
    r2  = stages.get("2nd", 0)
    r3  = stages.get("3rd/Technical", 0)
    fin = stages.get("Final", 0)

    total = r1 + r2 + r3 + fin # As per original logic

    rows_expert.append({
        "Team": team_name,
        "Expert": expert,
        "Total Interviews": total,
        "Screening": scr,
        "1st": r1,
        "2nd": r2,
        "3rd/Technical": r3,
        "Final": fin,
        "Screening -> 1st (%)": pct(r1, scr),
        "1st -> 2nd (%)": pct(r2, r1),
        "2nd -> 3rd (%)": pct(r3, r2),
        "3rd -> Final (%)": pct(fin, r3),
    })

expert_df = pd.DataFrame(rows_expert)
if not expert_df.empty:
    expert_df = expert_df.sort_values(by="Total Interviews", ascending=False)


# --- Team Table ---
rows_team = []
for team, stages in team_agg.items():
    # Only include selected teams (though expert filter handles most of this)
    if team not in selected_teams:
        continue

    scr = stages.get("Screening", 0)
    r1  = stages.get("1st", 0)
    r2  = stages.get("2nd", 0)
    r3  = stages.get("3rd/Technical", 0)
    fin = stages.get("Final", 0)

    total = r1 + r2 + r3 + fin

    rows_team.append({
        "Team": team,
        "Total Interviews": total,
        "Screening": scr,
        "1st": r1,
        "2nd": r2,
        "3rd/Technical": r3,
        "Final": fin,
        "Screening -> 1st (%)": pct(r1, scr),
        "1st -> 2nd (%)": pct(r2, r1),
        "2nd -> 3rd (%)": pct(r3, r2),
        "3rd -> Final (%)": pct(fin, r3),
    })

team_df = pd.DataFrame(rows_team)
if not team_df.empty:
    team_df = team_df.sort_values(by="Total Interviews", ascending=False)

# -----------------------------------
# 6. DASHBOARD LAYOUT & VISUALS
# -----------------------------------

st.title("🚀 Interview Operations Dashboard")
st.markdown(f"**Data Range:** `{start_date_str[:10]}` to `{end_date_str[:10]}` | **Matches:** `{filtered_docs_count}` records")

# --- Top Level Metrics ---
total_int = sum(item["Total Interviews"] for item in rows_team) if rows_team else 0
# Calculate aggregate conversion for the top metrics
total_scr = sum(item["Screening"] for item in rows_team) if rows_team else 0
total_1st = sum(item["1st"] for item in rows_team) if rows_team else 0
overall_conv = pct(total_1st, total_scr)

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.markdown(f'<div class="metric-card"><div class="metric-value">{total_int}</div><div class="metric-label">Total Interviews</div></div>', unsafe_allow_html=True)
with c2:
    st.markdown(f'<div class="metric-card"><div class="metric-value">{total_scr}</div><div class="metric-label">Screenings</div></div>', unsafe_allow_html=True)
with c3:
    st.markdown(f'<div class="metric-card"><div class="metric-value">{total_1st}</div><div class="metric-label">1st Rounds</div></div>', unsafe_allow_html=True)
with c4:
    st.markdown(f'<div class="metric-card"><div class="metric-value">{overall_conv:.1f}%</div><div class="metric-label">Scr → 1st Conv</div></div>', unsafe_allow_html=True)

st.markdown("---")

# --- Funnel Visualization ---
st.subheader("📉 Overall Pipeline Funnel")

# Aggregate counts for the funnel
funnel_data = {stage: 0 for stage in PIPELINE_ORDER}
for item in rows_team:
    funnel_data["Screening"] += item["Screening"]
    funnel_data["1st"] += item["1st"]
    funnel_data["2nd"] += item["2nd"]
    funnel_data["3rd/Technical"] += item["3rd/Technical"]
    funnel_data["Final"] += item["Final"]

funnel_df = pd.DataFrame({
    "Stage": list(funnel_data.keys()),
    "Count": list(funnel_data.values())
})

fig_funnel = px.funnel(funnel_df, x='Count', y='Stage', color='Stage',
                       title="Interview Stages Funnel",
                       color_discrete_sequence=px.colors.sequential.Plasma_r)
st.plotly_chart(fig_funnel, use_container_width=True)

# --- Team Performance ---
st.subheader("🏆 Team Performance")

col_left, col_right = st.columns(2)

with col_left:
    if not team_df.empty:
        fig_team = px.bar(team_df, x='Team', y='Total Interviews', color='Team',
                          title="Total Interviews by Team", text_auto=True)
        st.plotly_chart(fig_team, use_container_width=True)
    else:
        st.info("No data available for teams.")

with col_right:
    # Stacked bar of stages per team
    if not team_df.empty:
        # Melt for stacked bar
        team_melted = team_df.melt(id_vars=["Team"], value_vars=PIPELINE_ORDER,
                                   var_name="Stage", value_name="Count")
        fig_stack = px.bar(team_melted, x="Team", y="Count", color="Stage",
                           title="Stage Breakdown by Team",
                           category_orders={"Stage": PIPELINE_ORDER})
        st.plotly_chart(fig_stack, use_container_width=True)

# --- Leaderboard ---
st.subheader("🥇 Expert Leaderboard")
if not expert_df.empty:
    top_n = st.slider("Show Top N Experts", 5, 20, 10)
    top_experts = expert_df.head(top_n)

    fig_expert = px.bar(top_experts, x='Total Interviews', y='Expert', orientation='h',
                        color='Team', title=f"Top {top_n} Experts by Volume",
                        text_auto=True)
    fig_expert.update_layout(yaxis={'categoryorder':'total ascending'})
    st.plotly_chart(fig_expert, use_container_width=True)
else:
    st.info("No expert data found.")

# --- Detailed Tables ---
st.markdown("---")
with st.expander("📄 Detailed Data View", expanded=True):
    tab1, tab2 = st.tabs(["Team Stats", "Expert Stats"])

    with tab1:
        st.dataframe(team_df.style.format({
            "Screening -> 1st (%)": "{:.1f}%",
            "1st -> 2nd (%)": "{:.1f}%",
            "2nd -> 3rd (%)": "{:.1f}%",
            "3rd -> Final (%)": "{:.1f}%",
        }), use_container_width=True)

    with tab2:
        st.dataframe(expert_df.style.format({
            "Screening -> 1st (%)": "{:.1f}%",
            "1st -> 2nd (%)": "{:.1f}%",
            "2nd -> 3rd (%)": "{:.1f}%",
            "3rd -> Final (%)": "{:.1f}%",
        }), use_container_width=True)
