!pip install pymongo
teams = {
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

from pymongo import MongoClient
from collections import defaultdict
import pandas as pd

# ---- Mongo connection ----
mongo_uri = "mongodb://localhost:27017"
client = MongoClient(mongo_uri)

db = client["interviewSupport"]
coll = db["taskBody"]

# ---- Date range ----
start_date = "2025-12-01T00:00:00"
end_date   = "2025-12-30T23:59:59"

# 🔧 CONTROLS:
filter_team = "Team Bhavya"   # e.g. "Team Darshan" or None for all teams
filter_expert = None # e.g. "aman.agnihotri@vizvainc.com" or None for all experts

# This will store rows for Excel
excel_rows = []

for team_name, members in teams.items():
    # if a specific team is selected, skip others
    if filter_team is not None and team_name != filter_team:
        continue

    # if filtering by one expert, only include them (if they're in this team)
    effective_members = members
    if filter_expert is not None:
        effective_members = [m for m in members if m == filter_expert]
        if not effective_members:
            continue

    query = {
        "receivedDateTime": {"$gte": start_date, "$lte": end_date},
        "assignedTo": {"$in": effective_members},
        "actualRound": {"$nin": ["Screening", "On demand", "On Demand or AI Interview"]},
        "status": "Completed",
    }

    records = list(coll.find(query))

    # group by expert
    per_expert = defaultdict(list)
    for r in records:
        expert = r.get("assignedTo")
        subject = r.get("subject")
        per_expert[expert].append(subject)

        # add row for Excel
        excel_rows.append({
            "Team": team_name,
            "Expert": expert,
            "Subject": subject,
            "ReceivedDateTime": r.get("receivedDateTime"),
        })

    team_total = sum(len(subs) for subs in per_expert.values())

    # ---- Console output ----
    print(f"\n===== {team_name} ({team_total}) =====")
    for expert, subjects in per_expert.items():
        print(f"{expert} ({len(subjects)})")
        for s in subjects:
            print("  -", s)

# # ---- Create Excel file ----
# if excel_rows:
#     df = pd.DataFrame(excel_rows)
#     # Optional: sort a bit
#     df = df.sort_values(by=["Team", "Expert", "ReceivedDateTime"], ascending=[True, True, True])

#     output_file = f"interviews_{start_date[:10]}_to_{end_date[:10]}.xlsx"
#     df.to_excel(output_file, index=False)
#     print(f"\nExcel file created: {output_file}")
# else:
#     print("\nNo records found for the given filters/date range.")

from pymongo import MongoClient
import pandas as pd

# ---- Mongo connection ----
mongo_uri = "mongodb://localhost:27017"
client = MongoClient(mongo_uri)
db = client["interviewSupport"]
coll = db["taskBody"]

# ---- Date range ----
start_date = "2025-12-01T00:00:00"
end_date   = "2025-12-30T23:59:59"

# ---- Aggregate per expert ----
pipeline = [
    {
        "$match": {
            "receivedDateTime": {"$gte": start_date, "$lte": end_date},
            "actualRound": {"$nin": ["Screening", "On Demand or AI Interview"]},
            "assignedTo": {"$type": "string", "$ne": ""}
        }
    },
    {
        "$group": {
            "_id": "$assignedTo",
            "CompletedCount": {
                "$sum": {"$cond": [{"$eq": ["$status", "Completed"]}, 1, 0]}
            },
            "CancelledCount": {
                "$sum": {"$cond": [{"$eq": ["$status", "Cancelled"]}, 1, 0]}
            },
            "RescheduledCount": {
                "$sum": {"$cond": [{"$eq": ["$status", "Rescheduled"]}, 1, 0]}
            },
            "TotalInterviews": {"$sum": 1}
        }
    },
    {
        "$project": {
            "_id": 0,
            "Expert": "$_id",
            "CompletedCount": 1,
            "CancelledCount": 1,
            "RescheduledCount": 1,
            "TotalInterviews": 1
        }
    }
]

results = list(coll.aggregate(pipeline))

# map: expert email -> their stats
expert_stats = {r["Expert"]: r for r in results}

# ---- Teams definition ----

# 🔧 CONTROLS:
filter_team = None  # e.g. "Team Darshan" or None for all teams
filter_expert = None  # e.g. "aman.agnihotri@vizvainc.com" or None for all experts
# Example:
# filter_team = "Team Darshan"
# filter_expert = "vaibhav.kaushik@vizvainc.com"

# ---- Collect rows for Excel ----
excel_rows = []

# ---- Print team + member stats ----
for team_name, members in teams.items():
    # if a specific team is selected, skip others
    if filter_team is not None and team_name != filter_team:
        continue

    # start from original members
    effective_members = members

    # if filtering by one expert, only include them (if they're in this team)
    if filter_expert is not None:
        effective_members = [m for m in members if m == filter_expert]
        if not effective_members:
            # this team doesn't contain that expert, skip it
            continue

    # compute team totals only over effective_members
    team_completed = team_cancelled = team_rescheduled = team_total = 0

    for expert in effective_members:
        data = expert_stats.get(expert)
        if not data:
            continue
        team_completed   += data["CompletedCount"]
        team_cancelled   += data["CancelledCount"]
        team_rescheduled += data["RescheduledCount"]
        team_total       += data["TotalInterviews"]

    print(f"\n===== {team_name} ({team_total}) =====")

    # print each expert in effective_members
    for expert in effective_members:
        data = expert_stats.get(expert)
        if not data:
            c = x = r = t = 0
            print(f"{expert} (0) -> Completed: 0, Cancelled: 0, Rescheduled: 0, Total: 0")
        else:
            c = data["CompletedCount"]
            x = data["CancelledCount"]
            r = data["RescheduledCount"]
            t = data["TotalInterviews"]
            print(f"{expert} ({t}) -> Completed: {c}, Cancelled: {x}, Rescheduled: {r}, Total: {t}")

        # add row for Excel (even if 0s)
        excel_rows.append({
            "Team": team_name,
            "Expert": expert,
            "CompletedCount": c,
            "CancelledCount": x,
            "RescheduledCount": r,
            "TotalInterviews": t,
        })

    print(
        f"== Team Total -> Completed: {team_completed}, "
        f"Cancelled: {team_cancelled}, Rescheduled: {team_rescheduled}, "
        f"Total: {team_total}"
    )

# # ---- Write Excel ----
# if excel_rows:
#     df = pd.DataFrame(excel_rows)
#     df = df.sort_values(by=["Team", "Expert"])
#     output_file = f"team_summary_{start_date[:10]}_to_{end_date[:10]}.xlsx"
#     df.to_excel(output_file, index=False)
#     print(f"\nExcel file created: {output_file}")
# else:
#     print("\nNo data to export for the given filters/date range.")

from pymongo import MongoClient
from pprint import pprint

mongo_uri = "mongodb://localhost:27017"
client = MongoClient(mongo_uri)

db = client["interviewSupport"]
coll = db["taskBody"]

candidate_name = "Siddartha Rao Sandineni"   # <-- change if needed

pipeline = [
    {
        "$match": {
            # if your field is exactly "Candidate Name"
            "Candidate Name": candidate_name
            # if you suspect minor differences in spacing/case, you can use:
            # "Candidate Name": {"$regex": f"^{candidate_name}$", "$options": "i"}
        }
    },
    {
        "$facet": {
            # total number of interview records for this candidate
            "totalInterviews": [
                {"$count": "count"}
            ],
            # how many times each round occurred (1st Round, 2nd Round, Loop Round, etc.)
            "byRound": [
                {"$group": {"_id": "$actualRound", "count": {"$sum": 1}}},
                {"$sort": {"_id": 1}}
            ],
            # how many times each status occurred (Completed, Cancelled, Rescheduled, etc.)
            "byStatus": [
                {"$group": {"_id": "$status", "count": {"$sum": 1}}},
                {"$sort": {"_id": 1}}
            ]
        }
    }
]

results = list(coll.aggregate(pipeline))
if not results:
    print("No records found for that candidate.")
else:
    data = results[0]

    total = data["totalInterviews"][0]["count"] if data["totalInterviews"] else 0
    print(f"\nCandidate: {candidate_name}")
    print(f"Total interview records: {total}\n")

    print("By Round:")
    for r in data["byRound"]:
        print(f"  {r['_id']}: {r['count']}")

    print("\nBy Status:")
    for s in data["byStatus"]:
        print(f"  {s['_id']}: {s['count']}")

    # If you want to see the raw aggregated structure:
    # pprint(data)

from pymongo import MongoClient
from collections import defaultdict, Counter
import pandas as pd

# -----------------------------------
# 1. CONFIG
# -----------------------------------
mongo_uri = "mongodb://localhost:27017"
client = MongoClient(mongo_uri)

db = client["interviewSupport"]
task_coll = db["taskBody"]

# Turn this ON if you want to restrict by date, OFF to use *all* data.
USE_DATE_FILTER = True

# When date filter is ON, use this range (change as needed)
# start_date = "2025-01-01T00:00:00"
# end_date   = "2025-12-31T23:59:59"
# For Dec-2025 only:
start_date = "2025-12-01T00:00:00"
end_date   = "2025-12-31T23:59:59"

# 🔧 FILTERS:
filter_team = None  # e.g. "Team Darshan" or None for all teams
filter_expert = None  # e.g. "aman.agnihotri@vizvainc.com" or None for all experts
# examples:
# filter_team = "Team Darshan"
# filter_expert = "vaibhav.kaushik@vizvainc.com"

# Make sure `teams` exists somewhere above this block in your real script, e.g.:
# expert -> team map (case-insensitive)
expert_to_team = {}
for tname, members in teams.items():
    for m in members:
        expert_to_team[m.lower()] = tname

# Map actualRound strings into funnel stages
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


# -----------------------------------
# 2. BUILD QUERY & LOAD DOCS
# -----------------------------------
match_filters = {
    "status": "Completed",
    "assignedTo": {"$type": "string", "$ne": ""},
    "actualRound": {"$nin": ["On demand", "On Demand or AI Interview"]},
}

if USE_DATE_FILTER:
    match_filters["receivedDateTime"] = {"$gte": start_date, "$lte": end_date}

projection = {
    "assignedTo": 1,
    "Candidate Name": 1,
    "actualRound": 1,
    "receivedDateTime": 1,
}

docs = list(task_coll.find(match_filters, projection))
print(f"Matched {len(docs)} completed interviews.")

# expert -> stage -> count
expert_stage_counts = defaultdict(lambda: Counter())

for doc in docs:
    expert = doc.get("assignedTo")
    if not expert:
        continue

    stage = normalize_round(doc.get("actualRound"))
    if not stage:
        continue

    expert_stage_counts[expert][stage] += 1


# -----------------------------------
# 3. EXPERT FUNNEL TABLE (APPLY FILTERS)
# -----------------------------------
rows_expert = []

for expert, stages in expert_stage_counts.items():
    team_name = expert_to_team.get(str(expert).lower(), "Unmapped")

    # apply filters:
    if filter_team is not None and team_name != filter_team:
        continue
    if filter_expert is not None and expert != filter_expert:
        continue

    scr = stages.get("Screening", 0)
    r1  = stages.get("1st", 0)
    r2  = stages.get("2nd", 0)
    r3  = stages.get("3rd/Technical", 0)
    fin = stages.get("Final", 0)

    # total interviews used on the dashboard (1st+2nd+3rd+Final)
    total_interviews = r1 + r2 + r3 + fin

    # step conversion percentages
    scr_to_1st      = pct(r1, scr)
    first_to_second = pct(r2, r1)
    second_to_third = pct(r3, r2)
    third_to_final  = pct(fin, r3)

    rows_expert.append({
        "Team": team_name,
        "Expert": expert,
        "Interview_Counts": total_interviews,
        "Screening": scr,
        "1st": r1,
        "2nd": r2,
        "3rd/Technical": r3,
        "Final": fin,
        "ScreeningTO1st": scr_to_1st,
        "1stTO2nd": first_to_second,
        "2ndTO3rd/Tech": second_to_third,
        "3rd/TechTOFinal": third_to_final,
    })

expert_df = pd.DataFrame(rows_expert)

if not expert_df.empty:
    # Sort by Screening->1st conversion, then interview volume
    expert_df.sort_values(
        by=["ScreeningTO1st", "Interview_Counts"],
        ascending=[False, False],
        inplace=True,
    )
    expert_df.reset_index(drop=True, inplace=True)
    expert_df.insert(0, "Rank", range(1, len(expert_df) + 1))
else:
    expert_df = pd.DataFrame(columns=[
        "Rank", "Team", "Expert", "Interview_Counts",
        "Screening", "1st", "2nd", "3rd/Technical", "Final",
        "ScreeningTO1st", "1stTO2nd", "2ndTO3rd/Tech", "3rd/TechTOFinal",
    ])
    print("No expert funnel data (try changing date range / filters / USE_DATE_FILTER).")


# -----------------------------------
# 4. TEAM FUNNEL TABLE (APPLY FILTERS)
# -----------------------------------
team_rows = []

for team_name, members in teams.items():
    # filter by team
    if filter_team is not None and team_name != filter_team:
        continue

    # start from all members
    effective_members = members

    # if filtering by a specific expert, only aggregate that expert in its team
    if filter_expert is not None:
        effective_members = [m for m in members if m == filter_expert]
        if not effective_members:
            # this team doesn't contain that expert
            continue

    agg_stage = Counter()
    for m in effective_members:
        stages = expert_stage_counts.get(m, Counter())
        agg_stage.update(stages)

    scr = agg_stage.get("Screening", 0)
    r1  = agg_stage.get("1st", 0)
    r2  = agg_stage.get("2nd", 0)
    r3  = agg_stage.get("3rd/Technical", 0)
    fin = agg_stage.get("Final", 0)

    total_interviews = r1 + r2 + r3 + fin

    scr_to_1st      = pct(r1, scr)
    first_to_second = pct(r2, r1)
    second_to_third = pct(r3, r2)
    third_to_final  = pct(fin, r3)

    team_rows.append({
        "Team": team_name,
        "Interview_Counts": total_interviews,
        "Screening": scr,
        "1st": r1,
        "2nd": r2,
        "3rd/Technical": r3,
        "Final": fin,
        "ScreeningTO1st": scr_to_1st,
        "1stTO2nd": first_to_second,
        "2ndTO3rd/Tech": second_to_third,
        "3rd/TechTOFinal": third_to_final,
    })

team_df = pd.DataFrame(team_rows)
if not team_df.empty:
    team_df.sort_values(
        by=["ScreeningTO1st", "Interview_Counts"],
        ascending=[False, False],
        inplace=True,
    )
    team_df.reset_index(drop=True, inplace=True)


# -----------------------------------
# 5. EXPORT TO EXCEL & PRINT SUMMARY
# -----------------------------------
output_file = "expert_team_conversion_funnel_all_experts_filtered.xlsx"

with pd.ExcelWriter(output_file) as writer:  # openpyxl engine
    expert_df.to_excel(writer, sheet_name="Expert_Funnel", index=False)
    team_df.to_excel(writer, sheet_name="Team_Funnel", index=False)

print("\n=== EXPERTS (AFTER FILTER) ===")
print(expert_df.to_string(index=False))

print("\n=== TEAM FUNNEL (AFTER FILTER) ===")
print(team_df.to_string(index=False))

# print("\nExcel saved as:", output_file)
