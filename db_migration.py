
from db import get_db

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

def migrate_teams():
    db = get_db()
    teams_coll = db["teams"]

    # Check if we already have teams
    if teams_coll.count_documents({}) > 0:
        print("Teams collection already has data. Skipping seed to prevent overwrite.")
        return

    print("Seeding teams from hardcoded list...")
    for team_name, members in TEAMS.items():
        doc = {
            "name": team_name,
            "members": members
        }
        teams_coll.insert_one(doc)
    print("Migration complete. Teams seeded.")

if __name__ == "__main__":
    migrate_teams()
