#!/usr/bin/env python3
"""
Setup Verification Script
Run this script to verify your environment is properly configured.
"""

import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def print_status(check_name, status, message=""):
    """Print colored status message."""
    if status:
        print(f"✅ {check_name}: PASS {message}")
    else:
        print(f"❌ {check_name}: FAIL {message}")
    return status

def check_environment_variables():
    """Check if required environment variables are set."""
    print("\n" + "="*60)
    print("1. CHECKING ENVIRONMENT VARIABLES")
    print("="*60)

    all_pass = True

    # Required variables
    mongo_uri = os.getenv("MONGO_URI")
    mongo_db = os.getenv("MONGO_DB")

    all_pass &= print_status(
        "MONGO_URI",
        bool(mongo_uri),
        f"({mongo_uri[:30]}...)" if mongo_uri else "(NOT SET)"
    )

    all_pass &= print_status(
        "MONGO_DB",
        bool(mongo_db),
        f"({mongo_db})" if mongo_db else "(NOT SET)"
    )

    # Optional variables
    teams_uri = os.getenv("TEAMS_MONGO_URI")
    teams_db = os.getenv("TEAMS_MONGO_DB")

    print(f"\nℹ️  TEAMS_MONGO_URI: {'SET' if teams_uri else 'NOT SET (will use MONGO_URI)'}")
    print(f"ℹ️  TEAMS_MONGO_DB: {'SET' if teams_db else 'NOT SET (will use MONGO_DB)'}")

    return all_pass

def check_dependencies():
    """Check if required packages are installed."""
    print("\n" + "="*60)
    print("2. CHECKING DEPENDENCIES")
    print("="*60)

    required_packages = [
        'flask',
        'pymongo',
        'dotenv',
        'pandas',
        'openpyxl'
    ]

    all_pass = True
    for package in required_packages:
        try:
            __import__(package)
            print_status(package, True)
        except ImportError:
            print_status(package, False, "(Run: pip install -r requirements.txt)")
            all_pass = False

    return all_pass

def check_database_connection():
    """Check if database connection works."""
    print("\n" + "="*60)
    print("3. CHECKING DATABASE CONNECTION")
    print("="*60)

    try:
        from pymongo import MongoClient

        mongo_uri = os.getenv("MONGO_URI")
        mongo_db = os.getenv("MONGO_DB")

        if not mongo_uri or not mongo_db:
            print_status("Database Connection", False, "(Environment variables not set)")
            return False

        # Try to connect
        print("⏳ Connecting to MongoDB...")
        client = MongoClient(
            mongo_uri,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=10000
        )

        # Ping the database
        client.admin.command('ping')
        print_status("MongoDB Connection", True, f"(Connected to {mongo_db})")

        # Check if required collections exist
        db = client[mongo_db]
        collections = db.list_collection_names()

        print("\nℹ️  Available collections:")
        for collection in collections:
            print(f"   - {collection}")

        # Check for expected collections
        expected = ['candidateDetails', 'taskBody']
        for coll in expected:
            if coll in collections:
                count = db[coll].count_documents({})
                print(f"✅ {coll}: {count} documents")
            else:
                print(f"⚠️  {coll}: NOT FOUND (will be created on first insert)")

        client.close()
        return True

    except Exception as e:
        print_status("Database Connection", False, f"({str(e)})")
        return False

def check_file_structure():
    """Check if required files and folders exist."""
    print("\n" + "="*60)
    print("4. CHECKING FILE STRUCTURE")
    print("="*60)

    required_items = [
        ('app.py', 'file'),
        ('db.py', 'file'),
        ('requirements.txt', 'file'),
        ('routes', 'dir'),
        ('templates', 'dir'),
        ('static', 'dir'),
    ]

    all_pass = True
    for item, item_type in required_items:
        exists = False
        if item_type == 'file':
            exists = os.path.isfile(item)
        else:
            exists = os.path.isdir(item)

        all_pass &= print_status(item, exists)

    return all_pass

def main():
    """Run all checks."""
    print("\n" + "🔍 DASHBOARD SETUP VERIFICATION SCRIPT" + "\n")
    print("This script will verify your environment is properly configured.\n")

    checks = [
        ("Environment Variables", check_environment_variables),
        ("Dependencies", check_dependencies),
        ("File Structure", check_file_structure),
        ("Database Connection", check_database_connection),
    ]

    results = {}
    for check_name, check_func in checks:
        try:
            results[check_name] = check_func()
        except Exception as e:
            print(f"\n❌ Error running {check_name}: {e}")
            results[check_name] = False

    # Summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)

    all_pass = all(results.values())

    for check_name, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status} - {check_name}")

    print("\n" + "="*60)
    if all_pass:
        print("🎉 ALL CHECKS PASSED! Your environment is ready.")
        print("\nYou can now run the application:")
        print("  python app.py")
    else:
        print("⚠️  SOME CHECKS FAILED. Please fix the issues above.")
        print("\nCommon solutions:")
        print("  1. Copy .env.example to .env and fill in your values")
        print("  2. Run: pip install -r requirements.txt")
        print("  3. Check your MongoDB connection string")
        print("  4. Verify MongoDB network access settings")
    print("="*60 + "\n")

    sys.exit(0 if all_pass else 1)

if __name__ == "__main__":
    main()
