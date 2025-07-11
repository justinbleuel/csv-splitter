#!/usr/bin/env python3
"""Initialize the database tables"""
import os
import sys

# Add the current directory to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from flask_app import app, HAS_DB
from models import db, FileProcess

def init_database():
    """Create all database tables"""
    if not HAS_DB:
        print("Database support not available")
        return False
    
    with app.app_context():
        try:
            # Create all tables
            db.create_all()
            print("✅ Database tables created successfully!")
            
            # Test the database by checking if tables exist
            test_count = FileProcess.query.count()
            print(f"✅ Database is working! Current records: {test_count}")
            
            return True
        except Exception as e:
            print(f"❌ Error creating database tables: {e}")
            return False

if __name__ == "__main__":
    print("Initializing database...")
    print(f"DATABASE_URL: {os.environ.get('DATABASE_URL', 'Not set')}")
    
    if init_database():
        print("\n✅ Database initialization complete!")
    else:
        print("\n❌ Database initialization failed!")
        sys.exit(1)