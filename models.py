from datetime import datetime
from flask_sqlalchemy import SQLAlchemy
import json

db = SQLAlchemy()

class FileProcess(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    filename = db.Column(db.String(255), nullable=False)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)
    num_parts = db.Column(db.Integer, nullable=False)
    rows_processed = db.Column(db.Integer)
    processing_time = db.Column(db.Float)  # in seconds
    file_size = db.Column(db.Float)  # in MB
    
    @property
    def formatted_timestamp(self):
        return self.timestamp.strftime('%Y-%m-%d %H:%M:%S')
    
    @property
    def formatted_size(self):
        return f"{self.file_size:.2f} MB"

class DuplicateRemoval(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    filename = db.Column(db.String(255), nullable=False)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)
    original_rows = db.Column(db.Integer)
    duplicates_removed = db.Column(db.Integer)
    check_columns = db.Column(db.String(500))  # JSON list of column names
    keep_strategy = db.Column(db.String(100))
    strategy_column = db.Column(db.String(255), nullable=True)
    processing_time = db.Column(db.Float)  # in seconds
    file_size = db.Column(db.Float)  # in MB
    
    @property
    def formatted_timestamp(self):
        return self.timestamp.strftime('%Y-%m-%d %H:%M:%S')
    
    @property
    def formatted_size(self):
        return f"{self.file_size:.2f} MB"
    
    @property
    def columns_list(self):
        """Parse check_columns JSON string to list"""
        try:
            return json.loads(self.check_columns) if self.check_columns else []
        except:
            return []
    
    @property
    def removal_percentage(self):
        """Calculate percentage of rows removed"""
        if self.original_rows and self.original_rows > 0:
            return (self.duplicates_removed / self.original_rows) * 100
        return 0 