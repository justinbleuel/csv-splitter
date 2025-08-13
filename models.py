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

class MergeOperation(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)
    files_merged = db.Column(db.Integer)  # Number of files merged
    file_names = db.Column(db.Text)  # JSON list of file names
    merge_type = db.Column(db.String(50))  # vertical or horizontal
    merge_options = db.Column(db.Text)  # JSON string of merge options
    total_input_rows = db.Column(db.Integer)
    total_output_rows = db.Column(db.Integer)
    total_columns = db.Column(db.Integer)
    processing_time = db.Column(db.Float)  # in seconds
    total_size_mb = db.Column(db.Float)  # combined size of input files
    
    @property
    def formatted_timestamp(self):
        return self.timestamp.strftime('%Y-%m-%d %H:%M:%S')
    
    @property
    def formatted_size(self):
        return f"{self.total_size_mb:.2f} MB"
    
    @property
    def files_list(self):
        """Parse file_names JSON string to list"""
        try:
            return json.loads(self.file_names) if self.file_names else []
        except:
            return []
    
    @property
    def options_dict(self):
        """Parse merge_options JSON string to dict"""
        try:
            return json.loads(self.merge_options) if self.merge_options else {}
        except:
            return {} 