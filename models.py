from datetime import datetime
from flask_sqlalchemy import SQLAlchemy

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