from flask import Flask, request, send_file, render_template_string, jsonify
try:
    from models import db, FileProcess, DuplicateRemoval, MergeOperation
    HAS_DB = True
except Exception as e:
    print(f"Database models not available: {e}")
    HAS_DB = False
import time
import pandas as pd
import math
import os
import io
import zipfile
import shutil
import csv
from werkzeug.utils import secure_filename
from datetime import datetime
import uuid
import threading
import json
from duplicate_remover import DuplicateRemover
from csv_merger import CSVMerger

# Notification imports
try:
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import Mail
    HAS_SENDGRID = True
except ImportError:
    HAS_SENDGRID = False
    print("SendGrid not available - email notifications disabled")

try:
    from twilio.rest import Client as TwilioClient
    HAS_TWILIO = True
except ImportError:
    HAS_TWILIO = False
    print("Twilio not available - SMS notifications disabled")

app = Flask(__name__)

# Database configuration - only if available
if HAS_DB:
    # Fix for Railway PostgreSQL (they use postgresql:// but SQLAlchemy needs postgresql://)
    database_url = os.environ.get('DATABASE_URL', 'sqlite:///csv_splitter.db')
    if database_url and database_url.startswith('postgres://'):
        database_url = database_url.replace('postgres://', 'postgresql://', 1)
    app.config['SQLALCHEMY_DATABASE_URI'] = database_url
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    
    try:
        # Initialize database
        db.init_app(app)
        # Create tables with proper context
        with app.app_context():
            db.create_all()
            print("Database initialized successfully")
    except Exception as e:
        print(f"Database initialization failed: {e}")
        print("Continuing without database features")
        HAS_DB = False

# Add these after the Flask app initialization
app.processed_files = []
app.total_splits = 0
# Progress tracking for large files
app.processing_status = {}

# Notification configuration
SENDGRID_API_KEY = os.environ.get('SENDGRID_API_KEY')
NOTIFICATION_EMAIL = os.environ.get('NOTIFICATION_EMAIL')
TWILIO_ACCOUNT_SID = os.environ.get('TWILIO_ACCOUNT_SID')
TWILIO_AUTH_TOKEN = os.environ.get('TWILIO_AUTH_TOKEN')
TWILIO_PHONE_FROM = os.environ.get('TWILIO_PHONE_FROM')
NOTIFICATION_PHONE = os.environ.get('NOTIFICATION_PHONE')

def send_notification(filename, num_parts, total_rows, file_size):
    """Send email and/or SMS notification about file processing"""
    
    # Email notification
    if HAS_SENDGRID and SENDGRID_API_KEY and NOTIFICATION_EMAIL:
        try:
            message = Mail(
                from_email='noreply@csv-splitter.app',
                to_emails=NOTIFICATION_EMAIL,
                subject='CSV File Processed',
                html_content=f'''
                <h3>CSV File Processed</h3>
                <p>A new CSV file has been processed:</p>
                <ul>
                    <li><strong>Filename:</strong> {filename}</li>
                    <li><strong>Size:</strong> {file_size:.2f} MB</li>
                    <li><strong>Total Rows:</strong> {total_rows:,}</li>
                    <li><strong>Split into:</strong> {num_parts} parts</li>
                    <li><strong>Time:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}</li>
                </ul>
                '''
            )
            sg = SendGridAPIClient(SENDGRID_API_KEY)
            sg.send(message)
            print(f"Email notification sent to {NOTIFICATION_EMAIL}")
        except Exception as e:
            print(f"Failed to send email: {e}")
    
    # SMS notification
    if HAS_TWILIO and all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_FROM, NOTIFICATION_PHONE]):
        try:
            client = TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
            message = client.messages.create(
                body=f"CSV Processed: {filename} ({file_size:.1f}MB, {total_rows:,} rows, {num_parts} parts)",
                from_=TWILIO_PHONE_FROM,
                to=NOTIFICATION_PHONE
            )
            print(f"SMS notification sent to {NOTIFICATION_PHONE}")
        except Exception as e:
            print(f"Failed to send SMS: {e}")

TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>CSV Tools</title>
    <script src="https://cdn.jsdelivr.net/npm/canvas-confetti@1.6.0/dist/confetti.browser.min.js"></script>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background-color: #f0f0ff;
            margin: 0;
            padding: 40px;
        }

        .container {
            max-width: 800px;
            margin: 0 auto;
            text-align: center;
        }

        .card {
            background: white;
            border-radius: 16px;
            padding: 40px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.1);
            margin-top: 20px;
            position: relative;
            overflow: hidden;
        }

        h1 {
            font-size: 48px;
            color: #111;
            margin-bottom: 8px;
        }

        .subtitle {
            font-size: 18px;
            color: #666;
            margin-bottom: 32px;
        }

        .feature-cards {
            display: flex;
            gap: 20px;
            margin-top: 40px;
            justify-content: center;
            flex-wrap: wrap;
        }

        .feature-card {
            background: white;
            border-radius: 16px;
            padding: 40px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.1);
            cursor: pointer;
            transition: all 0.3s ease;
            flex: 1;
            min-width: 280px;
            max-width: 350px;
        }

        .feature-card:hover {
            transform: translateY(-4px);
            box-shadow: 0 8px 24px rgba(0,0,0,0.15);
        }

        .feature-card h2 {
            font-size: 28px;
            margin-bottom: 12px;
            color: #111;
        }

        .feature-card p {
            font-size: 16px;
            color: #666;
            margin-bottom: 20px;
        }

        .feature-card .icon {
            font-size: 48px;
            margin-bottom: 20px;
        }

        .back-btn {
            background: transparent;
            color: #666;
            border: none;
            font-size: 14px;
            cursor: pointer;
            margin-bottom: 20px;
            display: inline-flex;
            align-items: center;
            gap: 8px;
        }

        .back-btn:hover {
            color: #333;
        }

        .upload-zone {
            border: 2px dashed #ccd;
            border-radius: 12px;
            padding: 40px;
            margin: 20px 0;
            cursor: pointer;
            transition: all 0.3s ease;
            position: relative;
        }

        .upload-zone:hover, .upload-zone.dragover {
            border-color: #99f;
            background: #f8f8ff;
        }

        .input-group {
            margin: 20px 0;
        }

        input[type="number"] {
            padding: 12px;
            border: 2px solid #eef;
            border-radius: 8px;
            font-size: 16px;
            width: 200px;
        }

        button, .action-btn {
            background: #000;
            color: white;
            border: none;
            border-radius: 8px;
            padding: 12px 24px;
            font-size: 16px;
            cursor: pointer;
            transition: all 0.3s ease;
            text-decoration: none;
            display: inline-block;
            margin: 0 8px;
        }

        button:hover, .action-btn:hover {
            transform: translateY(-1px);
        }

.loading-container {
    display: none;
    position: relative;
    height: 80px;
    margin: 40px auto;
}

.spinner {
    width: 40px;
    height: 40px;
    margin: 0 auto;
    border: 3px solid #f3f3f3;
    border-top: 3px solid #4ecdc4;
    border-right: 3px solid #45b7d1;
    border-bottom: 3px solid #4CB9E7;
    border-radius: 50%;
    animation: spin 1s linear infinite;
}

@keyframes spin {
    0% { transform: rotate(0deg); }
    100% { transform: rotate(360deg); }
}

        .strand {
            position: absolute;
            height: 8px;
            width: 8px;
            border-radius: 50%;
            animation: moveStrand 2s infinite ease-in-out;
        }

        .success-container {
            display: none;
            animation: fadeIn 0.5s ease-out;
        }

        .action-buttons {
            margin-top: 20px;
            display: flex;
            justify-content: center;
            gap: 12px;
        }

        .download-btn {
            background: linear-gradient(45deg, #4ecdc4, #45b7d1);
        }

        .reset-btn {
            background: #f8f9fa;
            color: #333;
            border: 1px solid #dee2e6;
        }

        @keyframes moveStrand {
            0%, 100% { transform: translateY(0) scale(1); }
            50% { transform: translateY(50px) scale(0.5); }
        }

        @keyframes fadeIn {
            from { opacity: 0; transform: translateY(10px); }
            to { opacity: 1; transform: translateY(0); }
        }

        .hidden {
            display: none !important;
        }

        label {
            display: block;
            font-weight: 600;
            color: #333;
            margin-bottom: 8px;
            text-align: left;
        }

        select {
            border: 2px solid #eef;
            border-radius: 8px;
            font-size: 14px;
        }
    </style>
</head>
<body>
    <div class="container">
        <!-- Home Section with Feature Cards -->
        <div id="home-section">
            <h1>CSV Tools</h1>
            <p class="subtitle">Powerful tools for working with CSV files</p>
            
            <div class="feature-cards">
                <div class="feature-card" onclick="showSplitter()">
                    <div class="icon">✂️</div>
                    <h2>Split Large CSV</h2>
                    <p>Break large CSV files into smaller, manageable parts</p>
                </div>
                <div class="feature-card" onclick="showDuplicateRemover()">
                    <div class="icon">🧹</div>
                    <h2>Remove Duplicates</h2>
                    <p>Clean your CSV by removing duplicate rows based on your criteria</p>
                </div>
                <div class="feature-card" onclick="showMerger()">
                    <div class="icon">🔗</div>
                    <h2>Merge CSV Files</h2>
                    <p>Combine multiple CSV files into a single consolidated file</p>
                </div>
            </div>
        </div>

        <!-- CSV Splitter Section -->
        <div id="splitter-section" class="hidden">
            <button class="back-btn" onclick="showHome()">← Back to tools</button>
            <h1>CSV Splitter</h1>
            <p class="subtitle">Split large CSV files into manageable chunks</p>
            
            <div class="card">
            <form id="upload-form" action="/split" method="post" enctype="multipart/form-data">
                <div class="upload-zone" id="upload-zone">
                    <input type="file" id="file-input" name="file" accept=".csv" style="display: none">
                    <p id="file-name">Drop your CSV file here or click to browse</p>
                </div>

                <div class="input-group">
                    <input type="number" name="max_rows" value="50000" min="1" max="1000000" placeholder="Rows per file">
                </div>

                <button type="submit">Split CSV</button>
            </form>

        <div class="loading-container" id="loading">
            <div class="spinner"></div>
            <div class="progress-info" id="progress-info" style="display: none; margin-top: 20px; color: white;">
                <p id="progress-message">Processing...</p>
                <div style="width: 300px; height: 20px; background-color: rgba(255,255,255,0.3); border-radius: 10px; margin: 10px auto;">
                    <div id="progress-bar" style="width: 0%; height: 100%; background-color: white; border-radius: 10px; transition: width 0.3s;"></div>
                </div>
            </div>
        </div>

            <div class="success-container" id="success-container">
                <p class="success-message">
                    Your CSV has been split successfully! 🎉
                </p>
                <div class="action-buttons">
                    <a href="#" class="action-btn download-btn" id="download-btn">
                        Download Split Files
                    </a>
                    <button class="action-btn reset-btn" onclick="resetForm()">
                        Split Another File
                    </button>
                </div>
            </div>
        </div>
        </div>

        <!-- Duplicate Remover Section -->
        <div id="duplicate-remover-section" class="hidden">
            <button class="back-btn" onclick="showHome()">← Back to tools</button>
            <h1>Duplicate Remover</h1>
            <p class="subtitle">Remove duplicate rows from your CSV files</p>
            
            <div class="card">
                <!-- File Upload -->
                <div id="duplicate-upload-section">
                    <div class="upload-zone" id="duplicate-upload-zone">
                        <input type="file" id="duplicate-file-input" accept=".csv" style="display: none">
                        <p id="duplicate-file-name">Drop your CSV file here or click to browse</p>
                    </div>
                </div>

                <!-- Column Selection (hidden initially) -->
                <div id="duplicate-config-section" class="hidden">
                    <h3>Select Duplicate Detection Criteria</h3>
                    
                    <div class="input-group">
                        <label>Select columns to check for duplicates:</label>
                        <select id="duplicate-columns" multiple style="width: 100%; height: 120px; padding: 8px; margin-top: 8px;">
                        </select>
                    </div>

                    <div class="input-group">
                        <label>Keep strategy:</label>
                        <select id="keep-strategy" style="width: 100%; padding: 8px; margin-top: 8px;">
                            <option value="first">Keep first occurrence</option>
                            <option value="last">Keep last occurrence</option>
                            <option value="not_empty">Keep row where specific column is not empty</option>
                            <option value="most_recent">Keep row with most recent date</option>
                            <option value="max_value">Keep row with highest value</option>
                        </select>
                    </div>

                    <div id="strategy-column-group" class="input-group hidden">
                        <label>Select column for strategy:</label>
                        <select id="strategy-column" style="width: 100%; padding: 8px; margin-top: 8px;">
                        </select>
                    </div>

                    <div style="margin-top: 20px;">
                        <button onclick="previewDuplicates()" style="background: #666;">Preview Duplicates</button>
                        <button onclick="processDuplicates()">Remove Duplicates</button>
                    </div>
                </div>

                <!-- Preview Section -->
                <div id="duplicate-preview-section" class="hidden" style="margin-top: 20px; text-align: left;">
                    <h3>Duplicate Preview</h3>
                    <div id="duplicate-preview-content"></div>
                </div>

                <!-- Loading -->
                <div class="loading-container" id="duplicate-loading" style="display: none;">
                    <div class="spinner"></div>
                    <p style="color: #666; margin-top: 10px;">Processing duplicates...</p>
                </div>

                <!-- Success -->
                <div id="duplicate-success" class="hidden">
                    <p class="success-message">Duplicates removed successfully! 🎉</p>
                    <div id="duplicate-stats" style="margin: 20px 0;"></div>
                    <div class="action-buttons">
                        <a href="#" class="action-btn download-btn" id="duplicate-download-btn">
                            Download Cleaned CSV
                        </a>
                        <button class="action-btn reset-btn" onclick="resetDuplicateForm()">
                            Clean Another File
                        </button>
                    </div>
                </div>
            </div>
        </div>

        <!-- CSV Merger Section -->
        <div id="merger-section" class="hidden">
            <button class="back-btn" onclick="showHome()">← Back to tools</button>
            <h1>CSV Merger</h1>
            <p class="subtitle">Combine multiple CSV files into one</p>
            
            <div class="card">
                <!-- File Upload Zone -->
                <div id="merger-upload-section">
                    <div class="upload-zone" id="merger-upload-zone">
                        <input type="file" id="merger-file-input" accept=".csv" multiple style="display: none">
                        <p id="merger-file-text">Drop CSV files here or click to browse (select multiple)</p>
                    </div>
                    
                    <!-- File List -->
                    <div id="merger-file-list" class="hidden" style="margin-top: 20px;">
                        <h3>Files to Merge:</h3>
                        <div id="merger-file-items"></div>
                    </div>
                </div>

                <!-- Merge Configuration (hidden initially) -->
                <div id="merger-config-section" class="hidden">
                    <h3>Merge Configuration</h3>
                    
                    <div class="input-group">
                        <label>Merge Type:</label>
                        <select id="merge-type" style="width: 100%; padding: 8px; margin-top: 8px;">
                            <option value="vertical">Vertical Append (Stack rows)</option>
                            <option value="horizontal">Horizontal Join (Merge columns)</option>
                        </select>
                    </div>

                    <!-- Vertical Options -->
                    <div id="vertical-options" style="margin-top: 20px;">
                        <div class="input-group">
                            <label>Column Handling:</label>
                            <select id="columns-mode" style="width: 100%; padding: 8px; margin-top: 8px;">
                                <option value="union">Include all columns (union)</option>
                                <option value="intersection">Only common columns (intersection)</option>
                            </select>
                        </div>
                        
                        <div class="input-group" style="margin-top: 10px;">
                            <label>
                                <input type="checkbox" id="include-source" style="margin-right: 5px;">
                                Add source file column
                            </label>
                        </div>
                    </div>

                    <!-- Horizontal Options -->
                    <div id="horizontal-options" class="hidden" style="margin-top: 20px;">
                        <div class="input-group">
                            <label>Join Columns:</label>
                            <select id="join-columns" multiple style="width: 100%; height: 100px; padding: 8px; margin-top: 8px;">
                            </select>
                        </div>
                        
                        <div class="input-group" style="margin-top: 10px;">
                            <label>Join Type:</label>
                            <select id="join-type" style="width: 100%; padding: 8px; margin-top: 8px;">
                                <option value="inner">Inner Join (matching rows only)</option>
                                <option value="left">Left Join (all from first file)</option>
                                <option value="right">Right Join (all from second file)</option>
                                <option value="outer">Outer Join (all rows)</option>
                            </select>
                        </div>
                    </div>

                    <div style="margin-top: 20px;">
                        <button onclick="previewMerge()" style="background: #666;">Preview Merge</button>
                        <button onclick="processMerge()">Merge Files</button>
                    </div>
                </div>

                <!-- Preview Section -->
                <div id="merger-preview-section" class="hidden" style="margin-top: 20px; text-align: left;">
                    <h3>Merge Preview</h3>
                    <div id="merger-preview-content"></div>
                </div>

                <!-- Loading -->
                <div class="loading-container" id="merger-loading" style="display: none;">
                    <div class="spinner"></div>
                    <p style="color: #666; margin-top: 10px;">Merging files...</p>
                    <div class="progress-info" id="merger-progress-info" style="display: none; margin-top: 20px;">
                        <p id="merger-progress-message">Processing...</p>
                        <div style="width: 300px; height: 20px; background-color: rgba(200,200,200,0.3); border-radius: 10px; margin: 10px auto;">
                            <div id="merger-progress-bar" style="width: 0%; height: 100%; background-color: #4ecdc4; border-radius: 10px; transition: width 0.3s;"></div>
                        </div>
                    </div>
                </div>

                <!-- Success -->
                <div id="merger-success" class="hidden">
                    <p class="success-message">Files merged successfully! 🎉</p>
                    <div id="merger-stats" style="margin: 20px 0;"></div>
                    <div class="action-buttons">
                        <a href="#" class="action-btn download-btn" id="merger-download-btn">
                            Download Merged CSV
                        </a>
                        <button class="action-btn reset-btn" onclick="resetMergerForm()">
                            Merge More Files
                        </button>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <script>
        // Section navigation functions
        function showHome() {
            document.getElementById('home-section').classList.remove('hidden');
            document.getElementById('splitter-section').classList.add('hidden');
            document.getElementById('duplicate-remover-section').classList.add('hidden');
            document.getElementById('merger-section').classList.add('hidden');
        }

        function showSplitter() {
            document.getElementById('home-section').classList.add('hidden');
            document.getElementById('splitter-section').classList.remove('hidden');
            document.getElementById('duplicate-remover-section').classList.add('hidden');
            document.getElementById('merger-section').classList.add('hidden');
        }

        function showDuplicateRemover() {
            document.getElementById('home-section').classList.add('hidden');
            document.getElementById('splitter-section').classList.add('hidden');
            document.getElementById('duplicate-remover-section').classList.remove('hidden');
            document.getElementById('merger-section').classList.add('hidden');
        }

        function showMerger() {
            document.getElementById('home-section').classList.add('hidden');
            document.getElementById('splitter-section').classList.add('hidden');
            document.getElementById('duplicate-remover-section').classList.add('hidden');
            document.getElementById('merger-section').classList.remove('hidden');
        }

        // Initialize drag and drop zone
        const uploadZone = document.getElementById('upload-zone');
        const fileInput = document.getElementById('file-input');
        const fileName = document.getElementById('file-name');

        // Click to upload
        uploadZone.addEventListener('click', () => {
            fileInput.click();
        });

        // File input change
        fileInput.addEventListener('change', () => {
            if (fileInput.files.length > 0) {
                fileName.textContent = fileInput.files[0].name;
            }
        });

        // Drag and drop handlers
        uploadZone.addEventListener('dragenter', (e) => {
            e.preventDefault();
            uploadZone.classList.add('dragover');
        });

        uploadZone.addEventListener('dragover', (e) => {
            e.preventDefault();
            uploadZone.classList.add('dragover');
        });

        uploadZone.addEventListener('dragleave', (e) => {
            e.preventDefault();
            uploadZone.classList.remove('dragover');
        });

        uploadZone.addEventListener('drop', (e) => {
            e.preventDefault();
            uploadZone.classList.remove('dragover');
            
            const files = e.dataTransfer.files;
            if (files.length > 0 && files[0].name.endsWith('.csv')) {
                fileInput.files = files;
                fileName.textContent = files[0].name;
            }
        });

        // Form submission
        document.getElementById('upload-form').onsubmit = async (e) => {
            e.preventDefault();
            
            const formData = new FormData(e.target);
            document.getElementById('upload-form').style.display = 'none';
            document.getElementById('loading').style.display = 'block';

            try {
                const response = await fetch('/split', {
                    method: 'POST',
                    body: formData
                });

                if (!response.ok) throw new Error('Failed to process file');

                // Check if it's an async task
                const contentType = response.headers.get('content-type');
                if (contentType && contentType.includes('application/json')) {
                    const data = await response.json();
                    if (data.task_id) {
                        // Large file processing
                        document.getElementById('progress-info').style.display = 'block';
                        await trackProgress(data.task_id);
                    }
                } else {
                    // Small file - direct download
                    const blob = await response.blob();
                    const downloadUrl = window.URL.createObjectURL(blob);
                    
                    const downloadBtn = document.getElementById('download-btn');
                    downloadBtn.href = downloadUrl;
                    downloadBtn.download = 'split_csv_files.zip';
                    
                    showSuccess();
                }
            } catch (error) {
                alert('Error processing file: ' + error.message);
                resetForm();
            }
        };

        function showSuccess() {
    document.getElementById('loading').style.display = 'none';
    document.getElementById('success-container').style.display = 'block';
    
    // Celebration confetti
    const count = 200;
    const defaults = {
        origin: { y: 0.7 },
        colors: ['#4ecdc4', '#45b7d1', '#4CB9E7', '#FFB000']
    };

    function fire(particleRatio, opts) {
        confetti({
            ...defaults,
            ...opts,
            particleCount: Math.floor(count * particleRatio)
        });
    }

    fire(0.25, {
        spread: 26,
        startVelocity: 55,
    });
    fire(0.2, {
        spread: 60,
    });
    fire(0.35, {
        spread: 100,
        decay: 0.91,
        scalar: 0.8
    });
    fire(0.1, {
        spread: 120,
        startVelocity: 25,
        decay: 0.92,
        scalar: 1.2
    });
    fire(0.1, {
        spread: 120,
        startVelocity: 45,
    });
}

        async function trackProgress(taskId) {
            const progressBar = document.getElementById('progress-bar');
            const progressMessage = document.getElementById('progress-message');
            
            const checkInterval = setInterval(async () => {
                try {
                    const response = await fetch(`/progress/${taskId}`);
                    const status = await response.json();
                    
                    if (status.status === 'processing') {
                        progressBar.style.width = status.progress + '%';
                        progressMessage.textContent = status.message;
                    } else if (status.status === 'complete') {
                        clearInterval(checkInterval);
                        progressBar.style.width = '100%';
                        progressMessage.textContent = 'Processing complete!';
                        
                        // Set download link
                        const downloadBtn = document.getElementById('download-btn');
                        downloadBtn.href = `/download/${taskId}`;
                        downloadBtn.removeAttribute('download');
                        
                        setTimeout(() => {
                            showSuccess();
                        }, 500);
                    } else if (status.status === 'error') {
                        clearInterval(checkInterval);
                        throw new Error(status.message);
                    }
                } catch (error) {
                    clearInterval(checkInterval);
                    alert('Error: ' + error.message);
                    resetForm();
                }
            }, 1000);
        }
        
        function resetForm() {
            document.getElementById('upload-form').reset();
            fileName.textContent = 'Drop your CSV file here or click to browse';
            document.getElementById('upload-form').style.display = 'block';
            document.getElementById('success-container').style.display = 'none';
            document.getElementById('loading').style.display = 'none';
            document.getElementById('progress-info').style.display = 'none';
            document.getElementById('progress-bar').style.width = '0%';
        }

        // Duplicate Remover Functions
        let duplicateFile = null;
        
        // Initialize duplicate remover
        const duplicateUploadZone = document.getElementById('duplicate-upload-zone');
        const duplicateFileInput = document.getElementById('duplicate-file-input');
        const duplicateFileName = document.getElementById('duplicate-file-name');
        
        // Click to upload
        duplicateUploadZone.addEventListener('click', () => {
            duplicateFileInput.click();
        });
        
        // File input change
        duplicateFileInput.addEventListener('change', async () => {
            if (duplicateFileInput.files.length > 0) {
                duplicateFile = duplicateFileInput.files[0];
                duplicateFileName.textContent = duplicateFile.name;
                await analyzeCsvFile();
            }
        });
        
        // Drag and drop handlers
        duplicateUploadZone.addEventListener('dragenter', (e) => {
            e.preventDefault();
            duplicateUploadZone.classList.add('dragover');
        });
        
        duplicateUploadZone.addEventListener('dragover', (e) => {
            e.preventDefault();
            duplicateUploadZone.classList.add('dragover');
        });
        
        duplicateUploadZone.addEventListener('dragleave', (e) => {
            e.preventDefault();
            duplicateUploadZone.classList.remove('dragover');
        });
        
        duplicateUploadZone.addEventListener('drop', (e) => {
            e.preventDefault();
            duplicateUploadZone.classList.remove('dragover');
            
            const files = e.dataTransfer.files;
            if (files.length > 0 && files[0].name.endsWith('.csv')) {
                duplicateFileInput.files = files;
                duplicateFile = files[0];
                duplicateFileName.textContent = files[0].name;
                analyzeCsvFile();
            }
        });
        
        // Keep strategy change handler
        document.getElementById('keep-strategy').addEventListener('change', (e) => {
            const strategyColumnGroup = document.getElementById('strategy-column-group');
            if (['not_empty', 'most_recent', 'max_value'].includes(e.target.value)) {
                strategyColumnGroup.classList.remove('hidden');
            } else {
                strategyColumnGroup.classList.add('hidden');
            }
        });
        
        async function analyzeCsvFile() {
            const formData = new FormData();
            formData.append('file', duplicateFile);
            
            try {
                const response = await fetch('/analyze-csv', {
                    method: 'POST',
                    body: formData
                });
                
                if (!response.ok) throw new Error('Failed to analyze file');
                
                const data = await response.json();
                
                // Populate column selectors
                const columnSelect = document.getElementById('duplicate-columns');
                const strategyColumnSelect = document.getElementById('strategy-column');
                
                columnSelect.innerHTML = '';
                strategyColumnSelect.innerHTML = '';
                
                data.columns.forEach(col => {
                    const option1 = new Option(col, col);
                    const option2 = new Option(col, col);
                    columnSelect.appendChild(option1);
                    strategyColumnSelect.appendChild(option2);
                });
                
                // Show configuration section
                document.getElementById('duplicate-upload-section').classList.add('hidden');
                document.getElementById('duplicate-config-section').classList.remove('hidden');
                
            } catch (error) {
                alert('Error analyzing file: ' + error.message);
                resetDuplicateForm();
            }
        }
        
        async function previewDuplicates() {
            const selectedColumns = Array.from(document.getElementById('duplicate-columns').selectedOptions)
                .map(opt => opt.value);
                
            if (selectedColumns.length === 0) {
                alert('Please select at least one column to check for duplicates');
                return;
            }
            
            const formData = new FormData();
            formData.append('file', duplicateFile);
            formData.append('columns', JSON.stringify(selectedColumns));
            
            try {
                const response = await fetch('/preview-duplicates', {
                    method: 'POST',
                    body: formData
                });
                
                if (!response.ok) throw new Error('Failed to preview duplicates');
                
                const data = await response.json();
                
                const previewSection = document.getElementById('duplicate-preview-section');
                const previewContent = document.getElementById('duplicate-preview-content');
                
                if (data.total_duplicate_rows === 0) {
                    previewContent.innerHTML = '<p>No duplicates found with the selected columns.</p>';
                } else {
                    let html = `<p><strong>${data.total_duplicate_rows}</strong> duplicate rows found 
                        (${data.rows_to_remove} will be removed)</p>`;
                    
                    if (data.preview.length > 0) {
                        html += '<div style="margin-top: 10px; font-size: 14px;">';
                        data.preview.forEach((group, idx) => {
                            html += `<div style="margin-bottom: 15px; padding: 10px; background: #f5f5f5; border-radius: 4px;">`;
                            html += `<strong>Group ${idx + 1}:</strong> ${group.occurrences} occurrences<br>`;
                            html += `Duplicate values: ${JSON.stringify(group.duplicate_values)}<br>`;
                            html += '</div>';
                        });
                        html += '</div>';
                    }
                    
                    previewContent.innerHTML = html;
                }
                
                previewSection.classList.remove('hidden');
                
            } catch (error) {
                alert('Error previewing duplicates: ' + error.message);
            }
        }
        
        async function processDuplicates() {
            const selectedColumns = Array.from(document.getElementById('duplicate-columns').selectedOptions)
                .map(opt => opt.value);
                
            if (selectedColumns.length === 0) {
                alert('Please select at least one column to check for duplicates');
                return;
            }
            
            const keepStrategy = document.getElementById('keep-strategy').value;
            const strategyColumn = document.getElementById('strategy-column').value;
            
            const formData = new FormData();
            formData.append('file', duplicateFile);
            formData.append('columns', JSON.stringify(selectedColumns));
            formData.append('keep_strategy', keepStrategy);
            
            if (['not_empty', 'most_recent', 'max_value'].includes(keepStrategy)) {
                formData.append('strategy_column', strategyColumn);
            }
            
            // Hide config and show loading
            document.getElementById('duplicate-config-section').classList.add('hidden');
            document.getElementById('duplicate-preview-section').classList.add('hidden');
            document.getElementById('duplicate-loading').style.display = 'block';
            
            try {
                const response = await fetch('/process-duplicates', {
                    method: 'POST',
                    body: formData
                });
                
                if (!response.ok) throw new Error('Failed to process duplicates');
                
                const blob = await response.blob();
                const downloadUrl = window.URL.createObjectURL(blob);
                
                // Get stats from response header
                const stats = JSON.parse(response.headers.get('X-Process-Stats') || '{}');
                
                // Update success section
                document.getElementById('duplicate-stats').innerHTML = `
                    <p>Original rows: <strong>${stats.original_rows || 0}</strong></p>
                    <p>Duplicates removed: <strong>${stats.rows_removed || 0}</strong></p>
                    <p>Final rows: <strong>${stats.cleaned_rows || 0}</strong></p>
                `;
                
                const downloadBtn = document.getElementById('duplicate-download-btn');
                downloadBtn.href = downloadUrl;
                downloadBtn.download = `cleaned_${duplicateFile.name}`;
                
                // Show success
                document.getElementById('duplicate-loading').style.display = 'none';
                document.getElementById('duplicate-success').classList.remove('hidden');
                
            } catch (error) {
                alert('Error processing duplicates: ' + error.message);
                resetDuplicateForm();
            }
        }
        
        function resetDuplicateForm() {
            duplicateFile = null;
            duplicateFileInput.value = '';
            duplicateFileName.textContent = 'Drop your CSV file here or click to browse';
            
            document.getElementById('duplicate-upload-section').classList.remove('hidden');
            document.getElementById('duplicate-config-section').classList.add('hidden');
            document.getElementById('duplicate-preview-section').classList.add('hidden');
            document.getElementById('duplicate-loading').style.display = 'none';
            document.getElementById('duplicate-success').classList.add('hidden');
            
            document.getElementById('duplicate-columns').innerHTML = '';
            document.getElementById('strategy-column').innerHTML = '';
        }
        
        // CSV Merger Functions
        let mergerFiles = [];
        let mergerFileData = {};
        
        // Initialize merger
        const mergerUploadZone = document.getElementById('merger-upload-zone');
        const mergerFileInput = document.getElementById('merger-file-input');
        const mergerFileText = document.getElementById('merger-file-text');
        
        // Click to upload
        mergerUploadZone.addEventListener('click', () => {
            mergerFileInput.click();
        });
        
        // File input change
        mergerFileInput.addEventListener('change', async () => {
            await handleMergerFiles(mergerFileInput.files);
        });
        
        // Drag and drop handlers
        mergerUploadZone.addEventListener('dragenter', (e) => {
            e.preventDefault();
            mergerUploadZone.classList.add('dragover');
        });
        
        mergerUploadZone.addEventListener('dragover', (e) => {
            e.preventDefault();
            mergerUploadZone.classList.add('dragover');
        });
        
        mergerUploadZone.addEventListener('dragleave', (e) => {
            e.preventDefault();
            mergerUploadZone.classList.remove('dragover');
        });
        
        mergerUploadZone.addEventListener('drop', async (e) => {
            e.preventDefault();
            mergerUploadZone.classList.remove('dragover');
            
            const files = Array.from(e.dataTransfer.files).filter(f => f.name.endsWith('.csv'));
            if (files.length > 0) {
                await handleMergerFiles(files);
            }
        });
        
        // Merge type change handler
        document.getElementById('merge-type').addEventListener('change', (e) => {
            if (e.target.value === 'vertical') {
                document.getElementById('vertical-options').classList.remove('hidden');
                document.getElementById('horizontal-options').classList.add('hidden');
            } else {
                document.getElementById('vertical-options').classList.add('hidden');
                document.getElementById('horizontal-options').classList.remove('hidden');
                // Populate join columns if we have files
                if (mergerFiles.length >= 2) {
                    populateJoinColumns();
                }
            }
        });
        
        async function handleMergerFiles(files) {
            console.log('handleMergerFiles called with', files.length, 'files');
            for (const file of files) {
                if (!mergerFileData[file.name]) {
                    mergerFiles.push(file);
                    mergerFileData[file.name] = file;
                }
            }
            
            console.log('Total merger files:', mergerFiles.length);
            updateMergerFileList();
            
            if (mergerFiles.length >= 2) {
                console.log('Analyzing files...');
                // Analyze files
                await analyzeMergerFiles();
            }
        }
        
        function updateMergerFileList() {
            const fileList = document.getElementById('merger-file-list');
            const fileItems = document.getElementById('merger-file-items');
            
            if (mergerFiles.length === 0) {
                fileList.classList.add('hidden');
                return;
            }
            
            fileList.classList.remove('hidden');
            
            let html = '';
            mergerFiles.forEach((file, index) => {
                html += `
                    <div style="padding: 10px; background: #f5f5f5; margin: 5px 0; border-radius: 4px; display: flex; justify-content: space-between; align-items: center;">
                        <span>${file.name} (${(file.size / 1024 / 1024).toFixed(2)} MB)</span>
                        <button onclick="removeMergerFile(${index})" style="background: #ff4444; padding: 5px 10px; font-size: 12px;">Remove</button>
                    </div>
                `;
            });
            
            fileItems.innerHTML = html;
            mergerFileText.textContent = `${mergerFiles.length} files selected. Add more or continue.`;
        }
        
        function removeMergerFile(index) {
            const file = mergerFiles[index];
            delete mergerFileData[file.name];
            mergerFiles.splice(index, 1);
            updateMergerFileList();
            
            if (mergerFiles.length < 2) {
                document.getElementById('merger-config-section').classList.add('hidden');
            }
        }
        
        async function analyzeMergerFiles() {
            console.log('analyzeMergerFiles called');
            const formData = new FormData();
            mergerFiles.forEach((file, index) => {
                console.log(`Adding file ${index}:`, file.name);
                formData.append(`file_${index}`, file);
            });
            
            try {
                console.log('Fetching /analyze-merge-files...');
                const response = await fetch('/analyze-merge-files', {
                    method: 'POST',
                    body: formData
                });
                
                console.log('Response status:', response.status);
                if (!response.ok) {
                    const errorText = await response.text();
                    console.error('Error response:', errorText);
                    throw new Error('Failed to analyze files: ' + errorText);
                }
                
                const data = await response.json();
                console.log('Analysis data:', data);
                
                // Store analysis data globally for later use
                window.mergerAnalysisData = data;
                
                // Show configuration section
                console.log('Showing configuration section...');
                const configSection = document.getElementById('merger-config-section');
                console.log('Config section element:', configSection);
                if (configSection) {
                    configSection.classList.remove('hidden');
                    console.log('Hidden class removed');
                } else {
                    console.error('merger-config-section element not found!');
                }
                
                // If horizontal merge, populate join columns
                if (document.getElementById('merge-type').value === 'horizontal') {
                    populateJoinColumns();
                }
                
            } catch (error) {
                console.error('Error in analyzeMergerFiles:', error);
                alert('Error analyzing files: ' + error.message);
            }
        }
        
        function populateJoinColumns() {
            const joinColumnsSelect = document.getElementById('join-columns');
            
            if (window.mergerAnalysisData && window.mergerAnalysisData.common_columns) {
                joinColumnsSelect.innerHTML = '';
                
                window.mergerAnalysisData.common_columns.forEach(col => {
                    const option = new Option(col, col);
                    joinColumnsSelect.appendChild(option);
                });
                
                if (window.mergerAnalysisData.common_columns.length === 0) {
                    joinColumnsSelect.innerHTML = '<option>No common columns found</option>';
                }
            } else {
                joinColumnsSelect.innerHTML = '<option>No common columns found</option>';
            }
        }
        
        async function previewMerge() {
            const mergeType = document.getElementById('merge-type').value;
            const formData = new FormData();
            
            mergerFiles.forEach((file, index) => {
                formData.append(`file_${index}`, file);
            });
            
            formData.append('merge_type', mergeType);
            
            if (mergeType === 'vertical') {
                formData.append('columns_mode', document.getElementById('columns-mode').value);
                formData.append('include_source', document.getElementById('include-source').checked);
            } else {
                const selectedJoinColumns = Array.from(document.getElementById('join-columns').selectedOptions)
                    .map(opt => opt.value);
                formData.append('join_columns', JSON.stringify(selectedJoinColumns));
                formData.append('join_type', document.getElementById('join-type').value);
            }
            
            try {
                const response = await fetch('/preview-merge', {
                    method: 'POST',
                    body: formData
                });
                
                if (!response.ok) throw new Error('Failed to generate preview');
                
                const data = await response.json();
                
                const previewSection = document.getElementById('merger-preview-section');
                const previewContent = document.getElementById('merger-preview-content');
                
                let html = '<div style="overflow-x: auto;">';
                html += '<table style="border-collapse: collapse; width: 100%; font-size: 14px;">';
                
                // Header
                html += '<thead><tr>';
                data.columns.forEach(col => {
                    html += `<th style="border: 1px solid #ddd; padding: 8px; background: #f5f5f5;">${col}</th>`;
                });
                html += '</tr></thead>';
                
                // Data rows
                html += '<tbody>';
                data.preview_data.forEach(row => {
                    html += '<tr>';
                    data.columns.forEach(col => {
                        html += `<td style="border: 1px solid #ddd; padding: 8px;">${row[col] || ''}</td>`;
                    });
                    html += '</tr>';
                });
                html += '</tbody></table></div>';
                
                // Stats
                html += '<div style="margin-top: 15px;">';
                html += `<p><strong>Merge Statistics:</strong></p>`;
                html += `<ul style="list-style: none; padding: 0;">`;
                Object.entries(data.stats).forEach(([key, value]) => {
                    const label = key.replace(/_/g, ' ').replace(/\\b\\w/g, l => l.toUpperCase());
                    html += `<li>${label}: ${value}</li>`;
                });
                html += '</ul></div>';
                
                previewContent.innerHTML = html;
                previewSection.classList.remove('hidden');
                
            } catch (error) {
                alert('Error generating preview: ' + error.message);
            }
        }
        
        async function processMerge() {
            const mergeType = document.getElementById('merge-type').value;
            const formData = new FormData();
            
            mergerFiles.forEach((file, index) => {
                formData.append(`file_${index}`, file);
            });
            
            formData.append('merge_type', mergeType);
            
            if (mergeType === 'vertical') {
                formData.append('columns_mode', document.getElementById('columns-mode').value);
                formData.append('include_source', document.getElementById('include-source').checked);
            } else {
                const selectedJoinColumns = Array.from(document.getElementById('join-columns').selectedOptions)
                    .map(opt => opt.value);
                formData.append('join_columns', JSON.stringify(selectedJoinColumns));
                formData.append('join_type', document.getElementById('join-type').value);
            }
            
            // Hide config and show loading
            document.getElementById('merger-config-section').classList.add('hidden');
            document.getElementById('merger-preview-section').classList.add('hidden');
            document.getElementById('merger-loading').style.display = 'block';
            
            try {
                const response = await fetch('/process-merge', {
                    method: 'POST',
                    body: formData
                });
                
                if (!response.ok) throw new Error('Failed to merge files');
                
                // Check if it's an async task
                const contentType = response.headers.get('content-type');
                if (contentType && contentType.includes('application/json')) {
                    const data = await response.json();
                    if (data.task_id) {
                        // Large file processing
                        document.getElementById('merger-progress-info').style.display = 'block';
                        await trackMergerProgress(data.task_id);
                    }
                } else {
                    // Small file - direct download
                    const blob = await response.blob();
                    const downloadUrl = window.URL.createObjectURL(blob);
                    
                    // Get stats from response header
                    const stats = JSON.parse(response.headers.get('X-Merge-Stats') || '{}');
                    
                    // Update success section
                    document.getElementById('merger-stats').innerHTML = `
                        <p>Files merged: <strong>${stats.files_merged || mergerFiles.length}</strong></p>
                        <p>Total rows: <strong>${(stats.total_rows || 0).toLocaleString()}</strong></p>
                        <p>Total columns: <strong>${stats.total_columns || 0}</strong></p>
                    `;
                    
                    const downloadBtn = document.getElementById('merger-download-btn');
                    downloadBtn.href = downloadUrl;
                    downloadBtn.download = `merged_${new Date().getTime()}.csv`;
                    
                    // Show success
                    document.getElementById('merger-loading').style.display = 'none';
                    document.getElementById('merger-success').classList.remove('hidden');
                }
                
            } catch (error) {
                alert('Error merging files: ' + error.message);
                resetMergerForm();
            }
        }
        
        async function trackMergerProgress(taskId) {
            const progressBar = document.getElementById('merger-progress-bar');
            const progressMessage = document.getElementById('merger-progress-message');
            
            const checkInterval = setInterval(async () => {
                try {
                    const response = await fetch(`/merge-progress/${taskId}`);
                    const status = await response.json();
                    
                    if (status.status === 'processing') {
                        progressBar.style.width = status.progress + '%';
                        progressMessage.textContent = status.message;
                    } else if (status.status === 'complete') {
                        clearInterval(checkInterval);
                        progressBar.style.width = '100%';
                        progressMessage.textContent = 'Merge complete!';
                        
                        // Set download link
                        const downloadBtn = document.getElementById('merger-download-btn');
                        downloadBtn.href = `/download-merge/${taskId}`;
                        downloadBtn.removeAttribute('download');
                        
                        // Update stats
                        document.getElementById('merger-stats').innerHTML = `
                            <p>Files merged: <strong>${status.stats.files_merged || mergerFiles.length}</strong></p>
                            <p>Total rows: <strong>${(status.stats.total_rows || 0).toLocaleString()}</strong></p>
                            <p>Total columns: <strong>${status.stats.total_columns || 0}</strong></p>
                        `;
                        
                        setTimeout(() => {
                            document.getElementById('merger-loading').style.display = 'none';
                            document.getElementById('merger-success').classList.remove('hidden');
                        }, 500);
                    } else if (status.status === 'error') {
                        clearInterval(checkInterval);
                        throw new Error(status.message);
                    }
                } catch (error) {
                    clearInterval(checkInterval);
                    alert('Error: ' + error.message);
                    resetMergerForm();
                }
            }, 1000);
        }
        
        function resetMergerForm() {
            mergerFiles = [];
            mergerFileData = {};
            mergerFileInput.value = '';
            mergerFileText.textContent = 'Drop CSV files here or click to browse (select multiple)';
            
            document.getElementById('merger-file-list').classList.add('hidden');
            document.getElementById('merger-config-section').classList.add('hidden');
            document.getElementById('merger-preview-section').classList.add('hidden');
            document.getElementById('merger-loading').style.display = 'none';
            document.getElementById('merger-success').classList.add('hidden');
            document.getElementById('merger-progress-info').style.display = 'none';
            document.getElementById('merger-progress-bar').style.width = '0%';
            
            // Reset form values
            document.getElementById('merge-type').value = 'vertical';
            document.getElementById('columns-mode').value = 'union';
            document.getElementById('include-source').checked = false;
            document.getElementById('join-columns').innerHTML = '';
            document.getElementById('join-type').value = 'inner';
        }
    </script>
</body>
</html>
'''

@app.route('/')
def index():
    return render_template_string(TEMPLATE)

@app.template_filter('format_number')
def format_number(value):
    return "{:,}".format(value)

@app.template_filter('format_size')
def format_size(size_mb):
    if size_mb > 1024:
        return f"{size_mb/1024:.2f} GB"
    return f"{size_mb:.2f} MB"

@app.route('/debug-db')
def debug_db():
    """Debug database configuration"""
    import sys
    debug_info = f"""
    <h1>Database Debug Info</h1>
    <ul>
        <li>HAS_DB: {HAS_DB}</li>
        <li>DATABASE_URL exists: {'DATABASE_URL' in os.environ}</li>
        <li>DATABASE_URL value: {os.environ.get('DATABASE_URL', 'Not set')[:50]}...</li>
        <li>Python version: {sys.version}</li>
    </ul>
    """
    
    if HAS_DB:
        try:
            count = FileProcess.query.count()
            debug_info += f"<p>✅ Database connection successful. Records: {count}</p>"
        except Exception as e:
            debug_info += f"<p>❌ Database query failed: {str(e)}</p>"
    
    return debug_info

@app.route('/test-db-save')
def test_db_save():
    """Test database saving directly"""
    if not HAS_DB:
        return "Database not configured", 503
    
    try:
        test_record = FileProcess(
            filename="test_manual_save.csv",
            num_parts=3,
            rows_processed=150000,
            processing_time=5.5,
            file_size=25.5
        )
        db.session.add(test_record)
        db.session.commit()
        
        count = FileProcess.query.count()
        return f"""
        <h1>✅ Test Save Successful!</h1>
        <p>Record saved. Total records now: {count}</p>
        <p><a href="/stats">View Stats</a></p>
        """
    except Exception as e:
        return f"""
        <h1>❌ Test Save Failed</h1>
        <p>Error: {str(e)}</p>
        """, 500

@app.route('/init-db')
def init_db_route():
    """Initialize database tables - visit this URL once after deployment"""
    if not HAS_DB:
        return "Database not configured", 503
    
    try:
        with app.app_context():
            db.create_all()
            # Test by creating a sample record
            test_record = FileProcess(
                filename="test_init.csv",
                num_parts=1,
                rows_processed=0,
                processing_time=0,
                file_size=0
            )
            db.session.add(test_record)
            db.session.commit()
            # Delete the test record
            db.session.delete(test_record)
            db.session.commit()
            
        return '''
        <h1>✅ Database Initialized Successfully!</h1>
        <p>Tables have been created. You can now:</p>
        <ul>
            <li><a href="/">Go to the main app</a></li>
            <li><a href="/stats">View statistics</a></li>
        </ul>
        '''
    except Exception as e:
        return f'''
        <h1>❌ Database Initialization Failed</h1>
        <p>Error: {str(e)}</p>
        <p>Make sure DATABASE_URL is set correctly in Railway.</p>
        ''', 500

@app.route('/stats')
def stats():
    if not HAS_DB:
        return "Statistics not available without database", 503
    stats_template = '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>CSV Splitter Stats</title>
        <style>
            body {
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                background-color: #f0f0ff;
                margin: 0;
                padding: 40px;
            }
            .container {
                max-width: 800px;
                margin: 0 auto;
            }
            .card {
                background: white;
                border-radius: 16px;
                padding: 40px;
                box-shadow: 0 4px 12px rgba(0,0,0,0.1);
                margin-top: 20px;
            }
            .stat-number {
                font-size: 48px;
                color: #4ecdc4;
                margin: 10px 0;
            }
            .recent-list {
                margin-top: 20px;
            }
            .recent-item {
                padding: 10px;
                border-bottom: 1px solid #eee;
            }
            .stats-grid {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                gap: 20px;
                margin-bottom: 30px;
            }
            .stat-card {
                background: #f8f9fa;
                padding: 20px;
                border-radius: 8px;
                text-align: center;
            }
            .back-link {
                display: inline-block;
                margin-top: 20px;
                color: #666;
                text-decoration: none;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="card">
                <h1>Usage Statistics</h1>
                
                <div class="stats-grid">
                    <div class="stat-card">
                        <div class="stat-number">{{ total_files }}</div>
                        <p>Files Processed</p>
                    </div>
                    <div class="stat-card">
                        <div class="stat-number">{{ total_rows | format_number }}</div>
                        <p>Total Rows Processed</p>
                    </div>
                    <div class="stat-card">
                        <div class="stat-number">{{ total_size | format_size }}</div>
                        <p>Total Data Processed</p>
                    </div>
                </div>
                
                <h2>Recent Files</h2>
                <div class="recent-list">
                    {% for file in recent_files %}
                    <div class="recent-item">
                        <strong>{{ file.filename }}</strong>
                        <br>
                        <small>
                            {{ file.formatted_timestamp }} - 
                            Split into {{ file.num_parts }} parts - 
                            {{ file.formatted_size }} - 
                            {{ file.rows_processed | format_number }} rows
                        </small>
                    </div>
                    {% endfor %}
                </div>
                
                <a href="/" class="back-link">← Back to Splitter</a>
            </div>
        </div>
    </body>
    </html>
    '''
    
    # Get statistics from database
    try:
        recent_files = FileProcess.query.order_by(FileProcess.timestamp.desc()).limit(10).all()
        total_files = FileProcess.query.count()
        total_rows = db.session.query(db.func.sum(FileProcess.rows_processed)).scalar() or 0
        total_size = db.session.query(db.func.sum(FileProcess.file_size)).scalar() or 0
    except Exception as e:
        print(f"Could not get stats: {e}")
        return "Statistics temporarily unavailable", 503
    
    return render_template_string(
        stats_template,
        recent_files=recent_files,
        total_files=total_files,
        total_rows=total_rows,
        total_size=total_size
    )

@app.route('/progress/<task_id>', methods=['GET'])
def get_progress(task_id):
    status = app.processing_status.get(task_id, {'status': 'not_found'})
    return jsonify(status)

@app.route('/split', methods=['POST'])
def split_csv_endpoint():
    print(f"=== Starting file upload processing ===")
    print(f"HAS_DB at start: {HAS_DB}")
    
    # For chunked processing of large files
    CHUNK_SIZE = 10000  # Process 10k rows at a time
    if 'file' not in request.files:
        return 'No file uploaded', 400
    
    file = request.files['file']
    if file.filename == '':
        return 'No file selected', 400
    
    try:
        max_rows = int(request.form.get('max_rows', 50000))
    except ValueError:
        return 'Invalid max rows value', 400

    temp_dir = 'temp_split_files'
    os.makedirs(temp_dir, exist_ok=True)
    
    # List of encodings to try
    encodings = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252']
    
    # Generate task ID for progress tracking
    task_id = str(uuid.uuid4())
    
    # Initialize progress tracking
    app.processing_status[task_id] = {
        'status': 'processing',
        'progress': 0,
        'message': 'Starting file processing...'
    }
    
    try:
        start_time = time.time()
        file_size = len(file.read()) / (1024 * 1024)  # Size in MB
        file.seek(0)  # Reset file pointer
        
        # Return task ID immediately for large files
        if file_size > 50:
            # Save file temporarily
            temp_upload = f'temp_upload_{task_id}.csv'
            file.save(temp_upload)
            
            # Process in background thread
            thread = threading.Thread(
                target=process_large_file_async,
                args=(temp_upload, max_rows, task_id, file.filename, encodings)
            )
            thread.start()
            
            return jsonify({
                'task_id': task_id,
                'message': 'Processing large file in background'
            }), 202
        
        # Try different encodings
        for encoding in encodings:
            try:
                file.seek(0)  # Reset file pointer for each attempt
                # Read the first row to check if it's a table name
                first_row = pd.read_csv(file, nrows=1, encoding=encoding)
                file.seek(0)  # Reset file pointer
                
                # Check if first row is likely a table name
                if len(first_row.columns) == 1:
                    table_name = first_row.iloc[0, 0]
                    # For large files, read in chunks
                    if file_size > 50:  # If file is larger than 50MB
                        chunks = []
                        for chunk in pd.read_csv(file, header=1, encoding=encoding, chunksize=CHUNK_SIZE):
                            chunks.append(chunk)
                        df = pd.concat(chunks, ignore_index=True)
                    else:
                        df = pd.read_csv(file, header=1, encoding=encoding)
                else:
                    # For large files, read in chunks
                    if file_size > 50:  # If file is larger than 50MB
                        chunks = []
                        for chunk in pd.read_csv(file, encoding=encoding, chunksize=CHUNK_SIZE):
                            chunks.append(chunk)
                        df = pd.concat(chunks, ignore_index=True)
                    else:
                        df = pd.read_csv(file, encoding=encoding)
                    table_name = None
                
                break  # If successful, break the encoding loop
            except UnicodeDecodeError:
                continue
        else:
            # If no encoding worked
            raise UnicodeDecodeError(f"Could not decode file with any of these encodings: {', '.join(encodings)}")
        
        # Rest of the processing remains the same
        total_rows = len(df)
        num_files = math.ceil(total_rows / max_rows)
        
        print(f"File processed: {file.filename}, rows: {total_rows}, parts: {num_files}")
        
        # Track this file processing
        app.total_splits += 1
        app.processed_files.append({
            'filename': secure_filename(file.filename),
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'num_parts': num_files
        })
        # Keep only last 100 files in memory
        if len(app.processed_files) > 100:
            app.processed_files = app.processed_files[-100:]
        
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for i in range(num_files):
                # Update progress
                progress = int((i / num_files) * 100)
                app.processing_status[task_id] = {
                    'status': 'processing',
                    'progress': progress,
                    'message': f'Processing part {i + 1} of {num_files}'
                }
                start_idx = i * max_rows
                end_idx = min((i + 1) * max_rows, total_rows)
                
                output_file = f"part_{i + 1}_of_{num_files}.csv"
                output_path = os.path.join(temp_dir, output_file)
                
                # Write chunks to avoid memory issues
                if table_name:
                    with open(output_path, 'w', encoding=encoding, newline='') as f:
                        f.write(f"{table_name}\n")
                
                # Process in smaller chunks for writing
                write_header = True
                for j in range(start_idx, end_idx, CHUNK_SIZE):
                    chunk_end = min(j + CHUNK_SIZE, end_idx)
                    chunk_df = df.iloc[j:chunk_end]
                    
                    if table_name:
                        chunk_df.to_csv(output_path, index=False, mode='a', encoding=encoding, header=write_header)
                    else:
                        mode = 'w' if j == start_idx else 'a'
                        chunk_df.to_csv(output_path, index=False, mode=mode, encoding=encoding, header=write_header)
                    
                    write_header = False
                
                zip_file.write(output_path, output_file)
                # Remove the file immediately after adding to zip to save disk space
                os.remove(output_path)
        
        zip_buffer.seek(0)
        
        # Create database record if available
        print(f"About to save to database. HAS_DB={HAS_DB}, filename={file.filename}")
        if HAS_DB:
            try:
                process_record = FileProcess(
                    filename=secure_filename(file.filename),
                    num_parts=num_files,
                    rows_processed=total_rows,
                    processing_time=time.time() - start_time,
                    file_size=file_size
                )
                db.session.add(process_record)
                db.session.commit()
                print(f"Successfully saved to database: {secure_filename(file.filename)}")
                app.logger.info(f"Database save successful: {secure_filename(file.filename)}")
            except Exception as e:
                print(f"Could not save to database: {e}")
                app.logger.error(f"Database save failed: {e}")
        else:
            print(f"No database available (HAS_DB={HAS_DB})")
        
        # Send notification
        threading.Thread(
            target=send_notification,
            args=(secure_filename(file.filename), num_files, total_rows, file_size)
        ).start()
        
        # Mark as complete
        app.processing_status[task_id] = {
            'status': 'complete',
            'progress': 100,
            'message': 'Processing complete'
        }
        
        return send_file(
            zip_buffer,
            mimetype='application/zip',
            as_attachment=True,
            download_name=f'split_{secure_filename(file.filename.replace(".csv", ""))}.zip'
        )
    
    except Exception as e:
        print(f"ERROR in split_csv_endpoint: {str(e)}")
        print(f"Error type: {type(e)}")
        import traceback
        traceback.print_exc()
        return f'Error processing file: {str(e)}', 500
    
    finally:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

def process_large_file_async(temp_upload, max_rows, task_id, original_filename, encodings):
    """Process large files asynchronously"""
    print(f"=== Starting async processing for {original_filename} ===")
    print(f"HAS_DB in async: {HAS_DB}")
    temp_dir = f'temp_split_files_{task_id}'
    os.makedirs(temp_dir, exist_ok=True)
    
    try:
        start_time = time.time()  # Track processing time
        # Get file size
        file_size = os.path.getsize(temp_upload) / (1024 * 1024)  # in MB
        print(f"File size: {file_size:.2f} MB")
        # Try different encodings
        for encoding in encodings:
            try:
                # Check for table name
                with open(temp_upload, 'r', encoding=encoding) as f:
                    first_line = f.readline().strip()
                    f.seek(0)
                    first_row = pd.read_csv(f, nrows=1, encoding=encoding)
                    
                    if len(first_row.columns) == 1:
                        table_name = first_row.iloc[0, 0]
                        df = pd.read_csv(temp_upload, header=1, encoding=encoding)
                    else:
                        df = pd.read_csv(temp_upload, encoding=encoding)
                        table_name = None
                break
            except UnicodeDecodeError:
                continue
        else:
            raise UnicodeDecodeError(f"Could not decode file with any of these encodings: {', '.join(encodings)}")
        
        total_rows = len(df)
        num_files = math.ceil(total_rows / max_rows)
        
        # Create zip file
        zip_path = f'temp_result_{task_id}.zip'
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for i in range(num_files):
                # Update progress
                progress = int((i / num_files) * 100)
                app.processing_status[task_id] = {
                    'status': 'processing',
                    'progress': progress,
                    'message': f'Processing part {i + 1} of {num_files}'
                }
                
                start_idx = i * max_rows
                end_idx = min((i + 1) * max_rows, total_rows)
                
                output_file = f"part_{i + 1}_of_{num_files}.csv"
                output_path = os.path.join(temp_dir, output_file)
                
                # Write with table name if exists
                if table_name:
                    with open(output_path, 'w', encoding=encoding, newline='') as f:
                        f.write(f"{table_name}\n")
                
                # Write data in chunks
                CHUNK_SIZE = 10000
                write_header = True
                for j in range(start_idx, end_idx, CHUNK_SIZE):
                    chunk_end = min(j + CHUNK_SIZE, end_idx)
                    chunk_df = df.iloc[j:chunk_end]
                    
                    if table_name:
                        chunk_df.to_csv(output_path, index=False, mode='a', encoding=encoding, header=write_header)
                    else:
                        mode = 'w' if j == start_idx else 'a'
                        chunk_df.to_csv(output_path, index=False, mode=mode, encoding=encoding, header=write_header)
                    
                    write_header = False
                
                zip_file.write(output_path, output_file)
                os.remove(output_path)
        
        # Update status with download link
        app.processing_status[task_id] = {
            'status': 'complete',
            'progress': 100,
            'message': 'Processing complete',
            'download_file': zip_path,
            'original_filename': original_filename
        }
        
        # Track in database if available
        print(f"About to save async file to database. HAS_DB={HAS_DB}")
        if HAS_DB:
            try:
                # Need app context for database operations in thread
                with app.app_context():
                    process_record = FileProcess(
                        filename=secure_filename(original_filename),
                        num_parts=num_files,
                        rows_processed=total_rows,
                        processing_time=time.time() - start_time,
                        file_size=file_size
                    )
                    db.session.add(process_record)
                    db.session.commit()
                    print(f"Successfully saved async file to database: {original_filename}")
            except Exception as e:
                print(f"Could not save to database: {e}")
                import traceback
                traceback.print_exc()
        else:
            print(f"No database available in async (HAS_DB={HAS_DB})")
        
        # Send notification for async processing
        threading.Thread(
            target=send_notification,
            args=(secure_filename(original_filename), num_files, total_rows, file_size)
        ).start()
        
    except Exception as e:
        app.processing_status[task_id] = {
            'status': 'error',
            'progress': 0,
            'message': f'Error: {str(e)}'
        }
    
    finally:
        # Cleanup
        if os.path.exists(temp_upload):
            os.remove(temp_upload)
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

@app.route('/download/<task_id>', methods=['GET'])
def download_result(task_id):
    """Download the processed file for async tasks"""
    status = app.processing_status.get(task_id)
    
    if not status or status['status'] != 'complete':
        return 'File not ready or not found', 404
    
    zip_path = status.get('download_file')
    original_filename = status.get('original_filename', 'file.csv')
    
    if not os.path.exists(zip_path):
        return 'File not found', 404
    
    # Clean up status after download
    def cleanup():
        time.sleep(60)  # Keep file for 1 minute after download
        if os.path.exists(zip_path):
            os.remove(zip_path)
        if task_id in app.processing_status:
            del app.processing_status[task_id]
    
    threading.Thread(target=cleanup).start()
    
    return send_file(
        zip_path,
        mimetype='application/zip',
        as_attachment=True,
        download_name=f'split_{secure_filename(original_filename.replace(".csv", ""))}.zip'
    )

# Duplicate Remover Routes
@app.route('/analyze-csv', methods=['POST'])
def analyze_csv():
    """Analyze CSV file and return columns and metadata"""
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    # Save file temporarily
    temp_filename = f'temp_analyze_{uuid.uuid4()}.csv'
    file.save(temp_filename)
    
    try:
        remover = DuplicateRemover(temp_filename)
        analysis = remover.analyze_file()
        
        return jsonify(analysis)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
    finally:
        # Clean up temp file
        if os.path.exists(temp_filename):
            os.remove(temp_filename)

@app.route('/preview-duplicates', methods=['POST'])
def preview_duplicates():
    """Preview duplicates based on selected columns"""
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    
    file = request.files['file']
    columns = json.loads(request.form.get('columns', '[]'))
    
    if not columns:
        return jsonify({'error': 'No columns selected'}), 400
    
    # Save file temporarily
    temp_filename = f'temp_preview_{uuid.uuid4()}.csv'
    file.save(temp_filename)
    
    try:
        remover = DuplicateRemover(temp_filename)
        remover.load_file()
        preview_data = remover.find_duplicates(columns)
        
        return jsonify(preview_data)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
    finally:
        # Clean up temp file
        if os.path.exists(temp_filename):
            os.remove(temp_filename)

@app.route('/process-duplicates', methods=['POST'])
def process_duplicates():
    """Process file and remove duplicates"""
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    
    file = request.files['file']
    columns = json.loads(request.form.get('columns', '[]'))
    keep_strategy = request.form.get('keep_strategy', 'first')
    strategy_column = request.form.get('strategy_column', None)
    
    if not columns:
        return jsonify({'error': 'No columns selected'}), 400
    
    # Save file temporarily
    temp_filename = f'temp_process_{uuid.uuid4()}.csv'
    output_filename = f'cleaned_{uuid.uuid4()}.csv'
    file.save(temp_filename)
    
    try:
        start_time = time.time()
        file_size = os.path.getsize(temp_filename) / (1024 * 1024)  # MB
        
        remover = DuplicateRemover(temp_filename)
        remover.load_file()
        
        # Remove duplicates
        result = remover.remove_duplicates(columns, keep_strategy, strategy_column)
        
        # Save cleaned file
        remover.save_cleaned_file(result['cleaned_df'], output_filename)
        
        # Save to database if available
        if HAS_DB:
            try:
                with app.app_context():
                    removal_record = DuplicateRemoval(
                        filename=secure_filename(file.filename),
                        original_rows=result['original_rows'],
                        duplicates_removed=result['rows_removed'],
                        check_columns=json.dumps(columns),
                        keep_strategy=keep_strategy,
                        strategy_column=strategy_column,
                        processing_time=time.time() - start_time,
                        file_size=file_size
                    )
                    db.session.add(removal_record)
                    db.session.commit()
            except Exception as e:
                print(f"Could not save to database: {e}")
        
        # Create response with stats in header
        response = send_file(
            output_filename,
            mimetype='text/csv',
            as_attachment=True,
            download_name=f'cleaned_{secure_filename(file.filename)}'
        )
        
        response.headers['X-Process-Stats'] = json.dumps({
            'original_rows': result['original_rows'],
            'cleaned_rows': result['cleaned_rows'],
            'rows_removed': result['rows_removed']
        })
        
        # Clean up after sending
        def cleanup():
            time.sleep(1)
            if os.path.exists(output_filename):
                os.remove(output_filename)
        
        threading.Thread(target=cleanup).start()
        
        return response
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
    finally:
        # Clean up temp file
        if os.path.exists(temp_filename):
            os.remove(temp_filename)

# CSV Merger Routes
@app.route('/analyze-merge-files', methods=['POST'])
def analyze_merge_files():
    """Analyze multiple CSV files for merging"""
    files = []
    file_count = 0
    
    # Collect all uploaded files
    for key in request.files:
        if key.startswith('file_'):
            files.append(request.files[key])
            file_count += 1
    
    if file_count < 2:
        return jsonify({'error': 'At least 2 files required for merging'}), 400
    
    merger = CSVMerger()
    temp_files = []
    
    try:
        # Save files temporarily and analyze
        for idx, file in enumerate(files):
            temp_filename = f'temp_merge_{uuid.uuid4()}_{idx}.csv'
            file.save(temp_filename)
            temp_files.append(temp_filename)
            merger.add_file(temp_filename)
        
        analysis = merger.analyze_files()
        
        # Add common columns for horizontal merge
        if 'column_analysis' in analysis:
            analysis['common_columns'] = analysis['column_analysis']['common_columns']
        
        return jsonify(analysis)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
    finally:
        # Clean up temp files
        for temp_file in temp_files:
            if os.path.exists(temp_file):
                os.remove(temp_file)

@app.route('/preview-merge', methods=['POST'])
def preview_merge():
    """Preview the merge operation"""
    files = []
    
    # Collect all uploaded files
    for key in request.files:
        if key.startswith('file_'):
            files.append(request.files[key])
    
    if len(files) < 2:
        return jsonify({'error': 'At least 2 files required for merging'}), 400
    
    merge_type = request.form.get('merge_type', 'vertical')
    
    # Build options
    options = {}
    if merge_type == 'vertical':
        options['columns_mode'] = request.form.get('columns_mode', 'union')
        options['include_source'] = request.form.get('include_source') == 'true'
    else:
        options['join_columns'] = json.loads(request.form.get('join_columns', '[]'))
        options['join_type'] = request.form.get('join_type', 'inner')
    
    merger = CSVMerger()
    temp_files = []
    
    try:
        # Save files temporarily
        for idx, file in enumerate(files):
            temp_filename = f'temp_preview_{uuid.uuid4()}_{idx}.csv'
            file.save(temp_filename)
            temp_files.append(temp_filename)
            merger.add_file(temp_filename)
        
        # Generate preview
        preview_result = merger.preview_merge(merge_type, options)
        
        return jsonify(preview_result)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
    finally:
        # Clean up temp files
        for temp_file in temp_files:
            if os.path.exists(temp_file):
                os.remove(temp_file)

@app.route('/process-merge', methods=['POST'])
def process_merge():
    """Process the merge operation"""
    files = []
    file_names = []
    
    # Collect all uploaded files
    for key in request.files:
        if key.startswith('file_'):
            file = request.files[key]
            files.append(file)
            file_names.append(secure_filename(file.filename))
    
    if len(files) < 2:
        return jsonify({'error': 'At least 2 files required for merging'}), 400
    
    merge_type = request.form.get('merge_type', 'vertical')
    
    # Build options
    options = {}
    if merge_type == 'vertical':
        options['columns_mode'] = request.form.get('columns_mode', 'union')
        options['include_source'] = request.form.get('include_source') == 'true'
    else:
        options['join_columns'] = json.loads(request.form.get('join_columns', '[]'))
        options['join_type'] = request.form.get('join_type', 'inner')
    
    # Generate task ID for large files
    task_id = str(uuid.uuid4())
    
    # Check combined file size
    total_size_mb = sum(len(f.read()) / (1024 * 1024) for f in files)
    for f in files:
        f.seek(0)  # Reset file pointers
    
    # For large files, process asynchronously
    if total_size_mb > 50:
        # Save files temporarily
        temp_files = []
        for idx, file in enumerate(files):
            temp_filename = f'temp_merge_{task_id}_{idx}.csv'
            file.save(temp_filename)
            temp_files.append(temp_filename)
        
        # Initialize progress tracking
        app.processing_status[task_id] = {
            'status': 'processing',
            'progress': 0,
            'message': 'Starting merge operation...'
        }
        
        # Process in background thread
        thread = threading.Thread(
            target=process_merge_async,
            args=(temp_files, file_names, merge_type, options, task_id, total_size_mb)
        )
        thread.start()
        
        return jsonify({
            'task_id': task_id,
            'message': 'Processing large files in background'
        }), 202
    
    # Process synchronously for smaller files
    merger = CSVMerger()
    temp_files = []
    
    try:
        start_time = time.time()
        
        # Save files temporarily
        for idx, file in enumerate(files):
            temp_filename = f'temp_merge_sync_{uuid.uuid4()}_{idx}.csv'
            file.save(temp_filename)
            temp_files.append(temp_filename)
            merger.add_file(temp_filename)
        
        # Execute merge
        output_filename = f'merged_{uuid.uuid4()}.csv'
        result = merger.execute_merge(merge_type, options, output_filename)
        
        if result.get('error'):
            return jsonify(result), 500
        
        # Save to database if available
        if HAS_DB:
            try:
                with app.app_context():
                    merge_record = MergeOperation(
                        files_merged=len(files),
                        file_names=json.dumps(file_names),
                        merge_type=merge_type,
                        merge_options=json.dumps(options),
                        total_input_rows=merger.total_rows,
                        total_output_rows=result['rows'],
                        total_columns=result['columns'],
                        processing_time=time.time() - start_time,
                        total_size_mb=total_size_mb
                    )
                    db.session.add(merge_record)
                    db.session.commit()
            except Exception as e:
                print(f"Could not save to database: {e}")
        
        # Create response
        response = send_file(
            output_filename,
            mimetype='text/csv',
            as_attachment=True,
            download_name=f'merged_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        )
        
        response.headers['X-Merge-Stats'] = json.dumps({
            'files_merged': len(files),
            'total_rows': result['rows'],
            'total_columns': result['columns']
        })
        
        # Clean up after sending
        def cleanup():
            time.sleep(1)
            if os.path.exists(output_filename):
                os.remove(output_filename)
        
        threading.Thread(target=cleanup).start()
        
        return response
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
    finally:
        # Clean up temp files
        for temp_file in temp_files:
            if os.path.exists(temp_file):
                os.remove(temp_file)

def process_merge_async(temp_files, file_names, merge_type, options, task_id, total_size_mb):
    """Process large merge operations asynchronously"""
    print(f"=== Starting async merge for {len(temp_files)} files ===")
    
    try:
        start_time = time.time()
        merger = CSVMerger()
        
        # Add files to merger
        for idx, temp_file in enumerate(temp_files):
            progress = int((idx / len(temp_files)) * 30)  # 30% for loading files
            app.processing_status[task_id] = {
                'status': 'processing',
                'progress': progress,
                'message': f'Loading file {idx + 1} of {len(temp_files)}'
            }
            merger.add_file(temp_file)
        
        # Update progress
        app.processing_status[task_id] = {
            'status': 'processing',
            'progress': 40,
            'message': 'Analyzing files...'
        }
        
        # Execute merge
        output_path = f'temp_result_{task_id}.csv'
        
        # Update progress
        app.processing_status[task_id] = {
            'status': 'processing',
            'progress': 50,
            'message': 'Merging files...'
        }
        
        result = merger.execute_merge(merge_type, options, output_path)
        
        if result.get('error'):
            raise Exception(result['error'])
        
        # Update progress
        app.processing_status[task_id] = {
            'status': 'processing',
            'progress': 90,
            'message': 'Finalizing...'
        }
        
        # Save to database if available
        if HAS_DB:
            try:
                with app.app_context():
                    merge_record = MergeOperation(
                        files_merged=len(temp_files),
                        file_names=json.dumps(file_names),
                        merge_type=merge_type,
                        merge_options=json.dumps(options),
                        total_input_rows=merger.total_rows,
                        total_output_rows=result['rows'],
                        total_columns=result['columns'],
                        processing_time=time.time() - start_time,
                        total_size_mb=total_size_mb
                    )
                    db.session.add(merge_record)
                    db.session.commit()
            except Exception as e:
                print(f"Could not save to database: {e}")
        
        # Update status with completion
        app.processing_status[task_id] = {
            'status': 'complete',
            'progress': 100,
            'message': 'Merge complete',
            'download_file': output_path,
            'stats': {
                'files_merged': len(temp_files),
                'total_rows': result['rows'],
                'total_columns': result['columns']
            }
        }
        
    except Exception as e:
        app.processing_status[task_id] = {
            'status': 'error',
            'progress': 0,
            'message': f'Error: {str(e)}'
        }
    
    finally:
        # Cleanup temp files
        for temp_file in temp_files:
            if os.path.exists(temp_file):
                os.remove(temp_file)

@app.route('/merge-progress/<task_id>', methods=['GET'])
def get_merge_progress(task_id):
    """Get merge operation progress"""
    status = app.processing_status.get(task_id, {'status': 'not_found'})
    return jsonify(status)

@app.route('/download-merge/<task_id>', methods=['GET'])
def download_merge_result(task_id):
    """Download the merged file for async tasks"""
    status = app.processing_status.get(task_id)
    
    if not status or status['status'] != 'complete':
        return 'File not ready or not found', 404
    
    output_path = status.get('download_file')
    
    if not os.path.exists(output_path):
        return 'File not found', 404
    
    # Clean up status after download
    def cleanup():
        time.sleep(60)  # Keep file for 1 minute after download
        if os.path.exists(output_path):
            os.remove(output_path)
        if task_id in app.processing_status:
            del app.processing_status[task_id]
    
    threading.Thread(target=cleanup).start()
    
    return send_file(
        output_path,
        mimetype='text/csv',
        as_attachment=True,
        download_name=f'merged_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    )

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)
