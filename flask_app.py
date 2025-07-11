from flask import Flask, request, send_file, render_template_string, jsonify
from models import db, FileProcess
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

app = Flask(__name__)

# Database configuration
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL', 'sqlite:///csv_splitter.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Initialize database
db.init_app(app)

# Create tables
with app.app_context():
    db.create_all()

# Add these after the Flask app initialization
app.processed_files = []
app.total_splits = 0
# Progress tracking for large files
app.processing_status = {}

TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>CSV Splitter</title>
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
    </style>
</head>
<body>
    <div class="container">
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
        
        <!-- <div style="margin-top: 20px; text-align: center;">
            <a href="/stats" style="color: #666; text-decoration: none;">View Usage Statistics</a>
        </div> -->
    </div>

    <script>
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

@app.route('/stats')
def stats():
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
    recent_files = FileProcess.query.order_by(FileProcess.timestamp.desc()).limit(10).all()
    total_files = FileProcess.query.count()
    total_rows = db.session.query(db.func.sum(FileProcess.rows_processed)).scalar() or 0
    total_size = db.session.query(db.func.sum(FileProcess.file_size)).scalar() or 0
    
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
        
        # Create database record
        process_record = FileProcess(
            filename=secure_filename(file.filename),
            num_parts=num_files,
            rows_processed=total_rows,
            processing_time=time.time() - start_time,
            file_size=file_size
        )
        db.session.add(process_record)
        db.session.commit()
        
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
        print(f"Error: {str(e)}")  # For debugging
        return f'Error processing file: {str(e)}', 500
    
    finally:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

def process_large_file_async(temp_upload, max_rows, task_id, original_filename, encodings):
    """Process large files asynchronously"""
    temp_dir = f'temp_split_files_{task_id}'
    os.makedirs(temp_dir, exist_ok=True)
    
    try:
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
        
        # Track in database
        process_record = FileProcess(
            filename=secure_filename(original_filename),
            num_parts=num_files,
            rows_processed=total_rows,
            processing_time=time.time() - time.time(),
            file_size=os.path.getsize(temp_upload) / (1024 * 1024)
        )
        db.session.add(process_record)
        db.session.commit()
        
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

if __name__ == '__main__':
    app.run(debug=True, port=5001)
