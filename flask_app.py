from flask import Flask, request, send_file, render_template_string
import pandas as pd
import math
import os
import io
import zipfile
import shutil
import csv
from werkzeug.utils import secure_filename

app = Flask(__name__)

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

                const blob = await response.blob();
                const downloadUrl = window.URL.createObjectURL(blob);
                
                const downloadBtn = document.getElementById('download-btn');
                downloadBtn.href = downloadUrl;
                downloadBtn.download = 'split_csv_files.zip';
                
                showSuccess();
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

        function resetForm() {
            document.getElementById('upload-form').reset();
            fileName.textContent = 'Drop your CSV file here or click to browse';
            document.getElementById('upload-form').style.display = 'block';
            document.getElementById('success-container').style.display = 'none';
            document.getElementById('loading').style.display = 'none';
        }
    </script>
</body>
</html>
'''

@app.route('/')
def index():
    return render_template_string(TEMPLATE)

@app.route('/split', methods=['POST'])
def split_csv_endpoint():
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
    
    try:
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
                    df = pd.read_csv(file, header=1, encoding=encoding)
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
        
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for i in range(num_files):
                start_idx = i * max_rows
                end_idx = min((i + 1) * max_rows, total_rows)
                chunk_df = df.iloc[start_idx:end_idx].copy()
                
                output_file = f"part_{i + 1}_of_{num_files}.csv"
                output_path = os.path.join(temp_dir, output_file)
                
                if table_name:
                    with open(output_path, 'w', encoding=encoding, newline='') as f:
                        f.write(f"{table_name}\n")
                    chunk_df.to_csv(output_path, index=False, mode='a', encoding=encoding)
                else:
                    chunk_df.to_csv(output_path, index=False, encoding=encoding)
                
                zip_file.write(output_path, output_file)
        
        zip_buffer.seek(0)
        
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

if __name__ == '__main__':
    app.run(debug=True, port=5001)
