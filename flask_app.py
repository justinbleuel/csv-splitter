from flask import Flask, request, send_file, render_template_string
import pandas as pd
import math
import os
import io
import zipfile
import shutil
from werkzeug.utils import secure_filename

app = Flask(__name__)

# HTML template for the upload form
HTML_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>CSV Splitter</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
            text-align: center;
        }
        .container {
            background-color: #f9f9f9;
            border-radius: 8px;
            padding: 20px;
            margin-top: 20px;
        }
        .upload-form {
            margin: 20px 0;
        }
        .file-input {
            margin: 10px 0;
        }
        .submit-btn {
            background-color: #4CAF50;
            color: white;
            padding: 10px 20px;
            border: none;
            border-radius: 4px;
            cursor: pointer;
        }
        .submit-btn:hover {
            background-color: #45a049;
        }
        .row-input {
            margin: 10px 0;
            padding: 5px;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>CSV File Splitter</h1>
        <p>Upload a CSV file to split it into smaller files</p>
        <form class="upload-form" action="/split" method="post" enctype="multipart/form-data">
            <div class="file-input">
                <input type="file" name="file" accept=".csv" required>
            </div>
            <div>
                <input type="number" name="max_rows" class="row-input" 
                       value="50000" min="1" max="1000000" 
                       placeholder="Max rows per file">
            </div>
            <input type="submit" value="Split CSV" class="submit-btn">
        </form>
    </div>
</body>
</html>
'''

def split_csv(input_file, max_rows=50000):
    """
    Split a CSV file into multiple files and create a zip archive.
    
    Args:
        input_file: File object of the uploaded CSV
        max_rows (int): Maximum number of rows per output file
    
    Returns:
        BytesIO object containing the zip file
    """
    # Create a temporary directory to store split files
    temp_dir = 'temp_split_files'
    os.makedirs(temp_dir, exist_ok=True)
    
    try:
        # Read the input CSV file
        df = pd.read_csv(input_file)
        
        # Calculate the number of files needed
        total_rows = len(df)
        num_files = math.ceil(total_rows / max_rows)
        
        # Create a BytesIO object to store the zip file
        zip_buffer = io.BytesIO()
        
        # Create a ZIP file containing all split CSVs
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for i in range(num_files):
                start_idx = i * max_rows
                end_idx = min((i + 1) * max_rows, total_rows)
                
                # Create the output filename
                output_file = f"part_{i + 1}.csv"
                output_path = os.path.join(temp_dir, output_file)
                
                # Save the slice of the dataframe to a new CSV file
                df[start_idx:end_idx].to_csv(output_path, index=False)
                
                # Add the file to the ZIP archive
                zip_file.write(output_path, output_file)
        
        # Seek to the beginning of the BytesIO buffer
        zip_buffer.seek(0)
        return zip_buffer
    
    finally:
        # Clean up temporary directory
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

@app.route('/')
def index():
    """Render the upload form"""
    return render_template_string(HTML_TEMPLATE)

@app.route('/split', methods=['POST'])
def split_csv_endpoint():
    """Handle file upload and return zip file"""
    if 'file' not in request.files:
        return 'No file uploaded', 400
    
    file = request.files['file']
    if file.filename == '':
        return 'No file selected', 400
    
    if not file.filename.endswith('.csv'):
        return 'Please upload a CSV file', 400
    
    try:
        max_rows = int(request.form.get('max_rows', 50000))
    except ValueError:
        return 'Invalid max rows value', 400
    
    if max_rows < 1:
        return 'Max rows must be positive', 400
    
    try:
        # Generate the zip file
        zip_buffer = split_csv(file, max_rows)
        
        # Create the response
        return send_file(
            zip_buffer,
            mimetype='application/zip',
            as_attachment=True,
            download_name='split_csv_files.zip'
        )
    
    except Exception as e:
        return f'Error processing file: {str(e)}', 500

if __name__ == '__main__':
    app.run(debug=True)