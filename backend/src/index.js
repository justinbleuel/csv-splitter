const express = require('express');
const cors = require('cors');
const multer = require('multer');
const path = require('path');
require('dotenv').config();

const app = express();
const port = process.env.PORT || 8080;

// Configure storage
const storage = multer.diskStorage({
  destination: function (req, file, cb) {
    console.log('Storage destination called for file:', file);
    cb(null, 'uploads/');
  },
  filename: function (req, file, cb) {
    console.log('Storage filename called for file:', file);
    const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1E9);
    cb(null, file.fieldname + '-' + uniqueSuffix + path.extname(file.originalname));
  }
});

// Configure multer
const upload = multer({
  storage: storage,
  limits: {
    fileSize: 10 * 1024 * 1024 // 10 MB
  }
}).single('audio');

// Configure CORS
app.use(cors());

// Endpoint to handle file upload
app.post('/api/summarize', function(req, res) {
  console.log('Received request to /api/summarize');
  console.log('Content-Type:', req.headers['content-type']);
  
  upload(req, res, function(err) {
    if (err instanceof multer.MulterError) {
      // A Multer error occurred when uploading.
      console.error('Multer error:', err);
      return res.status(400).json({ error: `Upload error: ${err.message}` });
    } else if (err) {
      // An unknown error occurred when uploading.
      console.error('Unknown error:', err);
      return res.status(500).json({ error: err.message });
    }
    
    // Everything went fine.
    console.log('Upload completed. File:', req.file);
    
    if (!req.file) {
      console.log('No file in request:', req.body);
      return res.status(400).json({ error: 'No audio file uploaded' });
    }

    // Process the uploaded file
    res.json({
      status: 'success',
      message: 'File uploaded successfully',
      fileInfo: {
        filename: req.file.filename,
        originalname: req.file.originalname,
        size: req.file.size,
        mimetype: req.file.mimetype
      },
      summary: 'Test summary - file uploaded successfully'
    });
  });
});

app.listen(port, '0.0.0.0', () => {
  console.log(`Server running on port ${port}`);
});