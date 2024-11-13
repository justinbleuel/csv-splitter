const express = require('express');
const cors = require('cors');
const multer = require('multer');
const path = require('path');
require('dotenv').config();

const app = express();
const port = process.env.PORT || 8080;

// Configure storage
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    cb(null, 'uploads/');
  },
  filename: (req, file, cb) => {
    const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1E9);
    cb(null, file.fieldname + '-' + uniqueSuffix + path.extname(file.originalname));
  }
});

const upload = multer({ 
  storage: storage,
  limits: {
    fileSize: 10 * 1024 * 1024 // 10MB limit
  }
}).single('audio');

// Detailed CORS configuration
app.use(cors({
  origin: '*',
  methods: ['GET', 'POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Accept'],
  credentials: true,
}));

// Create uploads directory if it doesn't exist
const fs = require('fs');
if (!fs.existsSync('uploads')) {
  fs.mkdirSync('uploads');
}

// Test route
app.get('/test', (req, res) => {
  res.json({ message: 'Server is working!' });
});

// Upload endpoint with detailed logging
app.post('/api/summarize', (req, res) => {
  console.log('Received upload request');
  console.log('Headers:', req.headers);
  
  upload(req, res, function(err) {
    console.log('Processing upload...');
    
    if (err) {
      console.error('Upload error:', err);
      return res.status(400).json({
        error: err.message,
        details: err.stack
      });
    }

    if (!req.file) {
      console.log('No file in request');
      return res.status(400).json({
        error: 'No file uploaded',
        body: req.body
      });
    }

    console.log('File received:', req.file);

    // Send success response
    res.json({
      status: 'success',
      message: 'File uploaded successfully',
      file: {
        name: req.file.originalname,
        size: req.file.size,
        mimetype: req.file.mimetype
      }
    });
  });
});

// Error handling middleware
app.use((err, req, res, next) => {
  console.error('Global error handler:', err);
  res.status(500).json({
    error: 'Server error',
    message: err.message,
    stack: process.env.NODE_ENV === 'development' ? err.stack : undefined
  });
});

app.listen(port, '0.0.0.0', () => {
  console.log(`Server running on port ${port}`);
});