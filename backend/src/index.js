const express = require('express');
const cors = require('cors');
const multer = require('multer');
const path = require('path');
require('dotenv').config();

const app = express();
const port = process.env.PORT || 8080;

// Update CORS to accept Railway's domain
app.use(cors({
  //origin: process.env.FRONTEND_URL || 'http://localhost:8081',
  origin: '*',
  methods: ['GET', 'POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Accept', 'Origin'],
  credentials: true
}));

// Make sure the uploads directory exists
const fs = require('fs');
if (!fs.existsSync('uploads')) {
  fs.mkdirSync('uploads');
}

// Configure multer for audio file uploads
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    console.log('Processing file:', file);
    cb(null, 'uploads/');
  },
  filename: (req, file, cb) => {
    const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1E9);
    cb(null, file.fieldname + '-' + uniqueSuffix + path.extname(file.originalname));
  }
});

const upload = multer({
  storage: storage,
  fileFilter: (req, file, cb) => {
    console.log('Received file:', file);
    // Accept all audio files
    if (file.mimetype.startsWith('audio/') || file.originalname.match(/\.(mp3|wav|m4a|aac|ogg)$/)) {
      cb(null, true);
    } else {
      cb(new Error('Only audio files are allowed!'));
    }
  }
}).single('audio');

// Middleware
app.use(cors({
  origin: 'http://localhost:8081',
  methods: ['GET', 'POST'],
  allowedHeaders: ['Content-Type', 'Accept']
}));

// Audio upload endpoint
app.post('/api/summarize', (req, res) => {
    console.log('Received request to /api/summarize');
    
    upload(req, res, function(err) {
      if (err instanceof multer.MulterError) {
        console.error('Multer error:', err);
        return res.status(400).json({ error: 'File upload error: ' + err.message });
      } else if (err) {
        console.error('Other error:', err);
        return res.status(400).json({ error: err.message });
      }
  
      if (!req.file) {
        console.log('No file received');
        return res.status(400).json({ error: 'No audio file uploaded' });
      }
  
      console.log('File successfully uploaded:', req.file);
  
      res.json({
        status: 'success',
        summary: 'Test summary - file uploaded successfully',
        fileInfo: {
          filename: req.file.filename,
          originalName: req.file.originalname,
          size: req.file.size,
          mimetype: req.file.mimetype
        }
      });
    });
  });

// Add a test endpoint
app.get('/test', (req, res) => {
    res.json({ message: 'Backend is working!' });
  });

app.listen(port, '0.0.0.0', () => {
    console.log(`Server running on port ${port}`);
  });