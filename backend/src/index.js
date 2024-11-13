const express = require('express');
const cors = require('cors');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
require('dotenv').config();

const app = express();
const port = process.env.PORT || 8080;

// Ensure uploads directory exists
if (!fs.existsSync('uploads')) {
  fs.mkdirSync('uploads');
}

// Configure multer storage
const storage = multer.diskStorage({
  destination: function(req, file, cb) {
    console.log('Multer destination called');
    cb(null, 'uploads/');
  },
  filename: function(req, file, cb) {
    console.log('Multer filename called', file);
    const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1E9);
    cb(null, file.fieldname + '-' + uniqueSuffix + path.extname(file.originalname));
  }
});

// Configure multer upload
const upload = multer({ 
  storage: storage,
  fileFilter: (req, file, cb) => {
    console.log('Received file:', file);
    // Accept audio files
    if (file.mimetype.startsWith('audio/') || 
        file.originalname.match(/\.(mp3|wav|m4a|aac|ogg)$/)) {
      cb(null, true);
    } else {
      cb(new Error('Only audio files are allowed!'));
    }
  }
});

// CORS configuration
app.use(cors({
  origin: '*',
  methods: ['GET', 'POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Accept']
}));

// Parse JSON bodies
app.use(express.json());

// Root endpoint
app.get('/', (req, res) => {
  res.json({ message: 'Server is running' });
});

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ status: 'ok' });
});

// File upload endpoint
app.post('/api/summarize', (req, res) => {
  console.log('Received request to /api/summarize');
  console.log('Request headers:', req.headers);
  
  const uploadMiddleware = upload.single('audio');
  
  uploadMiddleware(req, res, async (err) => {
    try {
      if (err instanceof multer.MulterError) {
        console.error('Multer error:', err);
        return res.status(400).json({ error: `Upload error: ${err.message}` });
      } else if (err) {
        console.error('Other error:', err);
        return res.status(400).json({ error: err.message });
      }

      console.log('Request file:', req.file);
      console.log('Request body:', req.body);

      if (!req.file) {
        console.log('No file found in request');
        return res.status(400).json({ error: 'No audio file uploaded' });
      }

      // File was successfully uploaded
      const fileInfo = {
        filename: req.file.filename,
        originalName: req.file.originalname,
        mimetype: req.file.mimetype,
        size: req.file.size,
        path: req.file.path
      };

      console.log('File successfully uploaded:', fileInfo);

      // For now, return a success response
      res.json({
        status: 'success',
        message: 'File uploaded successfully',
        summary: 'This is a test summary (file was uploaded successfully)',
        fileInfo: fileInfo
      });

    } catch (error) {
      console.error('Server error:', error);
      res.status(500).json({ error: 'Internal server error' });
    }
  });
});

// Error handling middleware
app.use((err, req, res, next) => {
  console.error('Global error handler:', err);
  res.status(500).json({
    error: 'Server error',
    message: err.message
  });
});

// Start server
app.listen(port, '0.0.0.0', () => {
  console.log(`Server running on port ${port}`);
  console.log('Upload directory:', path.resolve('uploads'));
});