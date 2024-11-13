const express = require('express');
const cors = require('cors');
const multer = require('multer');
const path = require('path');
const { Anthropic } = require('@anthropic-ai/sdk');
require('dotenv').config();

const app = express();
const port = process.env.PORT || 8080;

// Initialize Claude
const anthropic = new Anthropic({
  apiKey: process.env.CLAUDE_API_KEY,
});

// Configure multer as before
const storage = multer.diskStorage({
  destination: function (req, file, cb) {
    cb(null, 'uploads/');
  },
  filename: function (req, file, cb) {
    const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1E9);
    cb(null, file.fieldname + '-' + uniqueSuffix + path.extname(file.originalname));
  }
});

const upload = multer({
  storage: storage,
  limits: {
    fileSize: 10 * 1024 * 1024 // 10 MB
  }
}).single('audio');

app.use(cors());

// Modified upload endpoint with Claude integration
app.post('/api/summarize', function(req, res) {
  upload(req, res, async function(err) {
    if (err) {
      console.error('Upload error:', err);
      return res.status(400).json({ error: err.message });
    }

    if (!req.file) {
      return res.status(400).json({ error: 'No audio file uploaded' });
    }

    try {
      // For now, we'll use placeholder text since we haven't integrated speech-to-text yet
      const transcribedText = "This is placeholder transcribed text. We'll add actual transcription next.";

      // Send to Claude for summarization
      const message = await anthropic.messages.create({
        model: "claude-3-opus-20240229",
        max_tokens: 1024,
        messages: [{
          role: "user",
          content: `Please provide a clear and concise summary of the following transcribed audio. 
                    Focus on the main points and key takeaways: ${transcribedText}`
        }]
      });

      const summary = message.content[0].text;

      res.json({
        status: 'success',
        summary: summary,
        fileInfo: {
          filename: req.file.filename,
          originalname: req.file.originalname,
          size: req.file.size,
          mimetype: req.file.mimetype
        }
      });

    } catch (error) {
      console.error('Processing error:', error);
      res.status(500).json({ 
        error: 'Error processing audio',
        details: error.message 
      });
    }
  });
});

app.listen(port, '0.0.0.0', () => {
  console.log(`Server running on port ${port}`);
});