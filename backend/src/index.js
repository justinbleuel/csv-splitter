const express = require('express');
const cors = require('cors');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const { Anthropic } = require('@anthropic-ai/sdk');
const OpenAI = require('openai');
require('dotenv').config();

const app = express();
const port = process.env.PORT || 8080;

// Initialize APIs
const anthropic = new Anthropic({
  apiKey: process.env.CLAUDE_API_KEY,
});

const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

// Configure multer
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
    fileSize: 25 * 1024 * 1024 // 25MB limit
  }
}).single('audio');

app.use(cors());

// Helper function for transcription
async function transcribeAudio(filepath) {
  try {
    console.log('Starting transcription for:', filepath);
    
    const transcription = await openai.audio.transcriptions.create({
      file: fs.createReadStream(filepath),
      model: "whisper-1",
      language: "en", // can be made dynamic based on needs
      response_format: "text"
    });

    console.log('Transcription completed');
    return transcription;
  } catch (error) {
    console.error('Transcription error:', error);
    throw new Error(`Transcription failed: ${error.message}`);
  }
}

// Helper function for summarization
async function summarizeText(text) {
  try {
    console.log('Starting summarization');
    
    const message = await anthropic.messages.create({
      model: "claude-3-opus-20240229",
      max_tokens: 1024,
      messages: [{
        role: "user",
        content: `Please provide a clear and concise summary of the following transcribed audio. 
                  Focus on the main points and key takeaways: ${text}`
      }]
    });

    console.log('Summarization completed');
    return message.content[0].text;
  } catch (error) {
    console.error('Summarization error:', error);
    throw new Error(`Summarization failed: ${error.message}`);
  }
}

// Main upload and process endpoint
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
      console.log('Processing file:', req.file.filename);

      // Step 1: Transcribe the audio
      const transcription = await transcribeAudio(req.file.path);
      console.log('Transcription:', transcription.substring(0, 100) + '...');

      // Step 2: Summarize the transcription
      const summary = await summarizeText(transcription);

      // Step 3: Clean up - delete the uploaded file
      fs.unlink(req.file.path, (err) => {
        if (err) console.error('Error deleting file:', err);
      });

      // Send response
      res.json({
        status: 'success',
        summary: summary,
        transcription: transcription, // Optionally include the full transcription
        fileInfo: {
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

      // Clean up on error
      if (req.file) {
        fs.unlink(req.file.path, (err) => {
          if (err) console.error('Error deleting file:', err);
        });
      }
    }
  });
});

app.listen(port, '0.0.0.0', () => {
  console.log(`Server running on port ${port}`);
});