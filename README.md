# CSV Splitter - test deploy to railway

A web application for splitting large CSV files into smaller, manageable chunks. Built with Flask and deployable on Railway.

## Features

- 📁 **Split CSV files** into multiple parts based on row count
- 🚀 **Async processing** for files larger than 50MB
- 📊 **Database tracking** of all processed files
- 📧 **Email/SMS notifications** when files are processed
- 📈 **Statistics page** showing processing history
- 🔄 **Progress tracking** for large file processing
- 🌐 **Multiple encoding support** (UTF-8, Latin1, ISO-8859-1, CP1252)
- ⏱️ **Extended timeout** (300s) for processing large files

## Live Demo

https://csv-splitter-production.up.railway.app

## Installation

### Local Development

1. Clone the repository:
```bash
git clone https://github.com/justinbleuel/csv-splitter.git
cd csv-splitter
```

2. Create a virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Run the application:
```bash
python flask_app.py
```

5. Open http://localhost:5001 in your browser

### Deploy to Railway

1. Fork this repository
2. Sign up at [Railway](https://railway.app)
3. Create a new project and connect your GitHub repo
4. Add PostgreSQL database:
   ```bash
   railway add
   # Select PostgreSQL
   ```
5. Deploy:
   ```bash
   railway up
   ```

## Configuration

### Environment Variables

Set these in Railway or your `.env` file:

#### Database (automatically set by Railway)
- `DATABASE_URL` - PostgreSQL connection string

#### Email Notifications (Optional)
- `SENDGRID_API_KEY` - Your SendGrid API key
- `NOTIFICATION_EMAIL` - Email to receive notifications

#### SMS Notifications (Optional)
- `TWILIO_ACCOUNT_SID` - Your Twilio account SID
- `TWILIO_AUTH_TOKEN` - Your Twilio auth token
- `TWILIO_PHONE_FROM` - Your Twilio phone number
- `NOTIFICATION_PHONE` - Phone number to receive SMS

## Usage

1. **Upload a CSV file** using the web interface
2. **Set the maximum rows** per split file (default: 50,000)
3. **Click "Split CSV"** to process
4. **Download the ZIP file** containing all split parts

### File Size Handling

- **Files < 50MB**: Processed synchronously with immediate download
- **Files > 50MB**: Processed asynchronously with progress tracking
  - Returns a task ID
  - Poll `/progress/<task_id>` for status
  - Download from `/download/<task_id>` when complete

## API Endpoints

- `GET /` - Main upload interface
- `POST /split` - Process CSV file
- `GET /stats` - View processing statistics
- `GET /progress/<task_id>` - Check async processing status
- `GET /download/<task_id>` - Download processed file
- `GET /init-db` - Initialize database (first time setup)
- `GET /debug-db` - Debug database connection

## Database Schema

The app tracks all processed files with:
- Filename
- Processing timestamp
- Number of parts created
- Total rows processed
- Processing time
- File size

## Development

### Project Structure
```
csv-splitter/
├── flask_app.py          # Main application
├── models.py             # Database models
├── requirements.txt      # Python dependencies
├── Procfile             # Heroku/Railway configuration
├── Dockerfile           # Docker configuration
├── railway.json         # Railway configuration
└── init_db.py          # Database initialization script
```

### Adding Features

1. The app uses Flask with SQLAlchemy for the database
2. Large file processing happens in background threads
3. Progress tracking uses in-memory storage
4. Notifications are sent asynchronously

## Troubleshooting

### Database Issues
1. Visit `/init-db` to create database tables
2. Check `/debug-db` for connection status
3. Ensure PostgreSQL is added in Railway

### Large File Timeouts
- The app is configured with a 300-second timeout
- Files are processed in chunks to reduce memory usage
- Check Railway logs for processing errors

### Notification Issues
- Verify environment variables are set correctly
- Check Railway logs for SendGrid/Twilio errors
- Ensure API keys have proper permissions

## License

MIT License - feel free to use this for your own projects!

## Contributing

Pull requests are welcome! Please feel free to submit improvements or bug fixes.

## Support

For issues or questions, please open an issue on GitHub.
