# Setting up Database and Notifications

## 1. Add PostgreSQL to Railway

Run this in your terminal:
```bash
railway add
```
Then select "PostgreSQL" from the list.

Or via the Railway dashboard:
1. Go to your project at https://railway.app/dashboard
2. Click "New" → "Database" → "Add PostgreSQL"
3. Railway will automatically set the DATABASE_URL variable

## 2. Email Notifications Setup

For email, we'll use SendGrid (free tier allows 100 emails/day):

1. Sign up at https://sendgrid.com/
2. Get your API key from Settings → API Keys
3. Add to Railway:
```bash
railway variables set SENDGRID_API_KEY=your_api_key_here
railway variables set NOTIFICATION_EMAIL=your-email@example.com
```

## 3. SMS Notifications Setup (Optional)

For SMS, we'll use Twilio:

1. Sign up at https://www.twilio.com/
2. Get your credentials from the Twilio Console
3. Add to Railway:
```bash
railway variables set TWILIO_ACCOUNT_SID=your_account_sid
railway variables set TWILIO_AUTH_TOKEN=your_auth_token
railway variables set TWILIO_PHONE_FROM=+1234567890
railway variables set NOTIFICATION_PHONE=+1234567890
```