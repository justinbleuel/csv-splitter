#!/bin/bash

echo "Deploying CSV Splitter to Railway..."

# Login to Railway (if needed)
echo "Making sure you're logged in to Railway..."
railway login

# Link to project (create new or link existing)
echo "Linking to Railway project..."
railway link

# Deploy
echo "Deploying your application..."
railway up

# Show deployment URL
echo "Getting deployment URL..."
railway open

echo "Deployment complete!"