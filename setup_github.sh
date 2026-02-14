#!/bin/bash

# GitHub Repository Setup Script
# This script helps set up the GitHub repository for CDC Historical Warehouse Platform

echo "🚀 CDC Historical Warehouse Platform - GitHub Setup"
echo "=================================================="

# Check if git is initialized
if [ ! -d ".git" ]; then
    echo "❌ Git repository not found. Please run 'git init' first."
    exit 1
fi

echo "✅ Git repository found"

# Get GitHub username
read -p "👤 Enter your GitHub username: " USERNAME

if [ -z "$USERNAME" ]; then
    echo "❌ Username cannot be empty"
    exit 1
fi

# Repository name
REPO_NAME="cdc-historical-warehouse-platform"

echo ""
echo "📋 Repository Details:"
echo "   Name: $REPO_NAME"
echo "   Username: $USERNAME"
echo "   Description: A production-ready Change Data Capture (CDC) platform that implements SCD Type 2 historical tracking for data warehousing"
echo ""

# Add remote origin
echo "🔗 Adding remote origin..."
git remote add origin "https://github.com/$USERNAME/$REPO_NAME.git" 2>/dev/null || echo "⚠️  Remote origin may already exist"

# Push to GitHub
echo "📤 Pushing to GitHub..."
echo ""
echo "📝 Manual Steps Required:"
echo "1. Open https://github.com/new in your browser"
echo "2. Repository name: $REPO_NAME"
echo "3. Description: A production-ready Change Data Capture (CDC) platform that implements SCD Type 2 historical tracking for data warehousing"
echo "4. Choose Public or Private"
echo "5. DO NOT initialize with README, .gitignore, or license (we already have them)"
echo "6. Click 'Create repository'"
echo ""
echo "7. After creating the repository, run:"
echo "   git push -u origin main"
echo ""

# Optional: Try to push if remote exists
if git remote get-url origin >/dev/null 2>&1; then
    read -p "🔄 Try to push to remote now? (y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        git push -u origin main
    fi
fi

echo ""
echo "✅ Setup complete! Your repository is ready for collaboration."
echo ""
echo "📚 Next Steps:"
echo "1. Visit your repository: https://github.com/$USERNAME/$REPO_NAME"
echo "2. Review the README.md"
echo "3. Set up branch protection rules"
echo "4. Add topics/tags to improve discoverability"
echo "5. Consider enabling GitHub Actions for CI/CD"
echo ""
echo "🎉 Happy coding with CDC Historical Warehouse Platform!"
