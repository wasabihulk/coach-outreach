#!/bin/bash
#============================================================================
# Coach Outreach System - Mac Launcher
#============================================================================
# This script launches the Coach Outreach web interface on macOS.
#
# Usage:
#   ./launch_mac.sh
#
# Or make it an app:
#   1. Open Automator
#   2. Create new "Application"
#   3. Add "Run Shell Script" action
#   4. Paste: cd /path/to/coach_outreach_v2 && ./launch_mac.sh
#   5. Save as "Coach Outreach.app"
#============================================================================

# Configuration
PORT=5001
VENV_NAME="venv"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo ""
echo "========================================"
echo "🏈 COACH OUTREACH SYSTEM"
echo "   Launching Web Interface..."
echo "========================================"
echo ""

# Check for Python
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}❌ Python 3 is not installed!${NC}"
    echo "   Please install Python 3 from python.org"
    exit 1
fi

# Check/create virtual environment
if [ ! -d "$VENV_NAME" ]; then
    echo -e "${YELLOW}📦 Creating virtual environment...${NC}"
    python3 -m venv "$VENV_NAME"
    
    echo -e "${YELLOW}📦 Installing dependencies...${NC}"
    source "$VENV_NAME/bin/activate"
    pip install --upgrade pip
    pip install -r requirements.txt
else
    source "$VENV_NAME/bin/activate"
fi

# Check for credentials
if [ ! -f "credentials.json" ]; then
    echo ""
    echo -e "${YELLOW}⚠️  credentials.json not found!${NC}"
    echo ""
    echo "To connect to Google Sheets, you need to:"
    echo "1. Go to console.cloud.google.com"
    echo "2. Create a project and enable Sheets API"
    echo "3. Create a Service Account"
    echo "4. Download the JSON key as 'credentials.json'"
    echo "5. Share your spreadsheet with the service account email"
    echo ""
    echo -e "${BLUE}The app will start but Sheets features won't work.${NC}"
    echo ""
fi

# Check if port is in use
if lsof -Pi :$PORT -sTCP:LISTEN -t >/dev/null ; then
    echo -e "${YELLOW}⚠️  Port $PORT is already in use.${NC}"
    echo "   Another instance may be running."
    echo ""
    
    # Try to open browser anyway
    open "http://127.0.0.1:$PORT"
    exit 0
fi

# Start the application
echo -e "${GREEN}🚀 Starting server on http://127.0.0.1:$PORT${NC}"
echo ""
echo "Press Ctrl+C to stop"
echo ""

# Run the app
python3 app.py --port $PORT

# Deactivate venv on exit
deactivate
