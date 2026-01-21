#!/bin/bash
# ============================================================================
# Coach Outreach Pro v6.0 - Mac Launcher
# ============================================================================
# Double-click this file to start the application.
# First run will set up the virtual environment automatically.
# ============================================================================

# Get the directory where this script is located
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo ""
echo -e "${BLUE}╔═══════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║      🏈 COACH OUTREACH PRO v6.0               ║${NC}"
echo -e "${BLUE}║         Enterprise Edition                    ║${NC}"
echo -e "${BLUE}╚═══════════════════════════════════════════════╝${NC}"
echo ""

# Check if Python 3 is installed
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}❌ Python 3 is not installed.${NC}"
    echo ""
    echo "Please install Python 3 from https://python.org"
    echo ""
    read -p "Press Enter to close..."
    exit 1
fi

PYTHON_VERSION=$(python3 --version 2>&1)
echo -e "${GREEN}✓${NC} Found $PYTHON_VERSION"

# Check/create virtual environment
if [ ! -d "venv" ]; then
    echo ""
    echo -e "${YELLOW}First run detected - setting up environment...${NC}"
    echo "This may take a minute."
    echo ""
    
    python3 -m venv venv
    if [ $? -ne 0 ]; then
        echo -e "${RED}❌ Failed to create virtual environment${NC}"
        read -p "Press Enter to close..."
        exit 1
    fi
    echo -e "${GREEN}✓${NC} Virtual environment created"
    
    # Activate and install dependencies
    source venv/bin/activate
    
    echo ""
    echo "Installing dependencies..."
    pip install --upgrade pip -q
    pip install -r requirements.txt -q
    
    if [ $? -ne 0 ]; then
        echo -e "${RED}❌ Failed to install dependencies${NC}"
        read -p "Press Enter to close..."
        exit 1
    fi
    echo -e "${GREEN}✓${NC} Dependencies installed"
else
    source venv/bin/activate
fi

echo ""
echo -e "${GREEN}Starting Coach Outreach Pro...${NC}"
echo ""
echo "Opening browser to http://localhost:5001"
echo "Keep this window open while using the app."
echo ""
echo "Press Ctrl+C to stop the server."
echo ""

# Open browser after a short delay (gives server time to start)
(sleep 2 && open http://localhost:5001) &

# Run the app
python app.py --port 5001

# If we get here, the app was stopped
echo ""
echo -e "${YELLOW}Application stopped.${NC}"
echo ""
read -p "Press Enter to close..."
