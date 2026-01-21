@echo off
REM ============================================================================
REM Coach Outreach Pro v6.0 - Windows Launcher
REM ============================================================================
REM Double-click this file to start the application.
REM First run will set up the virtual environment automatically.
REM ============================================================================

title Coach Outreach Pro v6.0

echo.
echo ========================================
echo     COACH OUTREACH PRO v6.0
echo     Starting Application...
echo ========================================
echo.

REM Check if Python is installed
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Python is not installed or not in PATH.
    echo.
    echo Please install Python from https://python.org
    echo Make sure to check "Add Python to PATH" during installation.
    echo.
    pause
    exit /b 1
)

for /f "tokens=*" %%i in ('python --version 2^>^&1') do set PYVER=%%i
echo [OK] Found %PYVER%

REM Check/create virtual environment
if not exist "venv" (
    echo.
    echo First run detected - setting up environment...
    echo This may take a minute.
    echo.
    
    python -m venv venv
    if %errorlevel% neq 0 (
        echo [ERROR] Failed to create virtual environment
        pause
        exit /b 1
    )
    echo [OK] Virtual environment created
    
    REM Activate and install dependencies
    call venv\Scripts\activate.bat
    
    echo.
    echo Installing dependencies...
    pip install --upgrade pip -q
    pip install -r requirements.txt -q
    
    if %errorlevel% neq 0 (
        echo [ERROR] Failed to install dependencies
        pause
        exit /b 1
    )
    echo [OK] Dependencies installed
) else (
    call venv\Scripts\activate.bat
)

echo.
echo [OK] Starting Coach Outreach Pro...
echo.
echo Opening browser to http://localhost:5001
echo Keep this window open while using the app.
echo.
echo Press Ctrl+C to stop the server.
echo.

REM Open browser after delay
start "" cmd /c "timeout /t 2 /nobreak >nul && start http://localhost:5001"

REM Run the app
python app.py --port 5001

echo.
echo Application stopped.
echo.
pause
