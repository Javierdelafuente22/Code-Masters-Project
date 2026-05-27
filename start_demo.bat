@echo off
REM Ampeer demo launcher.
REM   1. Starts the Gemini-backed chatbot server in a separate window
REM   2. Waits 2s for it to come up
REM   3. Opens index.html in your default browser
REM Close the server window (or Ctrl+C inside it) when the demo is done.

cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
  echo [demo] .venv\Scripts\python.exe not found. Run: python -m venv .venv ^&^& .venv\Scripts\pip install -r requirements.txt flask
  pause
  exit /b 1
)

start "Ampeer Server" cmd /k ".venv\Scripts\python.exe chatbot\server.py"

timeout /t 2 /nobreak >nul

start "" "http://localhost:5000/"
