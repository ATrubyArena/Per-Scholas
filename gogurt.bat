@echo off
REM Check if a commit message is provided
IF "%~1"=="" (
    echo Please provide a commit message.
    echo Usage: git_push.bat "Your commit message"
    exit /b 1
)

REM Navigate to the current script's directory
cd /d %~dp0

REM Stage all changes in the current directory
git add .

REM Commit with the provided message
git commit -m "%~1"

REM Push to origin main
git push origin main