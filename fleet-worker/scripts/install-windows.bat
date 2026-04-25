@echo off
setlocal enabledelayedexpansion

set "APP_NAME=Fleet Worker"
set "INSTALL_DIR=%LOCALAPPDATA%\FleetWorker"
set "BIN_DIR=%INSTALL_DIR%\bin"
set "CONFIG_DIR=%USERPROFILE%\.fleet-worker"
set "TASK_NAME=FleetWorker"

echo ==========================================
echo  %APP_NAME% Installer
echo ==========================================
echo.

:: Check for license key argument
if "%~1"=="" (
    echo Usage: install.bat LICENSE_KEY
echo Example: install.bat LR-XXXX-XXXX-XXXX
echo.
    pause
    exit /b 1
)

set "LICENSE_KEY=%~1"

:: Create directories
if not exist "%BIN_DIR%" mkdir "%BIN_DIR%"
if not exist "%CONFIG_DIR%" mkdir "%CONFIG_DIR%"

:: Copy binary and node_modules
echo Installing files to %INSTALL_DIR% ...
copy /Y "%~dp0fleet-worker.exe" "%BIN_DIR%\fleet-worker.exe" >nul
if errorlevel 1 (
    echo ERROR: Failed to copy fleet-worker.exe
    pause
    exit /b 1
)

:: Copy node_modules (robocopy handles directories better)
if exist "%~dp0node_modules" (
    robocopy "%~dp0node_modules" "%INSTALL_DIR%\node_modules" /E /NFL /NDL /NJH /NJS /nc /ns /np >nul
)

:: Write config
echo { "license_key": "%LICENSE_KEY%" } > "%CONFIG_DIR%\config.json"

:: Create scheduled task for auto-start
echo Creating startup task ...
schtasks /Create /F /TN "%TASK_NAME%" /TR "\"%BIN_DIR%\fleet-worker.exe\"" /SC ONLOGON /RL HIGHEST /NP >nul 2>&1
if errorlevel 1 (
    echo WARNING: Could not create startup task. You may need to start Fleet Worker manually.
) else (
    echo Startup task created.
)

:: Start the worker now
echo Starting %APP_NAME% ...
start /B "" "%BIN_DIR%\fleet-worker.exe" >nul 2>&1

echo.
echo ==========================================
echo  %APP_NAME% installed successfully!
echo.
echo  Binary: %BIN_DIR%\fleet-worker.exe
echo  Config: %CONFIG_DIR%\config.json
echo  Logs:   %CONFIG_DIR%\worker.log
echo.
echo  The worker is now running in the background
echo  and will auto-start on login.
echo ==========================================
echo.
pause
