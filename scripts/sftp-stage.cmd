@echo off
rem Stage SFTP file(s): move from sftp_data\home into sftp_data\incoming so the
rem Pattern-1 poller detects and downloads them. Files are MOVED (restore-safe).
rem Usage: scripts\sftp-stage.cmd [filename]   (no arg = stage all files in home)
setlocal
set ROOT=%~dp0..
set HOME_DIR=%ROOT%\sftp_data\home
set IN_DIR=%ROOT%\sftp_data\incoming
if not exist "%IN_DIR%" mkdir "%IN_DIR%"
if "%~1"=="" (
  for %%f in ("%HOME_DIR%\*") do move "%%f" "%IN_DIR%\" >nul && echo staged: %%~nxf
) else (
  if exist "%HOME_DIR%\%~1" (
    move "%HOME_DIR%\%~1" "%IN_DIR%\" >nul && echo staged: %~1
  ) else (
    echo skip ^(not in home^): %~1
  )
)
endlocal
