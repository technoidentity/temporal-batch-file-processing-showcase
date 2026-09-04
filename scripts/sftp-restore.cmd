@echo off
rem Restore SFTP files: move everything from sftp_data\incoming back to
rem sftp_data\home so no file is lost after a demo run.
setlocal
set ROOT=%~dp0..
set HOME_DIR=%ROOT%\sftp_data\home
set IN_DIR=%ROOT%\sftp_data\incoming
if not exist "%HOME_DIR%" mkdir "%HOME_DIR%"
for %%f in ("%IN_DIR%\*") do (
  if /I not "%%~nxf"==".gitkeep" move "%%f" "%HOME_DIR%\" >nul && echo restored: %%~nxf
)
endlocal
