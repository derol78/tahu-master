@echo off
setlocal enabledelayedexpansion

:: Define the file and target directory
set "source_file=C:\Users\derol\Documents\GitHub\tahu-master\tahu-master\java\examples\listener\node-red\CIM_Analytics.csv"
set "target_folder=C:\Users\derol\Documents\GitHub\tahu-master\tahu-master\java\examples\listener\node-red\db"

:: Check the file size
for %%A in ("%source_file%") do set size=%%~zA

:: If the file is larger than 30 MB (30 * 1024 * 1024 = 31457280 bytes)
if !size! gtr 31457280 (
    echo File size is greater than 30 MB. Proceeding...

    :: Get the current date and time
    for /f "tokens=2-4 delims=/ " %%a in ('date /t') do set today=%%c-%%a-%%b
    for /f "tokens=1-2 delims=:." %%a in ('time /t') do set time_=%%a%%b
    set timestamp=%today%_%time_%

    :: Generate the new file name with timestamp
    set file_name=%source_file%
    set new_file=%target_folder%\file_%timestamp%.txt

    :: Copy the file and delete the original
    copy "%source_file%" "%new_file%"
    del "%source_file%"
    echo File has been copied and renamed to "%new_file%" and the original deleted.
) else (
    echo File size is less than or equal to 30 MB. No action taken.
)

