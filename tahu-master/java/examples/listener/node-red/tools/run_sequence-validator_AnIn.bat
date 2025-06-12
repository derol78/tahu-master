@echo off

:: Copy and overwrite file if it already exists
copy /Y "C:\Users\derol\Documents\GitHub\tahu-master\tahu-master\java\examples\listener\node-red\AnIn1.csv" "C:\Users\derol\Documents\GitHub\tahu-master\tahu-master\java\examples\listener\node-red\tools\ek1.csv"

:: Execute the JAR file
java -jar sequence-validator-0.0.1-SNAPSHOT.jar

:: Pause to keep the window open after execution
pause
