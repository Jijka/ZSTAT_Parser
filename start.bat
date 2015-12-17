@echo off
ECHO %date% %time%

python bin\ProfileParser.py ./Logs/ZSTATDTL_63880.txt

pause