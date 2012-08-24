@ECHO OFF

SETLOCAL

SET CLIENT_JAR=akiban-client-tools-1.3.1.jar
SET DRIVER_JAR=postgresql.jar

IF EXIST "%~dp0..\pom.xml" GOTO FROM_BUILD

REM Installation Configuration

FOR %%P IN ("%~dp0..") DO SET AKIBAN_HOME=%%~fP

SET JAR_FILES=%AKIBAN_HOME%\lib\%CLIENT_JAR%;%AKIBAN_HOME%\lib\%DRIVER_JAR%

GOTO RUN_CMD

:FROM_BUILD

REM Build Configuration

FOR %%P IN ("%~dp0..") DO SET BUILD_HOME=%%~fP

SET JAR_FILES=%BUILD_HOME%\target\%CLIENT_JAR%;%BUILD_HOME%\target\dependency\%DRIVER_JAR%

GOTO RUN_CMD

:RUN_CMD
java %JVM_OPTS% -cp "%JAR_FILES%" com.akiban.client.dump.DumpClient %*
GOTO EOF

:EOF
ENDLOCAL
