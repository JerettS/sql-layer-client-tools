@ECHO OFF

SETLOCAL

SET CLIENT_JAR=akiban-client-tools-1.3.4.jar

IF EXIST "%~dp0..\pom.xml" GOTO FROM_BUILD

REM Installation Configuration

FOR %%P IN ("%~dp0..") DO SET AKIBAN_HOME=%%~fP

SET CLASSPATH=%AKIBAN_HOME%\lib\%CLIENT_JAR%;%AKIBAN_HOME%\lib\client\*

GOTO RUN_CMD

:FROM_BUILD

REM Build Configuration

FOR %%P IN ("%~dp0..") DO SET BUILD_HOME=%%~fP

SET CLASSPATH=%BUILD_HOME%\target\%CLIENT_JAR%;%BUILD_HOME%\target\dependency\*

GOTO RUN_CMD

:RUN_CMD
java %JVM_OPTS% -cp "%CLASSPATH%" com.akiban.client.dump.DumpClient %*
GOTO EOF

:EOF
ENDLOCAL
