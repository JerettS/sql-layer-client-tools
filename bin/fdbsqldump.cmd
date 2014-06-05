@ECHO OFF

SETLOCAL

SET CLIENT_JAR=fdb-sql-layer-client-tools-1.9.5.jar

IF EXIST "%~dp0..\pom.xml" GOTO FROM_BUILD

REM Installation Configuration

FOR %%P IN ("%~dp0..") DO SET FOUNDATIONDB_HOME=%%~fP

SET CLASSPATH=%FOUNDATIONDB_HOME%\sql\lib\%CLIENT_JAR%;%FOUNDATIONDB_HOME%\sql\lib\client\*

GOTO RUN_CMD

:FROM_BUILD

REM Build Configuration

FOR %%P IN ("%~dp0..") DO SET BUILD_HOME=%%~fP

SET CLASSPATH=%BUILD_HOME%\target\%CLIENT_JAR%;%BUILD_HOME%\target\dependency\*

GOTO RUN_CMD

:RUN_CMD
java %JVM_OPTS% -cp "%CLASSPATH%" com.foundationdb.sql.client.dump.DumpClient %*
GOTO EOF

:EOF
ENDLOCAL
