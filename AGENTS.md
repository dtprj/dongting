## Codebase
- Java 11 (Java 8 for client module)
- Apache Maven 3.9
- JUnit 5.13

## Build
- Build: mvn clean package -DskipTests
- Test: mvn clean test -Dtick=5

## logs
- logger: com.github.dtprj.dongting.log.DtLog
- logger factory: com.github.dtprj.dongting.log.DtLogs