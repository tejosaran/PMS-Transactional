FROM maven:3.9.4-eclipse-temurin-21 AS build
WORKDIR /workspace

COPY pom.xml .
RUN mvn -B dependency:go-offline

COPY src ./src
RUN mvn -B -f pom.xml clean package -DskipTests

FROM eclipse-temurin:21-jre-jammy
WORKDIR /app

RUN groupadd -r app && useradd -r -g app app

COPY --from=build /workspace/target/*.jar /app/app.jar

RUN chown app:app /app/app.jar
USER app

EXPOSE 8084

ENV JAVA_OPTS="-Xms256m -Xmx512m"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]