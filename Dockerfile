# Expects the fat jar to be built first: ./mvnw package -DskipTests
# (kept as a plain-JRE image so `make deploy` stays fast in class)
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY target/orderflow-0.0.1-SNAPSHOT.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
