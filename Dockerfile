# Use a lightweight Eclipse Temurin JDK 21 image
FROM eclipse-temurin:21-jre-jammy

# Set the working directory
WORKDIR /app

# Copy the JAR from the target folder (which we downloaded in the workflow)
# Using a wildcard to match the version automatically
COPY target/library-events-producer-*.jar app.jar

# Run the application
ENTRYPOINT ["java", "-jar", "app.jar"]