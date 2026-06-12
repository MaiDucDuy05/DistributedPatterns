# Build stage: Compile code
FROM maven:3.9.5-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
# Copy mã nguồn
COPY src ./src
# Build ứng dụng (Bỏ qua Unit test cho nhanh)
RUN mvn clean package -DskipTests

# Run stage: Chạy ứng dụng bằng JRE nhẹ
FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=build /app/target/order-service-0.0.1-SNAPSHOT.jar app.jar
EXPOSE 8000
ENTRYPOINT ["java","-jar","/app/app.jar"]
