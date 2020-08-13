#Multi-Stage Docker Build

FROM maven:3.6.3-adoptopenjdk-14 as PACKAGE
COPY src /usr/src/app/src  
COPY pom.xml /usr/src/app  
#TODO - use this approach if we sort out Redis host addressibility during Unit Test
#RUN apt-get update && apt-get -y install redis
#RUN redis-server --daemonize yes
RUN mvn -f /usr/src/app/pom.xml clean package

FROM adoptopenjdk:latest
COPY --from=package /usr/src/app/target/api-0.0.1-SNAPSHOT-jar-with-dependencies.jar /usr/src/app/target/api.jar  
ENTRYPOINT ["java","-jar","/usr/src/app/target/api.jar"]  