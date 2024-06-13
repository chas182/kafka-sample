# kafka-sample

## About The Project
This project is about setting up kafka broker with zookeeper on Docker using docker-compose to try the main functions of kafka

To run all of this use `docker-compose up -d` in terminal.

To stop use `docker-compose down --volumes --remove-orphans`.

Topics auto created:
* client-topic
* transaction-topic

Spring Boot Components available:
* Producer http://localhost:8080/swagger-ui/
