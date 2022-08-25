# MobilityDB-JDBC

**Author: Tatiana Millan Poveda**<br>
**Email: tatimilpo@gmail.com**

JDBC driver for MobilityDB has been performed as the Master Thesis of the MSc in Computer Science of ULB.


## Abstract
The purpose of this project is the creation of Java Database Connectivity Driver (JDBC) that helps programs written in Java to connect with MobilityDB databases by extending PostgreSQL JDBC types. MobilityDB is an open-source database management system extension of PostgreSQL, that offers support for object moving data and special temporal data.
The MobilityDB-JDBC driver provides support for the new MobilityDB  types: Period, PeriodSet, TimestampSet, TBox, STBox , TInt, TBool, TFloat, TText, TGeomPoint and TGeogPoint and also the temporal types TInstant, TInstantSet, TSequence and TSequenceSet, and was built based on the MobilityDB Python adapter with the objective of acquire feature parity.


## Development
### Requirements
- Java >= 11
- Docker

### Build
The project is using Gradle for building and Maven for the dependencies, but it is not required to install any of them, it is possible to build the project by running the following command on the root folder:
 
On Linux:

    ./gradlew build
On Windows:

    gradlew.bat build
    
To just execute the unit tests:

On Linux:

    ./gradlew test
On Windows:

    gradlew.bat test

### Code Analysis 
To run the code analysis it is required to have a project configured on Sonarqube and then execute the command:

On Linux:

    ./gradlew sonarqube -Dsonar.projectKey={project key} -Dsonar.host.url={host} -Dsonar.login={token}
On Windows:

    gradlew.bat sonarqube -Dsonar.projectKey={project key} -Dsonar.host.url={host} -Dsonar.login={token}

#### Sonarqube Docker Image
To configure the Sonarqube project it is recommended to run the docker image.
For more details check [the Sonarqube setup guide.](https://docs.sonarqube.org/latest/setup/get-started-2-minutes/)

Download the docker image:

    docker pull sonarqube
    
Run the image for the first time:

    docker run -d --name sonarqube -e SONAR_ES_BOOTSTRAP_CHECKS_DISABLE=true -p 9000:9000 sonarqube:latest

Run the image after a restart:

    sudo docker start sonarqube

Once the image is running open http://localhost:9000  in a browser and login using:
- User: admin
- Password: admin

Create a new project
- Set project key and display name e.g MobilityDB-JDBC
- Select the option to analyze locally
- Generate a token e.g 1234
- Select Gradle for the option that describes the build
- Copy the gradle command and execute it, e.g:

   
    ./gradlew sonarqube -Dsonar.projectKey=MobilityDB-JDBC -Dsonar.host.url=http://localhost:9000 -Dsonar.login=1234
    
- Review the results on Sonarqube
### Running Integration Tests
To run the integration tests it is required to have a PostgreSQL database with MobilityDB extension.
The integration tests uses the connection string:
    
    jdbc:postgresql://localhost:25432/mobilitydb
    
but it can be modified in BaseIntegrationTest class.
To execute them run the command:

On Linux:

    ./gradlew integrationTests
On Windows:

    gradlew.bat integrationTests

#### MobilityDB Docker Image
It is recommended to run the MobilityDB docker image for the integration tests.
For more details check https://github.com/MobilityDB/MobilityDB#docker-container

    docker pull mobilitydb/mobilitydb
    docker volume create mobilitydb_data
    docker run --name "mobilitydb" -d -p 25432:5432 -v mobilitydb_data:/var/lib/postgresql mobilitydb/mobilitydb
