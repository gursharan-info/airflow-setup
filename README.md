## Information

* Based on Python (3.7-slim-buster) official Image [python:3.7-slim-buster](https://hub.docker.com/_/python/) and uses the official [Postgres](https://hub.docker.com/_/postgres/) as backend and [Redis](https://hub.docker.com/_/redis/) as queue
* Install [Docker](https://www.docker.com/) on your system.
* Install [Docker Compose](https://docs.docker.com/compose/install/)
* Install [Git](https://git-scm.com/downloads) on your system.

## Installation

- Clone the repo on your local system.
- Use the develop branch on your local for development.

## Build
    
    docker-compose up -f docker-compose-LocalExecutor.yml up --build
       
If you are facing an error similar to the screenshot below :

    webserver_1  | /usr/bin/env: ‘bash\r’: No such file or directory
    idp_gatherer_webserver_1 exited with code 127
    
You need to correct the line endings while doing the clone using following steps

- Step 1: 
    ```
    git clone https://github.com/bippisb/idp_hfi_automation.git --config core.autocrlf=input
    ```
- Step 2: The service name is the name mentioned in the Docker compose file.
    ```
    docker build -t <service_name / webserver> -f ./docker/Dockerfile .
    ```
- Step 3:
    ```
    docker-compose up --build
    ```