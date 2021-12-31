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

Additional management commands for docker container.

1. Start Docker Containers In the Foreground

    When you don’t specify the -d option, docker-compose will start all the services in the foreground.

    In this case, you can see all log messages directly on the screen.

    This is helpful when you are debugging any startup related issues with your docker containers, images, or services.

    ```
    docker-compose up
    ```
2.  Start Docker Containers In the Background

    When you specify the -d option, docker-compose will start all the services in the background.
    The -d option runs the docker application in the background as a daemon. This will leave the application running until you decide to stop it.

    ```
    docker-compose up -d
    ```
3.  Additional docker-compose Startup Options
    When you use docker-compose up, if there are any changes in the docker-compose.yml file that affects the containers, they will stopped and recreated.

    But, you can force docker-compose not to stop and recreate the containers, you can use –no-recreate option as shown below during the docker-compose up. In other words, if the container already exits, this will not recreate it.

    ```
    docker-compose up -d --no-recreate
    ```

    You also can do the opposite. The following will forcefully recreate the containers even if nothing in the docker-compose.yml is changed.

    You can also specify the timeout value. Default value is 10 seconds, but the following command will use the time-out value of 30 seconds.

    ```
    docker-compose up -d -t 30
    ```

    The following are few additional options you can use along with “docker-compose up”

    - -no-deps This will not start any linked depended services.
    - -no-build This will not build the image, even when the image is missing
    - -abort-on-container-exit This will stop all the containers if any container was stopped. You cannot use this option with -d, you have to use this option by itself.
    - -no-color In the output, this will not show any color. This will display the monochrome output on screen.


For more details refer to the docker-compose [documentation](https://docs.docker.com/compose/) or this [tutorial](https://www.thegeekstuff.com/2016/04/docker-compose-up-stop-rm/)