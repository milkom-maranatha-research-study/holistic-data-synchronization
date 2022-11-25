# data-synchronization-service

It's a Python project used to pull data from Metabase, transform it into batch of Metabase data, and then pushes it to the Backend.

## Prerequisites
### Docker

All our components run in Docker containers. Development orchestration is handled by _docker-compose_. Therefore, installing Docker on your machine is required. Regarding installation guidelines, please follow the particular links below:

For machines running **MacOS** you can follow steps explained [here](https://docs.docker.com/docker-for-mac/install/)

For machines running **Linux (Ubuntu)** you can follow steps explained [here](https://docs.docker.com/desktop/install/linux-install/)

Please also ensures that _docker-compose_ command is installed.

### How to Start the Project

#### Setup the environment variables

- Open `.env.example`, and then fill in all the credential.
- If you don't have Metabase service account, leave these parameters as it is.
  <br/><br/>
  ```
  METABASE_URL=
  METABASE_SERVICE_ACCOUNT=
  METABASE_SERVICE_ACCOUNT_PASSWORD=
  ```
  However, please ensures the `DEV_MODE=True` on the `sync/settings.py`. When you run the project in a developer mode, it will make use of the downloaded Metabase data that are available on this repo (`ther_interactions.csv.tmp` and `thers_joining_nd.csv.tmp`).<br/><br/>
- Fills the Backend environment variables.
  <br/><br/>
  ```
  BACKEND_URL=your-backend-url
  BACKEND_SERVICE_ACCOUNT=your-backend-account-username
  BACKEND_SERVICE_ACCOUNT_PASSWORD=your-backend-account-password
  ```
  If you run [Backend App](https://github.com/milkom-maranatha-research-study/holistic-backend) on your local machine using `docker-compose`, fill the `BACKEND_URL` with `http://localhost:8080`. Also, don't forget to create a new service account in the Backend App through Django Admin. You can create a new User and select the `is_service_account` checkbox.<br/><br/>
  Once you have it, fills the `BACKEND_SERVICE_ACCOUNT` and `BACKEND_SERVICE_ACCOUNT_PASSWORD` with the username and password belonging to the User that you just created.

#### Run with Docker
- This project provides a docker-compose file already. Hence, after installing `Docker` and `docker-compose`, you only need to execute this command from the command line.
  <br/><br/>
  ```bash
  $ docker-compose up --build
  ```
- If you also run the [Backend App](https://github.com/milkom-maranatha-research-study/holistic-backend) with docker, then you need to expose your `http://localhost:8080` to the outside world. One of the alternatives is use the [ngrok](https://ngrok.com/download). Just create a new account on their website (it's free) and then use the command below on your terminal.
  <br/><br/>
  ```bash
  $ ngrok http http://localhost:8000
  ```
- Once you see the unique DNS for your localhost, copy paste it to the `BACKEND_URL`.

#### Run without Docker
- Open terminal, and then navigates to the root directory of this project.
- Setup [python virtual environment](https://docs.python.org/3/library/venv.html).
  <br/><br/>
  ```bash
  $ python virtualenv  -p .venv
  ```
- Activate virtual environment.
  <br/><br/>
  ```bash
  $ source .venv/bin/activate
  ```
- Install project's requirements.
  <br/><br/>
  ```bash
  $ pip install -r requirements.txt
  ```
- Run the project.
  <br/><br/>
  ```bash
  $ ./sync.sh locally
  ```

### How to Test

The project's test runner is also run on top of docker, so all you need to do is to call this command.

```bash
$ python exec -it holistic-data-synchronization\_sync\_1 python -m unittest discover
```
