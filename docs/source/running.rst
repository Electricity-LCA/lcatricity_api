Running API via Docker
----------------------

Create a copy of `template-docker-compose.yml` and update it to ensure the ports exposed match your configuration (e.g. are you running your api on port 8000 or 80, then change EXPOSE 8000 to EXPOSE 80, same for the Postgres database)

The environment variables in the `docker-compose.yml` should be the same as those specified in your .env file

Then run

.. code:: commandline

   docker compose up


This should start a web browser showing a dashboard on localhost:9000