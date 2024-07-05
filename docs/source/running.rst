Running API via Docker
----------------------

.. code:: commandline

   docker image build . -t elec_lca_microservice:latest

then

.. code:: commandline

   docker container run -d --rm -p 80:8000 -p 25060:25060 elec_lca_microservice:latest

Note that you should change ``25060`` to the port used by the (Postgres)
database you are using to store the data, as specified in your .env file

This should start a web browser showing a dashboard on localhost:8900