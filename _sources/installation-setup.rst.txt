Set up LCAtricity
=================

::
*PSST* ... If you just want to check out the API, we're live at `http://example.lcatricity.live:8000/docs <http://example.lcatricity.live:8000/docs>`__.
::

Set up environment
------------------

1. Create an ENTSO-E account and request an API key
2. Decide on a place to run a database and `setup
   postgres <https://www.postgresql.org/docs/current/tutorial-install.html>`__.
3. Create an empty database called ``electricity_lca`` and create a user
   account with privilege to create tables on the database
4. Clone this repository
5. Copy ``.env.template`` to a new file ``.env`` and fill the
   copied file with ``ENTSOE_SECURITY_TOKEN`` = your ENTSOE security
   token. Fill also the connection details to your postgres sql instance
6. Create Python virtual environment

.. code:: commandline

   venv create elec_lca_venv

7. Install requirements

.. code:: commandline

   pip install -r requirement.txt

7. Run ``python src/setup/setup.py`` to initalize the database schema
   and load static data
8. Run all tests under ``tests/``

Set up database
---------------

Run

.. code:: commandline

   python src/setup/setup.py

..

   This creates the tables in the database and fills the constant value
   (e.g. environmental impacts, regions, electricity generation types)
   using the data in the ``data/`` directory