Installation
============

System Requirements
------------------

PhotoClassify requires Python 3.8 or higher. The following operating systems are supported:

* Linux
* macOS
* Windows

Installing from Source
---------------------

1. Clone the repository:

   .. code-block:: bash

      git clone https://github.com/yourusername/PhotoClassify.git
      cd PhotoClassify

2. Create and activate a virtual environment:

   .. code-block:: bash

      # On Linux/macOS
      python -m venv .venv
      source .venv/bin/activate

      # On Windows
      python -m venv .venv
      .venv\Scripts\activate

3. Install dependencies:

   .. code-block:: bash

      pip install -r requirements.txt

4. Install the package:

   .. code-block:: bash

      pip install -e .

Verifying Installation
--------------------

To verify that PhotoClassify is installed correctly, run:

.. code-block:: bash

   photoclassify --version

You should see the version number displayed.

Troubleshooting
--------------

Common Issues
~~~~~~~~~~~~

1. **Permission Errors**
   - Ensure you have write permissions in the installation directory
   - Use ``sudo`` if necessary (Linux/macOS)

2. **Python Version Issues**
   - Verify your Python version with ``python --version``
   - Install Python 3.8 or higher if needed

3. **Virtual Environment Issues**
   - Make sure the virtual environment is activated
   - Try recreating the virtual environment if problems persist

Getting Help
~~~~~~~~~~~

If you encounter any issues during installation, please:

1. Check the `Troubleshooting`_ section above
2. Search the `GitHub Issues <https://github.com/yourusername/PhotoClassify/issues>`_
3. Create a new issue if your problem isn't documented 