Contributing to PhotoClassify
============================

We welcome contributions from the community! This guide will help you get started with contributing to PhotoClassify.

Development Setup
----------------

1. Fork the repository
2. Clone your fork:

   .. code-block:: bash

      git clone https://github.com/yourusername/PhotoClassify.git
      cd PhotoClassify

3. Create a development environment:

   .. code-block:: bash

      python -m venv .venv
      source .venv/bin/activate  # On Windows: .venv\Scripts\activate
      pip install -r requirements.txt
      pip install -e .

4. Install development dependencies:

   .. code-block:: bash

      pip install -r requirements-dev.txt

Coding Standards
---------------

Style Guide
~~~~~~~~~~

We follow PEP 8 style guidelines. Use the following tools to ensure compliance:

.. code-block:: bash

   # Run linter
   flake8 photoclassify tests

   # Run type checker
   mypy photoclassify tests

   # Run formatter
   black photoclassify tests

Documentation
~~~~~~~~~~~~

* All new code must be documented
* Use Google-style docstrings
* Update relevant documentation files
* Add type hints to all functions

Testing
~~~~~~~

* Write tests for all new features
* Ensure all tests pass before submitting
* Maintain or improve test coverage

.. code-block:: bash

   # Run tests
   pytest

   # Run with coverage
   pytest --cov=photoclassify

Pull Request Process
-------------------

1. Create a new branch for your feature/fix:

   .. code-block:: bash

      git checkout -b feature/your-feature-name

2. Make your changes
3. Run tests and checks:

   .. code-block:: bash

      pytest
      flake8 photoclassify tests
      mypy photoclassify tests
      black --check photoclassify tests

4. Commit your changes:

   .. code-block:: bash

      git commit -m "Description of your changes"

5. Push to your fork:

   .. code-block:: bash

      git push origin feature/your-feature-name

6. Create a Pull Request

Pull Request Guidelines
---------------------

* Provide a clear description of changes
* Reference any related issues
* Ensure all CI checks pass
* Update documentation as needed
* Keep PRs focused and manageable

Issue Reporting
--------------

When reporting issues:

1. Use the issue template
2. Provide detailed reproduction steps
3. Include relevant logs and error messages
4. Specify your environment details

Release Process
--------------

1. Update version in ``setup.py``
2. Update ``CHANGELOG.md``
3. Create a release tag
4. Build and upload to PyPI

Code of Conduct
--------------

Please be respectful and considerate of others. We follow the `Python Community Code of Conduct <https://www.python.org/psf/conduct/>`_.

Getting Help
-----------

* Join our `Discord server <https://discord.gg/your-server>`_
* Check the `GitHub Discussions <https://github.com/yourusername/PhotoClassify/discussions>`_
* Open an issue for questions 