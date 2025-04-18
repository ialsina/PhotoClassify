API Reference
=============

This section provides detailed documentation of PhotoClassify's API.

Core Modules
------------

photoclassify.protocols
~~~~~~~~~~~~~~~~~~~~~~~

.. automodule:: photoclassify.protocols
   :members:
   :undoc-members:
   :show-inheritance:

This module provides core file operation protocols including:

* File integrity verification
* Space checking
* Safe file operations
* Hash calculation

photoclassify.catalog
~~~~~~~~~~~~~~~~~~~~~

.. automodule:: photoclassify.catalog
   :members:
   :undoc-members:
   :show-inheritance:

The catalog module handles photo organization and metadata management.

photoclassify.diff
~~~~~~~~~~~~~~~~~~

.. automodule:: photoclassify.diff
   :members:
   :undoc-members:
   :show-inheritance:

This module provides functionality for finding duplicate and similar photos.

photoclassify.copies
~~~~~~~~~~~~~~~~~~~~

.. automodule:: photoclassify.copies
   :members:
   :undoc-members:
   :show-inheritance:

Handles the copying and organization of photos with various options.

photoclassify.config
~~~~~~~~~~~~~~~~~~~~

.. automodule:: photoclassify.config
   :members:
   :undoc-members:
   :show-inheritance:

Configuration management for PhotoClassify.

photoclassify.entrypoints
~~~~~~~~~~~~~~~~~~~~~~~~~

.. automodule:: photoclassify.entrypoints
   :members:
   :undoc-members:
   :show-inheritance:

Command-line interface entry points and argument parsing.

photoclassify.log
~~~~~~~~~~~~~~~~~

.. automodule:: photoclassify.log
   :members:
   :undoc-members:
   :show-inheritance:

Logging configuration and utilities.

photoclassify.photopath
~~~~~~~~~~~~~~~~~~~~~~~

.. automodule:: photoclassify.photopath
   :members:
   :undoc-members:
   :show-inheritance:

Path handling and manipulation for photo files.

Data Structures
---------------

Configuration
~~~~~~~~~~~~~

The configuration is handled through a YAML file with the following structure:

.. code-block:: yaml

   PATH:
     origin: str
     destination: str
     quarters: bool
   
   COPY:
     remove_from_sd: bool
     verbose: int
   
   DATE:
     day_starts_at: int
     process_after: str | None
     no_include_first: bool

Photo Metadata
~~~~~~~~~~~~~~

Photo metadata is stored in a dictionary with the following structure:

.. code-block:: python

   {
       'path': str,
       'hash': str,
       'size': int,
       'date': datetime,
       'metadata': dict
   } 