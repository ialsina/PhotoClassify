Usage Guide
===========

This guide will help you get started with PhotoClassify and explain its main features.

Basic Commands
--------------

Copy Photos
~~~~~~~~~~~

The ``copy`` command helps you safely transfer photos from one location to another:

.. code-block:: bash

   photoclassify copy /path/to/source /path/to/destination

Options:
   * ``--remove``: Remove photos from source after successful copy
   * ``--verbose``: Increase verbosity level
   * ``--quarters``: Organize photos by quarters
   * ``--day-starts-at``: Define when the day starts (for organization)
   * ``--process-after``: Only process photos after a specific date

Find Duplicates
~~~~~~~~~~~~~~~

The ``diff`` command helps you find duplicate or similar photos:

.. code-block:: bash

   photoclassify diff /path/to/source /path/to/destination

Options:
   * ``--type``: Type of comparison (candidates, twins, or both)
   * ``--level-one``: Use basic comparison only

Generate Statistics
~~~~~~~~~~~~~~~~~~~

The ``hist`` command generates statistics about your photo collection:

.. code-block:: bash

   photoclassify hist /path/to/photos

Options:
   * ``--nbins``: Number of bins for the histogram
   * ``--output``: Output file path

Configuration
-------------

PhotoClassify can be configured using a YAML configuration file. Create a ``config.yaml`` file:

.. code-block:: yaml

   PATH:
     origin: /path/to/source
     destination: /path/to/destination
     quarters: false
   
   COPY:
     remove_from_sd: false
     verbose: 0
   
   DATE:
     day_starts_at: 0
     process_after: null
     no_include_first: false

Examples
--------

Basic Photo Transfer
~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   # Copy photos from SD card to computer
   photoclassify copy /media/sdcard/DCIM /home/user/photos

   # Copy and organize by quarters
   photoclassify copy --quarters /media/sdcard/DCIM /home/user/photos

Find Duplicate Photos
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   # Find exact duplicates
   photoclassify diff --type twins /path/to/photos1 /path/to/photos2

   # Find potential duplicates
   photoclassify diff --type candidates /path/to/photos1 /path/to/photos2

Generate Collection Statistics
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   # Generate histogram of photo dates
   photoclassify hist --nbins 50 /path/to/photos

   # Save histogram to specific file
   photoclassify hist --output stats.png /path/to/photos

Advanced Usage
--------------

Parallel Processing
~~~~~~~~~~~~~~~~~~~

PhotoClassify supports parallel processing for better performance:

.. code-block:: bash

   # Disable parallel processing
   photoclassify copy --no-parallel /source /destination

   # Set number of workers
   photoclassify copy --max-workers 4 /source /destination

Date-based Organization
~~~~~~~~~~~~~~~~~~~~~~~

Customize how photos are organized by date:

.. code-block:: bash

   # Define day starts at 4 AM
   photoclassify copy --day-starts-at 4 /source /destination

   # Only process photos after specific date
   photoclassify copy --process-after "2023-01-01" /source /destination

Troubleshooting
---------------

Common Issues
~~~~~~~~~~~~~

1. **Permission Denied**
   - Ensure you have read/write permissions
   - Use ``sudo`` if necessary

2. **File System Errors**
   - Check disk space
   - Verify file system permissions
   - Ensure destination is writable

3. **Performance Issues**
   - Use ``--no-parallel`` for debugging
   - Adjust ``--max-workers`` based on system resources

Getting Help
~~~~~~~~~~~~

For additional help:

1. Use the ``--help`` flag with any command
2. Check the :doc:`API Reference <api>` documentation
3. Visit the `GitHub repository <https://github.com/yourusername/PhotoClassify>`_ 