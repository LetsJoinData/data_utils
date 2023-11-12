from setuptools import setup

setup(name='data_utils',
      version='0.1',
      description='Base Python Utils for moving data to cloud databases',
      url='https://github.com/LetsJoinData/data_utils',
      author='Lets Join Data',
      author_email='leon@letsjoindata.com',
      license='MIT',
      packages=['data_utils/databases', 'data_utils/importer', 'data_utils/services', 'data_utils/sql_helpers'],
      install_requires=[
          'pandas',
          'snowflake-connector-python',
          'sqlalchemy',
          'snowflake-connector-python[pandas]',
          'snowflake-sqlalchemy',
          'pysftp',
          'retrying'
      ],
      zip_safe=False)