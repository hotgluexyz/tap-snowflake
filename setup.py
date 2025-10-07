#!/usr/bin/env python

from setuptools import setup

with open('README.md') as f:
    long_description = f.read()

setup(name='pipelinewise-tap-snowflake',
      version='3.1.0',
      description='Singer.io tap for extracting data from Snowflake - PipelineWise compatible',
      long_description=long_description,
      long_description_content_type='text/markdown',
      author="TransferWise",
      url='https://github.com/transferwise/pipelinewise-tap-snowflake',
      classifiers=[
          'License :: OSI Approved :: Apache Software License',
          'Programming Language :: Python :: 3 :: Only'
      ],
      py_modules=['tap_snowflake'],
      install_requires=[
            'certifi==2022.9.24',
            'cryptography==38.0.4; python_version == "3.7"',
            'pyOpenSSL==22.1.0',
            'pipelinewise-singer-python==1.*',
            'requests==2.22.0',
            'snowflake-connector-python==2.0.2; python_version == "3.7"',
            'snowflake-connector-python~=3.4.0; python_version == "3.10"',
            'pendulum==1.2.0'
      ],
      extras_require={
          'test': [
            'pylint==2.8.*',
            'pytest==6.2.*',
            'pytest-cov==2.12.*',
            'unify==0.5'
          ]
      },
      entry_points='''
          [console_scripts]
          tap-snowflake=tap_snowflake:main
      ''',
      packages=['tap_snowflake', 'tap_snowflake.sync_strategies'],
)
