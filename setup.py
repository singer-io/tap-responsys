#!/usr/bin/env python

from setuptools import setup

setup(name='tap-responsys',
      version='0.0.1',
      description='Singer.io tap for extracting CSV files from Responsys via FTP',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_responsys'],
      install_requires=[
          'paramiko==2.4.1',
          'singer-encodings==0.0.3',
          'singer-python==5.1.5',
          'voluptuous==0.10.5',
          'pytz==2018.4'
      ],
      entry_points='''
          [console_scripts]
          tap-responsys=tap_responsys:main
      ''',
      packages=['tap_responsys'])
