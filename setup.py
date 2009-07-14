from setuptools import setup, find_packages
import sys, os

version = '0.1'

setup(name='out_of_band_cache',
      version=version,
      description="A beaker cache extension that supports out of band updates.",
      long_description="""\
""",
      classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      keywords='',
      author='Nick Pilon',
      author_email='npilon@oreilly.com',
      url='',
      license='',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          # -*- Extra requirements: -*-
          'beaker',
          'pmock',
      ],
      entry_points="""
      # -*- Entry points: -*-
      """,
      )
