from setuptools import setup, find_packages
import sys, os

version = '0.9'

setup(name='out_of_band_cache',
      version=version,
      description="A beaker cache extension that supports out of band updates. Also includes Redis Cache Manager.",
      long_description="""\
""",
      classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      keywords='',
      author='Nick Pilon',
      author_email='npilon@oreilly.com',
      url='http://github.com/oreillymedia/out_of_band_cache',
      license='BSD',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          # -*- Extra requirements: -*-
          'beaker>=1.5.4dev',
          'pmock',
	  'redis',
      ],
      entry_points="""
          [beaker.backends]
          ext:redis = out_of_band_cache.redis_manager:RedisNamespaceManager
      """,
      )
