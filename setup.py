#!/usr/bin/env python
from setuptools import setup


setup(name='aio-selectel-api',
      version='0.0.1',
      description='Simple selectel async API',
      author='Vasiliy Sidorov',
      author_email='vasili.sidorov@gmail.com',
      url='https://github.com/vasili-ii/aio-selectel-api',
      packages=['selectel'],
      install_requires=['asyncio >= 3.4.3', 'aiodns >= 2.0.0',
                        'aiohttp >= 1.2.1'],
      license='MIT',
      classifiers=['Intended Audience :: Developers',
                   'Topic :: Software Development :: Libraries',
                   'Development Status :: 3 - Alpha',
                   'Programming Language :: Python',
                   'Programming Language :: Python :: 3.5',
                   'Programming Language :: Python :: 3.6',
                   'Programming Language :: Python :: 3.7',
                   ])
