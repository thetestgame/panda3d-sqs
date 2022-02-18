try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

long_description = '''Panda3D module for interacting and integrating with AWS SQS hooks'''

setup(
    name='panda3d_sqs',
    description='Panda3D module for interacting and integrating with AWS SQS hooks',
    long_description=long_description,
    license='MIT',
    version='1.0.0',
    author='Jordan Maxwell',
    maintainer='Jordan Maxwell',
    url='https://github.com/NxtStudios/p3d-sqs',
    packages=['panda3d_sqs'],
    install_requires=['boto3'],
    classifiers=[
        'Programming Language :: Python :: 3',
    ])
