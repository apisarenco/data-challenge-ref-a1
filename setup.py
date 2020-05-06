from setuptools import setup, find_packages


def get_long_description():
    with open('README.md') as f:
        return f.read()


setup(
    name='pynger',
    version='0.0.1',

    description="Python pinger",
    long_description=get_long_description(),
    long_description_content_type='text/markdown',

    python_requires='~=3.6',

    install_requires=[
        'urllib3',
        'pytz',
        'pykafka'
    ],

    dependency_links=[
    ],

    extras_require={
        'test': [
            'pytest',
            'pytest-cov'
        ]
    },

    packages=find_packages(),

    author='Alexandru Pisarenco',
    license='MIT',

    entry_points={
        'console_scripts': [
            'pynger = pynger:main'
        ],
    }
)
