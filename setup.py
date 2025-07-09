from setuptools import setup, find_packages

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

setup(
    name='google_api_wrapper',
    version='0.1.0',
    packages=find_packages(),
    install_requires=requirements,
    author='Rushank Sheta',
    author_email='sheta.rushank@gmail.com',
    description='A wrapper for Google Forms and Drive APIs for data workflows',
    url='https://github.com/yourusername/google_api_wrapper',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.8',
)
