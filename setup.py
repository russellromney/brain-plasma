from setuptools import setup, find_packages


setup(
    name = 'dash-brain',
    version = '0.1',
    description = 'Share python between callbacks in Dash, using Apache Plasma',
    long_description = 'A basic API on top of the Apache Plasma in-memory object store, allowing you to share data locally between threads, processes, or programs that do not share memory',
    keywords = ' dash plasma callbacks plotly',
    url = 'https://github.com/russellromney/dash-plasma',
    author = 'Russell Romney',
    author_email = 'russellromney@gmail.com',
    license = 'MIT',
    packages = find_packages(),
    install_requires = [
        'pyarrow',
    ],
    include_package_data = False,
    zip_safe = False
)