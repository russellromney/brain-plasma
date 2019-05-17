from setuptools import setup, find_packages


setup(
    name = 'brain-plasma',
    version = '0.1.3',
    description = 'Share python between callbacks in Dash, using Apache Plasma',
    long_description = 'A basic API on top of the Apache Plasma in-memory object store, allowing you to share data locally between threads, processes, or programs that do not share memory',
    keywords = ' dash plasma callbacks plotly apache arrow pandas numpy redis',
    url = 'https://github.com/russellromney/brain-plasma',
    author = 'Russell Romney',
    author_email = 'russellromney@gmail.com',
    license = 'MIT',
    packages = find_packages(),
    install_requires = [
        'pyarrow>=0.13.0',
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    include_package_data = False,
    zip_safe = False
)
