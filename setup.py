from setuptools import setup, find_packages
  
with open('requirements.txt') as f:
    requirements = f.readlines()
  
long_description = 'combine_gtfs_feeds is a command line tool \
    to combine multiple gtfs feeds into a single feed/dataset. \
    The main purpose of combine_gtfs_tools is to be able work \
    from one GTFS feed when performing transit service analysis \
    for a particular geographic location.'
  
setup(
        name ='combine_gtfs_feeds',
        version ='0.1.0',
        author ='psrc staff',
        author_email ='scoe@psrc.org',
        url ='https://github.com/psrc/combine_gtfs_feeds',
        description ='Package to combine GTFS feeds.',
        long_description = long_description,
        long_description_content_type ="text/markdown",
        license ='MIT',
        packages = find_packages(),
        entry_points ={
            'console_scripts': [
                'combine_gtfs_feeds = combine_gtfs_feeds.cli.main:main'
            ]
        },
        classifiers =[
            "Programming Language :: Python :: 3",
            "License :: OSI Approved :: MIT License",
            "Operating System :: OS Independent",
        ],
        keywords ='GTFS',
        install_requires = requirements,
        zip_safe = False
)
