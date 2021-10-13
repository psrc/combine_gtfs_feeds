from setuptools import setup, find_packages
  
with open('requirements.txt') as f:
    requirements = f.readlines()
  
long_description = 'Sample Package made for a demo \
      of its making for the GeeksforGeeks Article.'
  
setup(
        name ='combine_gtfs_feeds',
        version ='1.0.0',
        author ='Stefan Coe',
        author_email ='scoe@psrc.org',
        url ='https://github.com/Vibhu-Agarwal/vibhu4gfg',
        description ='Package to consolidate GTFS feeds.',
        long_description = long_description,
        long_description_content_type ="text/markdown",
        license ='MIT',
        packages = find_packages(),
        entry_points ={
            'console_scripts': [
                'combine_gtfs_feeds = combine_gtfs_feeds.cli.main:main'
            ]
        },
        classifiers =(
            "Programming Language :: Python :: 3",
            "License :: OSI Approved :: MIT License",
            "Operating System :: OS Independent",
        ),
        keywords ='GTFS',
        install_requires = requirements,
        zip_safe = False
)
