from setuptools import setup

setup(
        name='daisy',
        version='1.0',
        description='Block-wise task dependencies for luigi.',
        url='https://github.com/funkelab/daisy',
        author='Jan Funke',
        author_email='funkej@janelia.hhmi.org',
        license='MIT',
        packages=[
            'daisy',
            'daisy.ext',
            'daisy.persistence',
            'daisy.tcp',
            'daisy.messages'
        ],
        install_requires=[
            "numpy",
            "tornado>=5",
            "networkx",
            "pymongo"
        ]
)
