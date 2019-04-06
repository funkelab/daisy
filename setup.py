from setuptools import setup

setup(
        name='daisy',
        version='0.3',
        description='Block-wise task dependencies for luigi.',
        url='https://github.com/funkelab/daisy',
        author='Jan Funke',
        author_email='funkej@janelia.hhmi.org',
        license='MIT',
        packages=[
            'daisy',
            'daisy.ext',
            'daisy.persistence'
        ],
        install_requires=[
            "numpy",
            "tornado>=5,<6",
            "networkx",
            "pymongo"
        ]
)
