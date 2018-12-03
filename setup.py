from setuptools import setup
import subprocess
try:
    import base_string as string_types
except ImportError:
    string_types = str

extras_require = {
}


dep_set = set()
for value in extras_require.values():
    if isinstance(value, string_types):
        dep_set.add(value)
    else:
        dep_set.update(value)

extras_require['full'] = list(dep_set)

setup(
        name='daisy',
        version='0.1',
        description='Block-wise task dependencies for luigi.',
        url='https://github.com/funkey/daisy',
        author='Jan Funke',
        author_email='jfunke@iri.upc.edu',
        license='MIT',
        packages=[
            'daisy',
            'daisy.ext'
        ],
        install_requires=[
            "numpy",
            "tornado"
        ],
        extras_require=extras_require,
)
