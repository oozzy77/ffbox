from setuptools import setup, find_packages

setup(
    name='ffbox',
    version='0.1',
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'ffbox=ffbox.cli:main',
        ],
    },
    include_package_data=True,
    package_data={
        'ffbox': ['cp_venv_to_portable.sh'],  # Include the shell script
    },
    install_requires=[
        'boto3',
        'fusepy',
        'xattr',
    ],
)