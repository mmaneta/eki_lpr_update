from setuptools import setup

setup(
    name='lrp_update',
    version='0.0.3dev',
    packages=['lrp_update'],
    url='',
    license='',
    author='Marco Maneta',
    author_email='mmaneta@ekiconsult.com',
    description='',
    install_requires=["requests", "matplotlib", "jupyter", "pandas", "pyarrow", "numpy", "pypdf==4.2.0",  "fpdf2==2.7.9",  "pillow"]
)
