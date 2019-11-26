from setuptools import setup, find_packages

setup(name='paceutils',
      version='0.1',
      py_modules=["paceutils"],
      description='PACE healthcare functions',
      long_description='functions to calculate PACE healthcare indicators and dataframes',
      classifiers=[
          'Development Status :: 3 - Alpha',
          'License :: OSI Approved :: MIT License',
          'Programming Language :: Python :: 3.7',
          'Topic :: Text Processing :: Linguistic',
      ],
      keywords='funniest joke comedy flying circus',
      url='http://github.com/storborg/funniest',
      author='SNelson',
      author_email='snelson_hc@gmail.com',
      license='MIT',
      packages=find_packages(),
      install_requires=[
          'markdown', 'pandas'
      ],
      include_package_data=False,
      zip_safe=False)
