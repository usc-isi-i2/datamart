from setuptools import setup


with open('requirements.txt', 'r') as f:
    install_requires = list()
    dependency_links = list()
    for line in f:
        re = line.strip()
        if re:
            install_requires.append(re)

print(dependency_links)
setup(name='Datamart',
      version='1.0dev',
      description='Data Augmentation',
      author='ISI',
      url='https://github.com/usc-isi-i2/datamart/tree/development',
      packages=['datamart',
                'datamart.es_managers',
                'datamart.joiners', 
                'datamart.materializers',
                'datamart.metadata',
                'datamart.profilers',
                'datamart.utilities',
               ],
      install_requires=install_requires,

      dependency_links=dependency_links)
