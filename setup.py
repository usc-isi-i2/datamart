from distutils.core import setup


with open('requirements.txt', 'r') as f:
    install_requires = list()
    dependency_links = list()
    for line in f:
        re = line.strip()
        if re:
            if re.startswith('-e git+'):
                dependency_links.append(re)
            else:
                install_requires.append(re)


setup(name='Datamart',
      version='1.0dev',
      description='Data Augmentation',
      author='ISI',
      url='https://github.com/usc-isi-i2/datamart/tree/development',
      packages=['datamart'],
      install_requires=install_requires,
      dependency_links=dependency_links)