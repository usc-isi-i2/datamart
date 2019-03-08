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
      version='1.1',
      description='Data Augmentation',
      author='ISI',
      url='https://github.com/usc-isi-i2/datamart/tree/master',
      packages=['datamart'],
      install_requires=install_requires,
      dependency_links=dependency_links)
