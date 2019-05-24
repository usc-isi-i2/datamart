from setuptools import setup
from setuptools.command.install import install


class PostInstallCommand(install):
    def run(self):
        import subprocess
        command_add = "python -m spacy download en_core_web_sm"
        p = subprocess.Popen(command_add, stdout=subprocess.PIPE, shell=True, stderr=subprocess.STDOUT)
        while p.poll() == None:
            out = p.stdout.readline().strip()
            if out:
                print (bytes.decode(out))
        install.run(self)

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
                'datamart.joiners.join_feature', 
                'datamart.materializers',
                'datamart.materializers.parsers',
                'datamart.metadata',
                'datamart.profilers',
                'datamart.profilers.helpers',
                'datamart.utilities',
                'datamart.resources',
               ],
      package_dir={'datamart': 'datamart'},
      package_data={'datamart': ['resources/*.json','resources/*.csv']},

      install_requires=install_requires,
      dependency_links=dependency_links,
      cmdclass={
          'install': PostInstallCommand
      }
      )
