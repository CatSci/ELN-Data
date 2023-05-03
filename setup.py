from setuptools import find_packages, setup
from typing import List

HYPEN_E_DOT = '-e .'

def get_requirements()->List[str]:
    """Returns a list of requirements"""

    requirements_list:List[str] = []
    with open('requirements.txt') as f:
        requirements_list = f.readlines()
        requirements_list = [req.replace("\n", "") for req in requirements_list]
        
        if HYPEN_E_DOT in requirements_list:
            requirements_list.remove(HYPEN_E_DOT)

    return requirements_list


setup(
    name = 'ELNdata',
    version = '0.0.1',
    author = 'CatSci',
    author_email = 'atul.yadav@catsci.com',
    packages = find_packages(),
    install_requires = get_requirements(), #[],
)