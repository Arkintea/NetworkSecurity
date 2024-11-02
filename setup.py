from setuptools import setup, find_packages
from typing import List

def read_requirements() -> List[str]:
    """
    Description: This function is going to return a list of requirements
    mentioned in requirements.txt file.
    Return: List of libraries specified in requirements.txt
    """
    try:
        with open('requirements.txt', 'r') as requirement_file:
            requirement_list = [
                requirement_name.strip()
                for requirement_name in requirement_file.readlines()
                # Ignore empty and commented lines
                if requirement_name.strip() and not requirement_name.startswith("#")
            ]
            if "-e ." in requirement_list:
                requirement_list.remove("-e .")
            # Debugging line to check requirements
            print("Requirements:", requirement_list)
            return requirement_list
    except FileNotFoundError:
        print("Error: requirements.txt file not found.")
        return []

setup(
    name='NetworkSecurity',
    version='0.0.1',
    description='A network security phishing data package',
    author='Akintayo Akinpelu',
    author_email='arkintea@gmail.com',
    packages=find_packages(),
    install_requires=read_requirements(),
)
