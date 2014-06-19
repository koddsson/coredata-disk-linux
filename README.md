Coredata Linux Virtual Disk
===========================

A very basic FUSE implementation against the coredata API.

Feel free to try it out and report back if it's missing features you'd like! 
Features are slowly but surely being added.

Usage
-----
Create a `config.py` in the root of this project with the following info ...
```
hostname = 'https://<hostname>/'
username = '<username>'
password = '<password>'
```
... and then execute the following commands in the terminal
```
mkvirtualenv coredata-linux-disk
pip install -r requirements.txt
python disk.py <mountpoint>
```
