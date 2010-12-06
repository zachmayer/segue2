#!/bin/bash

# Change these lines if you don't want to use the Iowa State CRAN mirror.
#debian R upgrade
echo "deb http://streaming.stat.iastate.edu/CRAN/bin/linux/debian lenny-cran/" | sudo tee -a /etc/apt/sources.list
echo "deb-src http://streaming.stat.iastate.edu/CRAN/bin/linux/debian lenny-cran/" | sudo tee -a /etc/apt/sources.list

#add key to keyring so it doesn't complain 
#this is optional
gpg --keyserver pgp.mit.edu --recv-key 381BA480
gpg -a --export 381BA480 > jranke_cran.asc
sudo apt-key add jranke_cran.asc

sudo apt-get update
sudo apt-get -t lenny-cran install --yes --force-yes r-base r-base-dev

