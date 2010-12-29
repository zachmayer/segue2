#!/bin/bash

# Change these lines if you don't want to use the main CRAN mirror.
# debian R upgrade
echo "deb http://cran.r-project.org/bin/linux/debian lenny-cran/" | sudo tee -a /etc/apt/sources.list
echo "deb-src http://cran.r-project.org/bin/linux/debian lenny-cran/" | sudo tee -a /etc/apt/sources.list

# add key to keyring so it doesn't complain 
gpg --keyserver pgp.mit.edu --recv-key 381BA480
gpg -a --export 381BA480 > jranke_cran.asc
sudo apt-key add jranke_cran.asc

# install the latest R
sudo apt-get update
sudo apt-get -t lenny-cran install --yes --force-yes r-base r-base-dev

#install littler
sudo apt-get install littler

# for the package update script to run
# the user hadoop needs to own the R library
sudo chown -R hadoop /usr/lib/R/library

