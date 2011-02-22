#!/bin/bash

# Change these lines if you don't want to use the main CRAN mirror.
# debian R upgrade
echo "deb http://cran.r-project.org/bin/linux/debian lenny-cran/" | sudo tee -a /etc/apt/sources.list
echo "deb-src http://cran.r-project.org/bin/linux/debian lenny-cran/" | sudo tee -a /etc/apt/sources.list

## test
echo "force-confold" | sudo tee -a  /etc/dpkg/dpkg.cfg
echo "force-confdef" | sudo tee -a  /etc/dpkg/dpkg.cfg
export DEBIAN_FRONTEND=noninteractive

# add key to keyring so it doesn't complain 
gpg --keyserver pgp.mit.edu --recv-key 381BA480
gpg -a --export 381BA480 > jranke_cran.asc
sudo apt-key add jranke_cran.asc

# install the gfortran
sudo apt-get update
sudo apt-get install --yes gfortran-4.2

## issues with libc
sudo /etc/init.d/mysql stop
sudo /etc/init.d/exim4 stop
sudo /etc/init.d/cron stop
sudo mv /etc/init.d/cron /etc/init.d/cron.bak
sudo apt-get install --yes --force-yes libc6
sudo mv /etc/init.d/cron.bak /etc/init.d/cron
sudo /etc/init.d/mysql start
sudo /etc/init.d/exim4 start
sudo /etc/init.d/cron start

## now that libc6 is installed R should install
## the libc6 issue was a total pain in my ass
## Amazon needs to provide custom AMIs for EMR
sudo apt-get install --yes --force-yes r-base r-base-dev
sudo apt-get install --yes r-cran-hmisc
sudo apt-get install --yes r-cran-rjava

#install littler
sudo apt-get install littler

#some packages have trouble installing without this link
sudo ln -s /usr/lib/libgfortran.so.3 /usr/lib/libgfortran.so

# for the package update script to run
# the user hadoop needs to own the R library
sudo chown -R hadoop /usr/lib/R/library

