#!/bin/bash

# Change these lines if you don't want to use the main CRAN mirror.
# debian R upgrade
echo "deb http://cran.r-project.org/bin/linux/debian lenny-cran/" | sudo tee -a /etc/apt/sources.list
echo "deb-src http://cran.r-project.org/bin/linux/debian lenny-cran/" | sudo tee -a /etc/apt/sources.list

## test
echo "force-confold" | sudo tee -a  /etc/dpkg/dpkg.cfg
echo "force-confdef" | sudo tee -a  /etc/dpkg/dpkg.cfg

# add key to keyring so it doesn't complain 
gpg --keyserver pgp.mit.edu --recv-key 381BA480
gpg -a --export 381BA480 > jranke_cran.asc
sudo apt-key add jranke_cran.asc

# install the gfortran
sudo apt-get update
sudo apt-get install --yes gfortran-4.2

# install R using the FRONTEND call to eliminate 
# user interactive requests
sudo DEBIAN_FRONTEND=noninteractive apt-get install --yes --force-yes --no-install-recommends r-base 
sudo DEBIAN_FRONTEND=noninteractive apt-get install --yes --force-yes --no-install-recommends r-base-dev r-cran-hmisc


## rJava and latest Sun Java
sudo DEBIAN_FRONTEND=noninteractive apt-get install --yes --force-yes sun-java6-jdk sun-java6-jre r-cran-rjava 

## get rJava working, by any means possible
echo "### Hacked in to get rJava working ###" | sudo tee -a  /home/hadoop/.bashrc
echo "export JAVA_HOME=/usr/lib/jvm/java-6-sun/jre" | sudo tee -a  /home/hadoop/.bashrc
sudo env JAVA_HOME=/usr/lib/jvm/java-6-sun/jre R CMD javareconf

#install littler
 sudo apt-get install littler
# the apt-get install does not work right on 64 bit... so building from source. 
#cd /home/hadoop
#wget http://dirk.eddelbuettel.com/code/littler/littler-0.1.3.tar.gz
#tar zxvf littler-0.1.3.tar.gz
#cd littler-0.1.3
#./configure
#make
#sudo make install


#some packages have trouble installing without this link
sudo ln -s /usr/lib/libgfortran.so.3 /usr/lib/libgfortran.so

# for the package update script to run
# the user hadoop needs to own the R library
sudo chown -R hadoop /usr/lib/R/library


