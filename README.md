[![Build Status](https://travis-ci.org/zachmayer/segue2.png?branch=master)](https://travis-ci.org/zachmayer/segue2)
[![Coverage Status](https://coveralls.io/repos/zachmayer/segue2/badge.svg)](https://coveralls.io/r/zachmayer/segue2)
### Segue2: 
##Abusing Amazon Elastic Map reduce for embarrassingly parallel R jobs
Note that this package depends on [RAmazonS3](http://www.omegahat.org/RAmazonS3/), which is not on CRAN:
```{R}
install.packages("RAmazonS3", repos="http://www.omegahat.org/R", type="source")
```