\name{createCluster}
\alias{createCluster}
\title{
Create a cluster by starting an EMR job and getting it ready to process input.
}
\description{
Creates all the local infrastructure for a cluster. This creates temp
directories locally, defins the elements of the cluster object.

Also creates the buckets on S3 for the cluster and uploads the bootstrap
code if needed. This function does everything except actually spin up
the EMR cluster. 
}
\usage{
createCluster(numInstances = 2, bootStrapLatestR = TRUE, cranPackages = NULL, enableDebugging = FALSE)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{numInstances}{
Number of desired nodes. This is limited by Amazon to 20 by default. 
}
  \item{bootStrapLatestR}{
Boolean (T or F) on whether the bootstrap script should run that
installs the latest version of R from CRAN on each node. This slows down
the starting of the nodes considerably. However, the default R on EMR
was the one passed to the Children of Isreal by Moses. 
}
  \item{cranPackages}{
Text vector of the names of any CRAN packages you desire loaded. Yes I know you have the
most beautiful little package you wrote yourself which you want to
install. This code does not support that... yet. Get your package on CRAN or fork my
fucking code and add that feature. ;)
}
  \item{enableDebugging}{
Thows Amazon EMR the debugging bit so you can log into the Amazon EMR
dashboard and see debugging information. This slows things down. And be
patient. Sometimes it takes 10 miutes for the debug data to populate the
dashboard. Don't gripe to me about debugging speed on AMZN. 
}
}
\value{
Returns an emrlapply() cluster object with appropriate fields
populated. Keep in mind that this creates the cluster but does not start
it. 
}

\author{
James "JD" Long
}
\seealso{
startCluster(), terminateCluster()
}
\examples{
myCluster   <- createCluster(numInstances=2, bootStrapLatestR=T, cranPackages=c("Hmisc", "plyr"))
}