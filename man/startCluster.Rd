% Generated by roxygen2 (4.1.0): do not edit by hand
% Please edit documentation in R/awsFunctions.R
\name{startCluster}
\alias{startCluster}
\title{Starts a cluster on Amazon's EMR service}
\usage{
startCluster(clusterObject)
}
\arguments{
\item{clusterObject}{cluster object to start}
}
\value{
a Job Flow ID
}
\description{
After a cluster has been defined with createCluster() this function actually
starts the machines running. Currently exported, but soon will be internal only.
}

