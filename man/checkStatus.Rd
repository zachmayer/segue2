\name{checkStatus}
\alias{checkStatus}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Check the status of an EMR Job
}
\description{
Sends a request to EMR and returns the status of a job. checkStatus()
only checks the status once. 
}
\usage{
checkStatus(jobFlowId)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{jobFlowId}{
The jobFlowId is an EMR convention of a given job. This is an element of
a cluster in emrlapply once the cluster has been started.
}
}
\value{
Returns a string value of the status.
}
\references{
%% ~put references to the literature/web site here ~
}
\author{
James "JD" Long
}
\seealso{
getFinalStatus()
}

}
