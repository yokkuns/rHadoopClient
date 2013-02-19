\name{read.hdfs}
\alias{read.hdfs}
\title{
Read data by HDFS
}
\description{
Read data by HDFS.
}
\usage{
read.hdfs(hdfs.path, hadoop.cmd = "hadoop")
}
\arguments{
  \item{hdfs.path}{
    hdfs path
}
  \item{hadoop.cmd}{
    hadoop command. default is "hadoop"
}
}
\details{

}
\value{
A data.frame
}
\references{

}
\author{
Yohei Sato
}
\note{

}

\seealso{

}
\examples{
\dontrun{
dummy.data <- read.hdfs("./dummy")
}
}
\keyword{ ~kwd1 }

