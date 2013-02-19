\name{read.hive}
\alias{read.hive}
\title{
Reading the execution result of HiveQL
}
\description{
Reading the execution result of HiveQL
}
\usage{
read.hive(sql, hive.cmd = "hive")
}
\arguments{
  \item{sql}{
    SQL String
}
  \item{hive.cmd}{
    Hive command. default is "hive"
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
dummy.data <- read.hdfs("select * from table_name")
}
}
\keyword{ ~kwd1 }
