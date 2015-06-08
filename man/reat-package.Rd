\name{reat-package}
\alias{reat-package}
\alias{reat}
\docType{package}
\title{
R Eat Data
}
\description{
Makes life easy loading flat files into a database.

Given a data definition list object, this package reads, assigns types, filters rows and columns, and stores them in a database. 

The data definition allows automatic type inferrences for all or some columns, making it easy to load a lot of columns strongly typed. Some types, for example dates, also allow fall-back formats. All original (text) columns will still be available in the data base table for tracing purpose. The code is designed to run within Rmd files, writing a log of done transformations to the console.
}
\details{
\tabular{ll}{
Package: \tab reat\cr
Type: \tab Package\cr
Version: \tab 1.0\cr
Date: \tab 2015-06-05\cr
License: \tab LGPL\cr
}

}
\author{
Gerben Oostra

Maintainer: Gerben Oostra <gerben.oostra@trifinance.nl>
}
\references{
}
\keyword{ package }
\seealso{
}
\examples{
rule_constants <- list(
  					ExtraCol1 = "Abc",
						ExtraCol2 = 12.4,
						load_date = as.Date("2015-02-01")
						)
data_def <- list(
      		  origin= "inputdata\\dataset.csv",
						read=function(x) read.csv(x, stringsAsFactors=F, na.strings=c()),
						name = "tablename",
						drop.cols = F,
						constants=rule_constants,
  					typechanges=list(
											list(test=F, tocharacter=T, col.names=c("bankaccountnr", "free field")),
										  list(test=F, col.names=c("modificationDate", "loadDate", "executionDate"), Date.format = "\%Y-\%m-\%d")
						)
rules <- load.data(data_def, connector=sourcedata_connect)

}
