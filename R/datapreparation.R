
prepare.data <- function(x, echo.Rmd=T, ...){
  stopifnot(length(x$file)>0)
  msg <- paste0("Preprocessing data read from *", x$file,"*")
  if(echo.Rmd){
    cat(paste0(msg,"  \r\n"))
    cat("\r\n```\r\n")
  }
  stopifnot(length(x$orig)>0)
  # Define columnnames, drop trailing/empty rows
  x$orig <- preprocess.orig(x$orig)
  # Basic preprocessing of columns (remove full NA cols for example)
  x$prep <- preprocess.data(x$orig, ...)
  if(echo.Rmd){
    cat("```\r\n")
    cat("Final column names of original dataframe:  \r\n")
    cat("\r\n```\r\n")
  }
  print(colnames(x$orig))
  if(echo.Rmd){
    cat("```\r\n")
  }
  return(x)
}

prepare.types <- function(x, 
                          trynumeric=F,
                          POSIXct.call=NULL,
                          POSIXct.format=NULL,
                          Date.format=NULL,
                          tocharacter=F,
                          col.names=colnames(x),
			              boolean.call=NULL,
                          col.ignore=c(),
						  test=T){
	if(is.numeric(col.ignore)){
		col.ignore=colnames(x)[col.ignore]
	}
	requireInSet(col.ignore, colnames(x), "Colums ignored in typechanged need to exist in dataframe.")

	if(is.numeric(col.names)){
		col.names=colnames(x)[col.names]
	}
  # make case insensitive
  requestedCols <- tolower(col.names)
  availableCols <- tolower(colnames(x))
	requireInSet(requestedCols, availableCols, "Typechanged columns need to exist in dataframe.")

	cols.included <- requestedCols
    if(length(col.ignore)>0){
      cols.included <- prob:::setdiff(cols.included, tolower(col.ignore))
    }
	if(length(cols.included)==0){
		stop(paste0("Typechange is not applied on any columns. Included cols=",
			        paste(collapse=", ",col.names),
                    ". Excluded cols=", paste(collapse=", ", col.ignore)))
	}
  # back to case sensitive
  cols.included <- colnames(x)[which(availableCols %in% cols.included)]
  # Now do change
	if(trynumeric){
		x <- columns.replace.and.add(x,
									 columns.trynumeric(x[, cols.included, drop=F], verbose=T, test=test))
	}
	if(!is.null(POSIXct.call)){
		POSIXct.call$x <- x[,cols.included, drop=F]
		POSIXct.call$verbose <- T
		POSIXct.call$test <- test
		x <- columns.replace.and.add(x,
									 do.call(columns.try.POSIXct, POSIXct.call))
	}
	if(length(POSIXct.format)>0){
		for(f in POSIXct.format){
		  x <- columns.replace.and.add(x,
                                   columns.try.POSIXct(x[, cols.included, drop=F], verbose=T, format=f, test=test))
		}
	}
  if(length(Date.format)>0){
    for(f in Date.format){
      x <- columns.replace.and.add(x,
                                   columns.try.Date(x[, cols.included, drop=F], verbose=T, format=f, test=test))
    }
  }
  if(tocharacter){
    for(col.name in cols.included){
      x[,col.name]<-as.character(x[,col.name])
    }
  }
  if(!is.null(boolean.call)){
      for(col.name in cols.included){
          boolean.call$x <- x[,col.name]
          boolean.call$verbose<-NULL #as.boolean has no verbosity
          tryCatch({
	      	x[,col.name] <- do.call(as.boolean, boolean.call)
	  	},
	 	 error=function(e)stop(paste0("Failed to cast column '",col.name,"' to boolean. ",e,"\r\n"))
	  )
		  cat(paste0("Casted column '", col.name, "' to boolean.\r\n"))
      }
  }
  return(x)
}

save.prepared <- function(x, connector, name=x$name, echo.Rmd=T, row.names=F){
  if(echo.Rmd){
    cat("\r\n```\r\n")
  }
  save(x, # $prep : The complete dataframe
       #$orig : The original dataframe, before any transformation was done
       #$file : The filename of the source
       file=paste0("./data/",name,".RData"))
  write.csv(x$prep, file=paste0("./output/",name,".csv"), row.names=row.names)
  export.save.safe(connector, x$prep, name, row.names=row.names)
  export.save.safe(connector, x$orig, paste0(name, "_orig"), row.names=row.names)
  if(echo.Rmd){
    cat("\r\n```\r\n")
    cat("\r\nSaved processed file originating from ", paste0("*",x$file,"*"),"in the database as table named ",paste0("**",name,"**"),"  \r\n")
  }
}

load.prepared <- function(name){
  load(file=paste0("./data/",name,".RData"))
  return(x)
}
