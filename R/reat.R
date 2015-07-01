# Package to easily read and interpret data from disk to save them into a sql database
# Steps
# --setting up 'orig' dataset
# 1: read raw data from disk
# 2: annotate raw data: assign (unique) column names
# --setting up 'prep' dataset
# 3: clean unusable data : remove empty rows & cols
# 4: interpret data types
# 5: filter out unwanted rows
# --storing the prep dataset
# 6: save the rows

## All together now
load.data <- function(datadef, connector, echo.Rmd=T, save=T){
  stopifnot(!is.null(datadef))

  # Read data from the origin
  # Takes care of disk IO, determines HEADER(colnames), rows and values
  if(!is.null(datadef$origin)){
	  if(is.null(datadef$read)){
		  stop("No 'read' function provided in the datadef to read from the origin.")
	  }
	  datadef$file <- basename(datadef$origin)
 	  datadef$path <- datadef$origin # as long as not everything is refactored
	  datadef$orig <- datadef$read(datadef$origin)
	  if(is.null(datadef$meta) & !is.null(datadef$read.meta)){
	    datadef$meta <- datadef$read.meta(datadef$origin)
	  }
  }

  # Prepare the raw data from the origin
  # Remove unused columns/rows, create valid colnames
  datadef <- prepare.data(datadef, cols.keep=datadef$cols.keep, cols.map=datadef$cols.map, cols.drop=datadef$cols.drop, drop.cols=datadef$drop.cols)
  # Transform the prepared frame (infer types)
  if(length(datadef$typechanges)>0){
    if(echo.Rmd){
      cat("\r\n### Data preparation (standardization)  \r\n")
      cat("The following other data type changes have been applied;  \r\n")
    }
    for(prepargs in datadef$typechanges){
      cat("\r\n```\r\n")
      datadef$prep <- do.call(prepare.types, args = c(list(x=datadef$prep), prepargs))
      cat("```\r\n")
    }
  }
  if(length(datadef$transforms)>0){
	  if(echo.Rmd){
		  cat("\r\n### Data transforms and enrichments \r\n")
	  }
	  for(transforms in datadef$transforms){
		  datadef$prep <- transforms(datadef$prep)
	  }
  }

  # Filter rows
  if(length(datadef$filter)>0){
    if(echo.Rmd){
      cat("\r\n### Data filtering  \r\n")
      cat("The following filters have been applied;  \r\n")
    }
    for(filterargs in datadef$filter){
      requireInSet(c("col.name","value","type","keep"), names(filterargs))
      if(length(filterargs$col.name)==0){
        # if no names provided, use all names except the _orig _failed ones.
        used_names <- colnames(datadef$prep)
        used_names <- used_names[!grepl("_orig", used_names, fixed=T)]
        used_names <- used_names[!grepl("_failed", used_names, fixed=T)]
        filterargs$col.name <- used_names
      }
      rows_before <- nrow(datadef$prep)
      datadef$prep <- do.call(filter.rows, args = c(list(x=datadef$prep), filterargs))
      rows_after <- nrow(datadef$prep)
      
      action<-
        if(filterargs$keep)
          paste0("Keeping ", rows_after)
        else
          paste0("Dropping ", rows_before-rows_after)
      cat(action,"rows for which",
          paste0("'",filterargs$col.name,"'"),
          "matches ",
          paste0("'",filterargs$value,"'"),
          "using",
          filterargs$type,
          ". This results in ",
          rows_after,
          "rows out of the initial",
          rows_before
          ,"  \r\n")
    }
  }

  if(echo.Rmd){
    cat("Resulting data characteristics:  \r\n")
    cat("\r\n```\r\n")
    print(summary(datadef$prep))
    cat("```\r\n")
  }

	default_constants <- as.data.frame(list(source_file=datadef$file))
	if(!is.null(datadef$constants)){
	  datadef$constants<-cbind(as.data.frame(datadef$constants),
							   default_constants
							   )
	}else{
	  datadef$constants <- default_constants
	}
	colnames(datadef$constants) <- names(datadef$constants)
	if(echo.Rmd){
	  cat("\r\nAdding the following constants to all rows:  \r\n")
	  cat("\r\n```\r\n")
	  print(datadef$constants, row.names=F)
	  cat("```\r\n")
	}
	if(nrow(datadef$prep)>0)
		datadef$prep<-merge(datadef$prep, datadef$constants, by=c())
	else
		datadef$prep <- cbind(datadef$prep, datadef$constants[c(),])
	colnames(datadef$prep)[ncol(datadef$prep)-col(datadef$constants)+(1:ncol(datadef$constants))] <- colnames(datadef$constants)

  # Enrich dataframe with constant meta data
  if(!is.null(datadef$meta)){
    if(echo.Rmd){
      cat("\r\nGot the following meta data constant for all rows:  \r\n")
      cat("\r\n```\r\n")
      print(datadef$meta, row.names=F)
      cat("```\r\n")
    }
    datadef$prep<-merge(datadef$prep, datadef$meta, by=c())
  }

  # Write the prepared data to the destination
  if(is.null(datadef$destination)){
	  datadef$destination <- connector
  }
  
  if(save){
	  save.data(datadef, echo.Rmd)
  }
  return(datadef)
}

save.data <- function(datadef, echo.Rmd=T){
	if(echo.Rmd){
		cat("\r\n## Saving to disk and database  \r\n")
	}
	if(is.null(datadef$row.names)){
		datadef$row.names<-FALSE
	}
	if(is.null(datadef$destination)){
		stop("No database connector stored in the datadefs 'destination'")
	}
    save.prepared(datadef, datadef$destination, datadef$name, echo.Rmd=T, row.names=datadef$row.names)
}
