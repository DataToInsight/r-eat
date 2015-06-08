library(RODBC)

sqlDropTables <- function(dbhandle_create, tablenameregex){
    result_tables <- sqlTableDetails(dbhandle_create, schema="dbo")$TABLE_NAME
    matching_tables <- result_tables[grepl(tablenameregex, result_tables)]
    dbconn <- dbhandle_create()
    for(matching_table in matching_tables){
      sqlDrop(dbconn, matching_table, errors=FALSE) 
    }
    close(dbconn)
}

# Writes a table in an ODBC database. If the table already exists, it is first removed
# Complements RODBC with compatibility with Date, POSIXt, logical and double precision data types.
sql.replace<-function(dbconn, dat, tablename = NULL, replace=T, tight=T, row.names=T,...){
  # get the name of the variable passed as dat, taken from sqlSave implementation
  if (is.null(tablename)) 
    tablename <- if (length(substitute(dat)) == 1) 
      as.character(substitute(dat))
  else as.character(substitute(dat)[[2L]])
  if (length(tablename) != 1L) 
    stop(sQuote(tablename), " should be a name")
  
  if(replace)
    sqlDrop(dbconn, tablename, errors=FALSE)
  varTypes <- sapply(dat, function(x){
    if(is.numeric(x)){
      "float"
      }else if(is.factor(x)|is.character(x)){
        if(!tight)
          "[varchar](max)"
        else paste0("[varchar](",if(length(x)==0){32}else{
          varcharl<-max(1,max(nchar(as.character(x)), na.rm=T))
          },")")
        } else if(is.logical(x)){
          "BIT"
          }else if("Date"%in%class(x)) {
          "date"
          } else if ("POSIXt"%in%class(x)){
            "datetime"
            }else{NA}})# let RODBC determine
  varTypes <- varTypes[!is.na(varTypes)]
  dat2<-sapply(dat, function(x){
    if(is.numeric(x)){
      x2<-sprintf("%.15e",x)
      if(sum(is.na(x))>0)
        x2[is.na(x)]<-NA
      x2
    } else if(is.logical(x)){
      x2<-rep(0,length(x))
      x2[!is.na(x) & x==T]<-1
      if(sum(is.na(x))>0)
        x2[is.na(x)] <- NA
      x2
      } else as.character(x)
  })
  if(is.null(dim(dat2))){ #sapply returns vector when dat has nrow 0
    dat2<-t(dat2)
  }
  dat2 <- as.data.frame(dat2)
  if(nrow(dat)==0){
	  dat2[,varTypes=='float'] <- sprintf("%.15e", 1.0)
	  dat2[,varTypes=='BIT'] <- 1
	  dat2[,varTypes=='date'] <- as.character(as.Date('01-01-2001'))
	  dat2[,varTypes=='datetime'] <- as.character(Sys.time())
  }
  # sqlSave only works for dataframes of >=1 row
  if(nrow(dat)>0 || replace)
    sqlSave(dbconn, dat2, tablename, fast=FALSE, varTypes=varTypes, append=!replace, rownames=row.names, ...)
  if(nrow(dat)==0 && replace){ # the transpose function causes the list of 0 length vectors te become a matrix with 1 row, which is nice because sqlSave cannot deal with empty dataframes
	  query=paste0("DELETE FROM [", tablename, "] WHERE 1=1 ")
      sqlQuery(dbconn, query, errors = F)
  	  sql_errors <- odbcGetErrMsg(dbconn)
	  if(length(sql_errors)>0){
		warning(sql_errors)
		stop(paste(sep="\r\n",
				   "Failed to execute query:",
				   query,
				   "RODBC returned the following errors:",
				   paste0(collapse="\r\n", sql_errors)))
	  }
  }
}

sqlReplace <- function(dbhandle_create, dat, tablename=NULL, row.names=F, replace=T, tight=T){
	dbconn <- dbhandle_create()
	tryCatch({
  		sql.replace(dbconn, dat, tablename = tablename, row.names=row.names, replace=replace, tight=tight)
	},
	finally=close(dbconn)
	)
}

# Saves a dataframe to the database, overwriting the table if it already exists
export.save <- function(dbhandle, dat, tablename=NULL, row.names=F, replace=T, tight=T){
  # get the name of the variable passed as dat, taken from sqlSave implementation
  if (is.null(tablename)) 
    tablename <- if (length(substitute(dat)) == 1) 
      as.character(substitute(dat))
  else as.character(substitute(dat)[[2L]])
  if (length(tablename) != 1L) 
    stop(sQuote(tablename), " should be a name")
  sql.replace(dbhandle, dat, tablename, row.names=row.names, replace=replace, tight=tight)
}

# Loads a dataframe stored with export.save back from the database
export.load <- function(dbhandle, name, row.names=F){
  dat<-sqlFetch(dbhandle, name, stringsAsFactors=F, rownames=row.names)
  dat<-as.data.frame(stringsAsFactors=F, lapply(dat, function(x){
    if(is.character(x)|is.factor(x)){
      # Test if character columns can be converted to dates without losing any information
      x_date <- as.Date(as.character(x), "%Y-%m-%d")
      if( sum(is.na(x_date))==sum(is.na(x))){ #no additional NA's, prevents length differences on next call:
            # ignore NA's to not get NA as sum result
        if(sum(as.character(x_date[!is.na(x_date)])!=as.character(x[!is.na(x)]))==0){ #No lost information (hours/minutes/seconds/miliseconds)
          return(x_date)
        } else{
          x
        }
      }else{
        x
      }
    }else{
      x
    }
  }))
  return(dat)
}

export.load.safe <- function(dbhandle_create_func, name, row.names=F){
  dbconn <- dbhandle_create_func()
  tryCatch({
	  df_fetch<-export.load(dbconn, name, row.names=row.names)
  },
  finally= close(dbconn)
  )
  df_fetch
}


# Saves the export-dataframe
export.save.safe <- function(dbhandle_create_func, df, name, row.names=F){
  # Store to database
  dbconn <- dbhandle_create_func()
  tryCatch({
	  export.save(dbconn, df, tablename = name, row.names=row.names)
  },
  finally= close(dbconn)
  )
  
  # Verify that data is correctly stored
  df_fetch <- export.load.safe(dbhandle_create_func, name, row.names=row.names)
  
  # When fetched, the order is unknown, so sort them to get corresponding elements on same row.
  # The rownames are set to be NULL to ignore them
  df_orig <- sort.dataframe(df)
  rownames(df_orig) <- NULL
  df_fetch <- sort.dataframe(df_fetch)
  rownames(df_fetch) <- NULL
  
  all.equal.describe(df_orig, sort.dataframe(df_fetch))
  stopifnot(nrow(df_orig)==nrow(df_fetch))
  
  mismatching_colnames<-subset(
    data.frame(names_orig=colnames(df),
               names_sql_expected=make.names.sql(colnames(df)),
               names_sql=colnames(df_fetch)),
    as.character(names_sql_expected)!=as.character(names_sql))
  if(nrow(mismatching_colnames)>0){
    print(mismatching_colnames, row.names=F)
  }
}

sqlSelect <- function(query, connect, stringsAsFactors=default.stringsAsFactors()){
  db_handle <- connect()
  tryCatch({
	  results <- sqlQuery(db_handle, query, errors = F, stringsAsFactors=stringsAsFactors) #Errors=F to be able to check errors with odbcGetErrMsg
	  sql_errors <- odbcGetErrMsg(db_handle)
  },
  finally= close(db_handle)
  )
  if(length(sql_errors)>0){
    warning(sql_errors)
    stop(paste(sep="\r\n",
               "Failed to execute query:",
               query,
               "RODBC returned the following errors:",
               paste0(collapse="\r\n", sql_errors)))
  }
  results
}
sqlTableDetails <- function(connect, ...){
  db_handle <- connect()
  tryCatch({
	  results <- sqlTables(db_handle, errors = F, ...)
	  sql_errors <- odbcGetErrMsg(db_handle)
  },
  finally= close(db_handle)
  )
  if(length(sql_errors)>0){
    warning(sql_errors)
    stop(paste(sep="\r\n",
               "Failed to retrieve tables",
               "RODBC returned the following errors:",
               paste0(collapse="\r\n", sql_errors)))
  }
  results
}
#Returns all table, column combinations in the database
sqlTableColls <- function(connect, ...){
  result_tables <- sqlTableDetails(connect, ...)$TABLE_NAME
  result_cols <- lapply(result_tables, 
                        function(table){
                          colnames(
                            sqlSelect(
                              paste0("select top 1 * from [",table,"]"),
                              connect
                            ))})
  table_cols<- data.frame(
                  TABLE_NAME=rep(result_tables, sapply(result_cols, length)),
                  COL_NAME=as.character(unlist(result_cols)),
                  stringsAsFactors=F
                  )
  return(table_cols)
}

sqlDropView <- function (channel, sqtable, errors = TRUE) 
{
  if (!odbcValidChannel(channel)) 
    stop("first argument is not an open RODBC channel")
  if (missing(sqtable)) 
    stop("missing argument 'sqtable'")
  dbname <- odbcTableExists(channel, sqtable, abort = errors)
  if (!length(dbname)) {
    if (errors) 
      stop("table ", sQuote(sqtable), " not found")
    return(invisible(-1L))
  }
  res <- sqlQuery(channel, paste("DROP VIEW", dbname), errors = errors)
  if (errors && (!length(res) || identical(res, "No Data"))) 
    invisible()
  else invisible(res)
}

# Runs the query in the file located at sql_file_path.
# provide a list of table names (or just 1) for replace to indicate which tables need to be removed in advance
sqlFile <- function(dbhandle_create_func, sql_file_path, views=NULL, tables=NULL, verbose=F, strip_comments=T){
  if(!file.exists(sql_file_path)){
    stop(paste0("There is no file at ", sql_file_path))
  }
  query<-paste(collapse="\r\n",readLines(sql_file_path, encoding="UTF-8-BOM"))
  for(s in c("\xff"#when there is no UTF-8 BOM
             )){
    query<<-gsub(s, "", query, fixed=T)
  }
  
  if(length(views)>0){
    for(view in views){
      dbCon <- dbhandle_create_func()
  	  tryCatch({
		  print(attr(dbCon, "connection.string"))
		  drop_view_query <- paste("DROP VIEW", views)
		  if(verbose){
			cat(drop_view_query,"\r\n")
		  }
		  sqlQuery(dbCon, drop_view_query, errors=F)
		  odbcClearError(dbCon) # ignoring 
	  },
	  finally= close(dbCon)
	  )
    }
  }
  if(length(tables)>0){
    for(table in tables){
      dbCon <- dbhandle_create_func()
      tryCatch({
		  print(attr(dbCon, "connection.string"))
		  drop_table_query <- paste("DROP TABLE", table)
		  if(verbose){
			cat(drop_table_query,"\r\n")
		  }
		  sqlQuery(dbCon, drop_table_query, errors=F)
		  odbcClearError(dbCon) # ignoring 
	  },
	  finally= close(dbCon)
	  )
    }
  }
  results <- NULL
  for(subquery in strsplit(query, "\\bGO\\b", fixed = F)[[1]]){
    subquery<-sql.clean(subquery)
    if(sub("^\\s+$", "", subquery)!=""){
      # Not only whitespace
      dbCon <- dbhandle_create_func()
	  tryCatch({
		  print(attr(dbCon, "connection.string"))
		  if(verbose){
			cat(subquery,"\r\n")
		  }
		  results <- sqlQuery(dbCon, subquery, errors = F)
		  sql_errors <- odbcGetErrMsg(dbCon)
	  },
	  finally= close(dbCon)
	  )
      if(length(sql_errors)>0){
        warning(sql_errors)
        stop(paste(sep="\r\n",
                   "RODBC returned the following errors:",
                   paste0(collapse="\r\n", sql_errors),
                   "When executing the query (if fails with weird initial character resave it as utf-8 (Codepage 1252 in Advanced save options for MS SQL):",
                   subquery
        ))
      }
    }
  }
  if(!is.null(results)){
    return(results)
  }
}
