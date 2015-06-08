requireInSet <- function(subset, superset, description=""){
  na_elements <-prob:::setdiff(subset, superset)
  if(length(na_elements)>0){
    stop(paste0(description, " Unexpected elements: '",paste(na_elements, collapse="'', '"), "'. ",
                "The folowing are possible: '", paste(superset, collapse="', '"),"'."))
  }
}

#Note: Hmis::summarize function hides summarize function used for ddply, use summarise with ddply instead!
is.orderable <- function(values){length(intersect(class(values),c("integer","numeric","Date")))>0}

# Creates a summary table of all columns, one row per column, and in the columns a short description the values
# Columns include the number of NA's, unique's, most occurring and least occurring values
explore <- function(df, maxwidth=NA){
  t(sapply(colnames(df), function(x){
    values <- df[,x]
    non_orderable <- !is.orderable(values)
    #print(x)
    list(
      #variable = as.character(x),
      "#unique" = length(unique(values)),
      "#empty" = sum(nchar(as.character(values))<1),
      "#<NA>" = sum(is.na(values)),
      "#available" = sum(!is.na(values) & nchar(as.character(values))>0),
      min = if(non_orderable) NA else as.character(min(values, na.rm = T)),
      max = if(non_orderable) NA else as.character(max(values, na.rm = T)),
      "#maj." = sort(table(values), decreasing=T)[1],
      majority = maybe.trim(names(sort(table(values),decreasing = T))[1], maxwidth),
      "#min." = sort(table(values))[1],
      minority = maybe.trim(names(sort(table(values)))[1], maxwidth)
      )
  }))
}
# Creates a table of observed character lengths. One row per column in df, the string lengths as columns, the frequency in the cells
explore.nchar <- function(df, useNA="ifany"){
  value_lengths <- transform(melt(df, id.vars=c()), length=nchar(value)) 
  value_lengths$length[is.na(value_lengths$value)] <- NA
  table(value_lengths$variable, value_lengths$length, useNA=useNA)
}


# Given a dataframe looks for variables (columns) that correlate with target_column
# The advantage of this function is that it also works for factors and string values.
# Details: Instead of using linear models this function utilizes random forests (actually boosted models).
# The advantage of this approach is that no dummy variables are created, allowing simpler interpretation of most influential variables
# returns the matrix with a row per variable, together with its influence, ordered by decreasing influence
# Details taken care of : 
# * gbm doesn't allow NA's
# * gbm doesn't allow factors of >1024 levels
# * (almost) unique identifiers are excluded to prevent creating a mapping of id->value
# * constant columns are removed
# * gbm doesn't allow Dates (as they are not numeric, ordered nor a factor)
influential.vars <- function(target_column, dataframe, interaction.depth=2, n.trees=200, n.cores=4, shrinkage=0.01, 
                             ignore=c(), #which columns (indices/names) of dataframe should be excluded from analysis
                             plot.it=T, #Whether to plot variable influences and fitting performances
                             verbose=T, #Whether to print variable modifications and result summaries
                             ...){
  library(caret)
  library(randomForest)
  library(e1071)
  library(gbm)
  # Remove NA's
  dataframe_without_NA <- dataframe
  for(i in 1:ncol(dataframe)){
    values <- dataframe[,i]
    if(is.factor(values)) {
      values<-as.character(values)
    }
    if(is.logical(values)){
      values<-as.numeric(values)
    }
    if("POSIXt" %in% class(values)){
      values <- as.integer(values) # seconds since epoch
    }
    if(sum(is.na(values))>0){
      na_replacement <- 
        if("Date"%in%class(values)){
          as.Date("9999-12-31")
        }else if(is.orderable(values)) {
          if(length(unique(values))==1){
            0 # replace all na's,which are the only values, by 1
          } else{ 
            min(values, na.rm = T)-max(values, na.rm=T)-1 # Inf or -Inf cannot be plotted, using min-max to have outlying value to indicate the NA value
          }
          
        }else if (is.character(values)){
          "<NA>"
        } else {
          stop(paste0("We cannut use NA values in the model, but don't know where to replace them with for values of class ",paste(class(values))))
        }
      values[is.na(values)] <- na_replacement
    }
    # If there are only two unique values, make it a factor
    if(length(unique(values))==2){
      if(verbose)
        print(paste0("Because of the 2 unique values (", paste(collapse=", ", unique(values)), ") of ", colnames(dataframe)[i]," made it a factor"))
      values<-as.character(values) # values <- as.boolean(values, unique(values)[1], unique(values)[2])
    }
    if(is.character(values)) values <- as.factor(values)
    if("Date"%in%class(values)) values <- as.numeric(values)
    dataframe_without_NA[,i] <- values
  }
  
  excluded_columns <- c(
    #which(apply(dataframe,2,function(x)sum(is.na(x)))>0), #Ignore columns with NA values
    which(apply(dataframe,2,function(x)length(unique(x))/length(x)>0.8)), #ignore identifyig columns (id's or semi-ids)
    which(apply(dataframe,2,function(x)length(unique(x))==1)), #ignore constant columns (or only one value other than NA)
    which(apply(dataframe,2,function(x)length(unique(x))>1024)) #gbm only allows factors with less than 1024 levels
  )
  if(verbose & length(excluded_columns)>0)
    print(paste0("Excluded columns with only 1 unique value or > ",min(0.8*nrow(dataframe), 1024)," unique values: ",paste(collapse=", ", colnames(dataframe)[unique(excluded_columns)])))
  if(length(ignore)>0){
    if(is.numeric(ignore)){
      excluded_columns <- c(ignore,excluded_columns)
    } else{
      excluded_columns <- c(which(colnames(dataframe)%in%ignore), excluded_columns)
    }
  }
  excluded_columns <- sort(unique(excluded_columns))
  dataframe_without_NA <- dataframe_without_NA[,-excluded_columns]
  fit <- gbm(as.formula(paste0(target_column, " ~ .")) , 
             data=dataframe_without_NA, 
             interaction.depth=interaction.depth,
             n.trees=n.trees, 
             n.cores=n.cores, 
             shrinkage=shrinkage, ...)
  fit_best <- gbm.perf(fit, plot.it=plot.it)
  s<-summary(fit, n.trees=fit_best, plotit=plot.it)
  variables_with_influence<-as.character(subset(s, rel.inf>0)$var)
  if(plot.it){
    for(variable_with_influence in variables_with_influence){
      plot(fit, variable_with_influence, n.trees=fit_best)
    }
  }
  colidx <- which(colnames(dataframe_without_NA)%in%head(variables_with_influence, n=3))
  if(verbose){
    cat("Summary of the transformed values of all influential variables\r\n")
    print(summary(dataframe_without_NA[,variables_with_influence]))
    print(lapply(dataframe_without_NA[,variables_with_influence], unique))
  }
  if(plot.it)
    plot(fit, colidx, n.trees=fit_best)
  return(s)
}

## Compares table of cooccurrences of values in v1 and v2. Returns the v1 values withi which the distribution of v2 counts contains outliers
pairs.outliers <- function(v1, v2){
  has_outliers <- apply((table(v1, v2)), 1, function(x)length(boxplot(x[x>0], plot = F)$out)>0)
  names(has_outliers)[has_outliers]
}

# Given an 'almost' unique id, checks which columns sometimes have multiple values within rows sharing one id
# If already some other 'almost', or fully, unique identifiers are known, they can be added as last argument. These will be excluded from the returned character vector
within.id.columns <- function(dataset, candidate_id, known.ids=c()){
  dataset <- melt(dataset, id.vars = candidate_id)
  dataset_rebuilt <- dcast(dataset, as.formula(paste0(candidate_id,"~variable")), fun.aggregate = function(x)length(unique(x)))
  altering_cols <- apply(dataset_rebuilt[,-1],2,max)>1 # The highest nr of uniques within 1 isin is > 1
  setdiff(names(altering_cols[as.vector(altering_cols)]),c(candidate_id, known.ids)) 
}

# Given an 'almost' unique id, checks which columns always have multiple values within rows sharing one id
# If already some other 'almost', or fully, unique identifiers are known, they can be added as last argument. These will be excluded from the returned character vector
within.id.columns.intersect <- function(fullDataset, candidate_id, known.ids=c()){
  dataset <- subset(ddply(fullDataset, candidate_id, nrow), V1>1) #non-unique ids
  dataset <- merge(dataset[,-2,drop=F], fullDataset, by.x=candidate_id, by.y=candidate_id) #rows with non-unique ids
  dataset <- melt(dataset, id.vars = candidate_id)
  dataset_rebuilt <- dcast(dataset, as.formula(paste0(candidate_id,"~variable")), fun.aggregate = function(x)length(unique(x)))
  altering_cols <- apply(dataset_rebuilt[,-1],2,min)>1 # The lowest nr of uniques within 1 (duplicated) isin (thus for all isins) is > 1
  setdiff(names(altering_cols[as.vector(altering_cols)]),c(candidate_id, known.ids)) 
}

# Given an should be unique id, returns the rows and columns which have multiple values for the non-candidate-id cols
within.id.altering <- function(fullDataset, candidate_id, known.ids=c()){
  dataset <- subset(ddply(fullDataset, candidate_id, nrow), V1>1) #non-unique ids
  if(nrow(dataset)==0)
    return(dataset)
  dataset <- merge(dataset[,-2,drop=F], fullDataset, by.x=candidate_id, by.y=candidate_id) #rows with non-unique ids
  dataset_melt <- melt(dataset, id.vars = candidate_id)
  dataset_colcounts <- dcast(dataset_melt, as.formula(paste0(candidate_id,"~variable")), fun.aggregate = function(x)length(unique(x)))
  altering_cols <- apply(dataset_colcounts[,-1,drop=F],2,max)>1 # The lowest nr of uniques within 1 (duplicated) isin (thus for all isins) is > 1
  altering_cols <- setdiff(names(altering_cols[as.vector(altering_cols)]),c(candidate_id, known.ids)) 
  altering_rows <- apply(dataset_colcounts[,altering_cols,drop=F],1,max)>1 # The lowest nr of uniques within 1 (duplicated) isin (thus for all isins) is > 1
  if(sum(altering_rows)==0)
    return(fullDataset[F,c(candidate_id,altering_cols),drop=F])
  merge(dataset_colcounts[altering_rows,candidate_id,drop=F], dataset, by=candidate_id)
}

# Adds a n column to a whitelist indicating the number of observed records of that combination
whitelist.count <- function(whitelist, observations, by=intersect(names(whitelist), names(observations)), by.x=by, by.y=by){
  if(length(by.x)==0 | length(by.y)==0){
    data.frame(whitelist, n=0)
    stop("The whitelist has no columns to use to merge with the observations.") #Or should we do warning() ?
  }
  else{
    duplicate_keys <- ddply(whitelist, by.x, nrow)
    duplicate_keys <- duplicate_keys[duplicate_keys[,"V1"]>1,]
    if(nrow(duplicate_keys)>0){
      stop("The whitelist contains duplicates : ", paste(duplicate_keys, sep=",", collapse=" & "))
    }
    ddply(merge(whitelist, all.x=T, all.y=F, cbind(observations, a_counting_column=1), by.x=by.x, by.y=by.y), 
          names(whitelist), 
          function(xsub)data.frame(n=sum(!is.na(xsub$a_counting_column))))
  }
    
}
# Returns the observations in the observations which do not map the whitelist
non.whitelisted <- function(whitelist, observations, by=intersect(names(whitelist), names(observations)), by.x=by, by.y=by){
  blacklist <- as.data.frame(prob:::setdiff(unique(observations[,by.y,drop=F]), whitelist[,by.x,drop=F]))
  colnames(blacklist)<-by.y
  merge(blacklist, observations, by=by.y)
}
non.whitelisted.count <- function(whitelist, observations, by=intersect(names(whitelist), names(observations)), by.x=by, by.y=by){
  counts<-ddply(non.whitelisted(whitelist, cbind(observations, a_counting_column=1), by=by, by.x=by.x, by.y=by.y),
        by.y, 
        function(xsub)data.frame(n=sum(!is.na(xsub$a_counting_column))))
  colnames(counts)[1:length(by.y)]<-by.x
  counts
}
# Returns the combination of values that were not in the whitelist, but were found in the dataset.
diff.whitelist <- function(whitelist, dataset){
  specials <- non.whitelisted(whitelist, dataset)
  ddply(specials, colnames(whitelist), summarise, n=length(id))
}

whitelist.rmd <- function(x, dataset, ...){
  cat(paste0("\r\n## Whitelist ", paste0(colnames(x), collapse=" - "), "  \r\n"))
  cat(paste0("The following combinations of ", paste(colnames(x), collapse=", "), " are expected (allowed), together with the actual occurring number.  \r\n"))
  cat("```\r\n")
  whitelist_counts<-whitelist.count(x, dataset, ...)
  print(rmdPrep(trimmed(whitelist_counts)), row.names=F)
  cat("```\r\n")
  cat(paste0("The following combinations of ", paste(colnames(x), collapse=", "), " are not expected (allowed), together with the occurring number.  \r\n"))
  cat("```\r\n")
  blacklist_counts<-non.whitelisted.count(x, dataset, ...)
  print(rmdPrep(trimmed(blacklist_counts)), row.names=F)
  cat("```\r\n")
  cat(paste0("As a check, the sum of 'n' in the previous two lists is *", 
             sum(whitelist_counts$n)+sum(blacklist_counts$n),
             "*, and there are *",
             nrow(dataset),
             "* rows in the dataset. "))
  if(nrow(dataset)!=sum(whitelist_counts$n)+sum(blacklist_counts$n)){
    cat(paste0("These are **NOT EQUAL**.  \r\n"))
    stop("The sum of the whitelisted rows and the blacklisted rows are not equal to the total number of rows. Are the correct by arguments given?")
  }else{
    cat(paste0("These are **equal**.  \r\n"))
  }
}

# Returns subset of the whitelist, that do not match the rows in the dataset, but do have 1 matching column
related.whitelist <- function(whitelist, dataset){
  specials_combinations <- diff.whitelist(whitelist, dataset)
  list_filter <- rep(F, times = nrow(whitelist))
  for(s in colnames(whitelist)){
    list_filter <- list_filter | if (is.factor(whitelist[,s])){
      as.character(whitelist[,s]) %in% as.character(unique(specials_combinations[,s]))
    }else{
      whitelist[,s] %in% unique(specials_combinations[,s])
    }
  }
  # TODO, rbind with specials_combinations, and add Listed column indicating whether it was listed/not
  #   This is only useful if the FALSE combinations are near the related ones (which is not just the ordered dataset)
  whitelist.count(whitelist[list_filter,], dataset)
}

# Returns the unique rows of the dataframe based on the provided list of columns. 
# Of the other columns the first available (non empty) value is taken 
unique.by <- function(x, by=colnames(x)){
  dcast( melt(x, id.vars = by), 
         as.formula(paste0(
          paste0(by, collapse=" + "),
          " ~ variable")),
         fun.aggregate =
           function(x){
             if(sum(!is.na(x))==0){ #there are no available values
               x[1]
             }else{
               if(is.character(x) & sum(nchar(x)>0)>0) # there are non empty string
                 x[nchar(x)>0][1] #the first non empty string
               else
                 x[!is.na(x)][1] # the first available value
             }
           }
  )
}

dupsBetweenGroups <- function (df, idcol) {
  # df: the data frame
  # idcol: the column which identifies the group each row belongs to
  
  # Get the data columns to use for finding matches
  datacols <- setdiff(names(df), idcol)
  
  # Sort by idcol, then datacols. Save order so we can undo the sorting later.
  sortorder <- do.call(order, df)
  df <- df[sortorder,]
  
  # Find duplicates within each id group (first copy not marked)
  dupWithin <- duplicated(df)
  
  # With duplicates within each group filtered out, find duplicates between groups. 
  # Need to scan up and down with duplicated() because first copy is not marked.
  dupBetween = rep(NA, nrow(df))
  dupBetween[!dupWithin] <- duplicated(df[!dupWithin,datacols])
  dupBetween[!dupWithin] <- duplicated(df[!dupWithin,datacols], fromLast=TRUE) | dupBetween[!dupWithin]
  
  
  # =================== Replace NA's with previous non-NA value =====================
  # This is why we sorted earlier - it was necessary to do this part efficiently
  
  # Get indexes of non-NA's
  goodIdx <- !is.na(dupBetween)
  
  # These are the non-NA values from x only
  # Add a leading NA for later use when we index into this vector
  goodVals <- c(NA, dupBetween[goodIdx])
  
  # Fill the indices of the output vector with the indices pulled from
  # these offsets of goodVals. Add 1 to avoid indexing to zero.
  fillIdx <- cumsum(goodIdx)+1
  
  # The original vector, now with gaps filled
  dupBetween <- goodVals[fillIdx]
  
  # Undo the original sort
  dupBetween[sortorder] <- dupBetween
  
  # Return the vector of which entries are duplicated across groups
  return(dupBetween)
}


# Like all.equal, but prints some examples of found differences
all.equal.describe <- function(target, current, idcolids=c(1)){
  rownames(target)<-NULL
  rownames(current)<-NULL
  diffs <- all.equal(target, current)
  if(length(diffs)>1 || diffs!=T){
    col_pattern <- "Component .(.*).: \\d+.*"
    diff_cols <- diffs[grepl(col_pattern, diffs)]
    diff_cols <- gsub(pattern = col_pattern, "\\1", diff_cols)
    diff_cols <- diff_cols[nchar(diff_cols)>0]
    for(diff_col in diff_cols){
      
      target_col <- which(colnames(target)==diff_col)
      if(length(target_col)==0){
        print(paste0("Column '",diff_col,' not found in target.'))
      }
      if(length(target_col)>1){
        print(paste0("Column '",diff_col,' was found multiple times.'))
      }      
      
      if(length(target_col)==1){
        diff_rows <- if(is.factor(target[,target_col]) || is.factor(current[,target_col])){
		   as.character(target[,target_col]) != as.character(current[,target_col])
		} else{
	 		target[,target_col]!=current[,target_col]
		}
        if(is.character(target[,target_col])|is.factor(target[,target_col])|
             is.character(current[,target_col])|is.factor(current[,target_col])){
          diff_rows <- as.character(target[,target_col])!=as.character(current[,target_col])
        }
        na_rows <- is.na(diff_rows)
        if(sum(na_rows)>0)
          diff_rows[na_rows] <- is.na(target[na_rows,target_col]) != is.na(current[na_rows,target_col])
        if(sum(diff_rows)>0){
          diff_col_msg <- paste0("Differences in '", diff_col,"'")
          print(diff_col_msg)
          data<-(cbind(
            target[diff_rows, unique(c(idcolids, target_col))],
            current[diff_rows, unique(c(idcolids, target_col))],
            difference=if(is.character(target[,target_col])|is.factor(target[,target_col])){
              NA_character_
            }else{
              target[diff_rows,target_col]-current[diff_rows,target_col]
            }
          ))
          if(nrow(data)>0)
            print(data)
        }else{
          diff_col_msg <- paste0("A difference in reprensentation of '", diff_col,"' has been found, but they values are equivalent so they can be disregarded.")
          print(diff_col_msg)
        }
      }
    }
  }
}
