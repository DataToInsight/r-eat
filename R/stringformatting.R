
make.names.sql <- function(x){
	non_nas <- !is.na(x)
  x[non_nas] <- make.names(x[non_nas]) #to make names R compatible
  x[non_nas]<-gsub("\\.","",x[non_nas]) #to make the names sql compatible
  x
}

# Capitalizes the first character
capit <- function(x) paste0(toupper(substring(x, 1, 1)), substring(x, 2, nchar(x)))

# Capitalizes the first character of each word, where words are separated by the provided sep. The separataros are removed
camel <- function(x, sep="_"){ #function for camel case
  sapply(strsplit(x, sep, fixed=T), function(x) paste(capit(x), collapse=""))
}

# Basic variable/column name normalization. All caps get lowercased 2nd and further characters. Then everything is camel-cased
camel.label <- function(x, sep=c("_"," ",".","(","["), rem=c(")","]")){
  x <- as.character(x)
  all_cap <- toupper(x)==x
  x[all_cap] <- capit(tolower(x[all_cap]))
  x <- capit(x)
  for(s in sep){
    multiword <- grepl(s, x, fixed=T)
    x[multiword] <- camel(tolower(x[multiword]), s)
  }
  for(s in rem){
    x <- gsub(s, "", x, fixed=T)
  }
  x
}

# returns string w/o leading whitespace
trim.leading <- function (x)  sub("^\\s+", "", x)

# returns string w/o trailing whitespace
trim.trailing <- function (x) sub("\\s+$", "", x)

# returns string w/o leading or trailing whitespace
trim <- function (x) gsub("^\\s+|\\s+$", "", x)

sql.clean<-function(query){
  query <- gsub("/\\*.*?\\*/", "", query) # Remove all /* comment */ blocks. The ? operator makes the *-repeator non-greedy
  query <- gsub("--.*?\\n", "\r\n", query) #Remove all -- comments till next newline
  query <- gsub("\\s+(\\r)?\\n", "\r\n", query) #Remove all line trailing whitespace
  query <- trim(query) #Remove all query leading/ trailing whitespace
  query <- gsub("(\\r\\n)+", "\r\n", query) #Remove empty lines
  return(trim(query))
}

rmdPrep.vector<-function(x){
  if(is.character(x)|is.factor(x)){
    gsub("^", "\\^", fixed=T,
         gsub("*", "\\*", fixed=T,
              gsub("$", "\\$", fixed=T, # TODO: escape - and digits at beginning of line (only prefixed with whitespace), as they become (numbered) lists
                   x
              )
         )
    )
  } else {x}
}
rmdPrep<-function(x){
  if(!is.null(dim(x))){
    as.data.frame(sapply(x, rmdPrep.vector))
  }else{
    rmdPrep.vector(x)
  }
  
}
rmdPrepCollapse <- function(x){
  paste(rmdPrep(x), sep="  \r\n", collapse="  \r\n")
}

maybe.trim <- function(value, maxwidth){
  if(is.na(maxwidth)||is.null(value)||is.na(value)||is.numeric(value)){value}else{strtrim(value, maxwidth)}
}

trimmed.summary <- function(df, ...){
  summary(trimmed(df), ...)
}

#Returns the dataframe in which the (character&factor) contents are trimmed such that a call to print won't use any wrapping
#Gets the available line width from the R options, but is overridable by argument
#The required column widths can also be provided, either 1 constant to be used for all columns, or a vector holding the width per column
trimmed <- function(df, linewidth=NA, colwidth=NA){
  be_trimmed <- sapply(df, function(x){is.factor(x)|is.character(x)})
  if(is.na(colwidth)){
    availablewidth <- if(is.na(linewidth)){
      ((options("width")[[1]])-1) - ncol(df) #1 space before, after and between each column
    } else{
      linewidth
    } 
    # observed column widths
    num <- apply(df, 2, function(x){max(nchar(as.character(x)))})
    min_num <- nchar(colnames(df))
    max_num <- max(c(num,min_num))
    # Some widths are determined by the colname, which will be further ignored
    by_colname_width <- num<min_num
    availablewidth <- availablewidth-sum(min_num[by_colname_width]) # the column widths which are claimed by colnames
    # the average width for columns which length is determined by the contents
    avgwidth <- availablewidth/(ncol(df)-sum(by_colname_width))
    if(avgwidth<0){
      warning("Colnames too long to fit on one line. Trimming to colname width")
      maxwidth <- min_num
    } else{
      if(max_num<avgwidth){
        maxwidth<-NA
      }else{
        limits <- rep(avgwidth, ncol(df))
        # set the colname determined widths
        limits[by_colname_width] <- min_num[by_colname_width] 
        num[by_colname_width] <- min_num[by_colname_width]
        # while columns are shorter than the max width, distribute spare space on other columns
        done <- by_colname_width
        while(sum(!done & (num<limits))>0){
          not_trimmed <- (num<limits)
          extrawidth <- sum( (limits-num)[not_trimmed] ) # (availablewidth-sum(limits))
          limits[not_trimmed] <- num[not_trimmed]
          to_widen <- !not_trimmed & !done
          limits[to_widen] <- limits[to_widen] + extrawidth/sum(to_widen)
          done <- done | not_trimmed
        }
        maxwidth <- limits
      }
    }
  }else{
    maxwidth<-rep(maxwidth, length.out=ncol(df))
  }
  for(i in (1:ncol(df))[be_trimmed]){
    x<-df[,i]
    x<-if(is.factor(x)){
      as.factor(maybe.trim(as.character(x), maxwidth[i]))
    } else if(is.character(x)){
      maybe.trim(as.character(x), maxwidth[i])
    } else {
      x
    }
    df[,i]<-x
  }
  df
}
