# this function reformats all the .csv files in the named directory
# prepending the provided prefix to the filenames for the processed
# files. It returns the number of files processed.
# Note this requires the csv_reformat function to be accessible

reformat_csv_in_dir <- function(dirname, prefix)
{
    # get all the matching files
    filenames <- list.files(pattern="*.csv", path=dirname, ignore.case=TRUE)
    print(sprintf("found %d files in directory %s", length(filenames), dirname))

    # prepend prefix to the existing filenames for the reformatted files
    newfilenames <- paste0(prefix, filenames)
    pathtofns <- paste(dirname, filenames, sep="/")
    pathtonewfns <- paste(dirname, newfilenames, sep="/")

    # reformat all the csv files as a vectorized operation
    vec_reformat <- Vectorize(csv_reformat, SIMPLIFY=FALSE)
    vec_reformat(pathtofns, pathtonewfns)
    print(sprintf("reformatted %d files", length(newfilenames)))
    # print(sprintf(paste(newfilenames, sep="", collapse=", ")))

    # return the number of files processed
    return(length(newfilenames))
}

