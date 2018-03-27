# this function reformats a .csv file, replacing its multi-line header
# with a single line, and converting the date-time field from Excel
# format to 2 columns, 1 for date in dd/mm/YYYY format, and 1 for time
# in HH:MM:SS format

csv_reformat <- function(infile, outfile)
{
	# remove redundant header info from file with skip, and strip white space
	data <- read.csv(infile, sep=",", dec=".", skip=8, strip.white=TRUE)

	# date conversion from Windows Excel format: see ?format.Date
    dates <- format(as.Date("1899-12-30") + floor(data[[2]]), "%d/%m/%Y")

    # time conversion from Windows Excel format: see ?as.POSIXct
    # and: http://stackoverflow.com/questions/12161984/how-to-elegantly-convert-datetime-from-decimal-to-d-m-y-hms
    times <- format(as.POSIXct(data[[2]]*60*60*24, origin=as.Date("1899-12-30"), tz="UTC"), "%H:%M:%S")

	# remove old date.time col (2) and add new Date and Time columns
	newtable <- data.frame(data[1], dates, times, data[3:ncol(data)])

	# add column names as new header row
	names(newtable) <- c("Scan Number", "Date", "Time", "Sensor 1 Raw Value", "Sensor 1 Water mm", "Sensor 2 Raw Value", "Sensor 2 Water mm", "Sensor 3 Raw Value", "Sensor 3 Water mm", "Sensor 4 Raw Value", "Sensor 4 Water mm", "Sensor 5 Raw Value", "Sensor 5 Water mm")

	# write the reformatted data back out to a csv file
	write.table(newtable, file=outfile, sep=",", row.names=FALSE)
}

