library("seewave")
library("tuneR")

args <- commandArgs(trailingOnly = TRUE)
# directory is the first (only) argument
dir = args[1]
print(sprintf("processing files in directory %s", dir))

File_list = list.files(dir, pattern="\\.wav$", full.names = TRUE)
show(File_list)

# WARNING: this assumes that all files are 180 mins!
Tot_mins = 180
Chunck_size = 30
N = Tot_mins/ Chunck_size
print(sprintf("number of chunks of %d mins to process in a file of %d mins is set to: %d", Chunck_size, Tot_mins, N))

# process each file in the specified directory
for (j in File_list) {
    dout <- vector()
    show(j)
    dout <- c(dout, j)
    for (i in 1:N) {
        Deagon_Dec<-readWave(j, from = (i-1)* Chunck_size + 1, to = i*Chunck_size, units = "minutes")
        val <- H(Deagon_Dec)
        show(val)
        dout <- c(dout, val)
    }

    write(dout, paste(c(dir,"entropy.csv"), collapse="/"), length(dout), append=TRUE, sep=",")
}


