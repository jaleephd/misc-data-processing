#!/bin/bash

# NOTE: the following JOB constants can be modified if required
# define the name of the PBS job
JOBNAME="bcl2fastq"
# use a whole node
JOBCPUS=16
# for each CPU allocate 2G RAM
MEM=$(($JOBCPUS * 2))
JOBMEM="${MEM}G"
JOBWALLTIME="12:00:00"

# TODO: the proper destinations for processed data

# *** TESTING ONLY!!!
SEQDATADIR=$HOME/scratch/NextSeq500/raw
FASTQDATADIR=$HOME/scratch/NextSeq500/fastq
FASTQCDATADIR=$HOME/scratch/NextSeq500/fastqc

echo "**************************************"
echo "* bcl2fastq conversion job generator *"
echo "**************************************"
echo
echo "You can terminate this script at anytime by pressing Ctrl-C"
echo

# Ask the user if they want to change the defaults

echo -n "sequencer data directory (Hit <Enter> for default $SEQDATADIR) : "
read INPUT
if [ $? -ne 0 ]; then exit; fi
if [ -n "$INPUT" ]; then
  SEQDATADIR=$INPUT
fi

echo -n "fastq data directory (Hit <Enter> for default $FASTQDATADIR) : "
read INPUT
if [ $? -ne 0 ]; then exit; fi
if [ -n "$INPUT" ]; then
  FASTQDATADIR=$INPUT
fi

echo -n "fastqc data directory (Hit <Enter> for default $FASTQCDATADIR) : "
read INPUT
if [ $? -ne 0 ]; then exit; fi
if [ -n "$INPUT" ]; then
  FASTQCDATADIR=$INPUT
fi

# get the name of the sequencer run - there should be a directory
# of this name under SEQDATADIR

while :
do
  echo -n "sequencer bcl data run name: "
  read INPUT
  if [ $? -ne 0 ]; then exit; fi
  RUN="$INPUT"
  BCLDIR="${SEQDATADIR}/${RUN}"
  FASTQDIR="${FASTQDATADIR}/${RUN}"
  FASTQCDIR="${FASTQCDATADIR}/${RUN}"
  if [ ! -d "$BCLDIR" ]; then
      echo "Error: sequencer run directory does not exist: $BCLDIR"
  else
    if [ ! -f "$BCLDIR/SampleSheet.csv" ]; then
      echo "WARNING: sequencer run directory does NOT contain a SampleSheet.csv."
      echo "Continuing without SampleSheet.csv... press CTRL-C to abort!"
    fi
    break
  fi
done

while :
do
  echo -n "job submission script name: "
  read INPUT
  if [ $? -ne 0 ]; then exit; fi
  if [ -n "$INPUT" ]; then
    if [ -f "$INPUT" ]; then
      echo "file already exists!"
    else
      SUBFILE="$INPUT"
      break
    fi
  fi
done


cat << EOF | tee $SUBFILE > /dev/null
#!/bin/bash -l

#PBS -N bcl2fastq
#PBS -l select=1:ncpus=$JOBCPUS:mem=$JOBMEM
#PBS -l walltime=$JOBWALLTIME
#PBS -j oe

module load illumina
module load java
module load fastqc

# current directory (for PBS output/error file)
cd \$PBS_O_WORKDIR

# generate the fastq files from the BCL data
# use -d <#threads> to manually specify the number of demultiplexing threads
# use -p <#threads> to manually specify the number of processing threads
# where:
# - demultiplexing threads is roughly 1/3 of the available cpus.
# - processing threads is roughly 2/3 of the available cpus.
# and there should be >=1 cpu left for the master thread.
bcl2fastq -R "$BCLDIR" -o "$FASTQDIR"

# perform a quality check on the generated fastq data
mkdir "$FASTQCDIR"
if [ -d "$FASTQCDIR" ]; then
    fastqc -t $JOBCPUS -o "$FASTQCDIR" "$FASTQDIR"/*.fastq.gz
else
    echo "unable to create fastqc directory.. no fastqc tests run!"
fi

echo "done!"
echo "fastq data is in: $FASTQDIR"
echo "fastqc results are in: $FASTQDIR"

EOF

echo
echo "PBS job submission script has been created: $SUBFILE"
echo
echo "Submit conversion job with: qsub $SUBFILE"
echo "To see your job status: qstat -u `whoami`"
echo

