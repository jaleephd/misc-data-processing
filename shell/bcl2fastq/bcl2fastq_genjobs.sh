#!/bin/bash

USAGE_STR="Usage: $(basename $0) [ -d data_parent_directory ] [ -o output_parent_directory ] [ run_name1 [ .. run_nameN ] ]"

# for testing only!
DATADIR=$HOME/scratch/NextSeq500
OUTDIR=$HOME/scratch/NextSeq500Converted

declare -a CONVERSIONRUNS

if [ $# -gt 1 ] && [ "$1" = "-d" ]; then
    shift
    DATADIR=$1
    # the output location should be the same by default
    OUTDIR=$1
    shift
fi

if [ $# -gt 1 ] && [ "$1" = "-o" ]; then
    shift
    OUTDIR=$1
    shift
fi

if [ $# -gt 0 ] && [ "${1:0:1}" = "-" ]; then
    echo >&2 "$USAGE_STR"
    exit 1
fi

if [ $# -gt 0 ]; then
    CONVERSIONRUNS=( "$@" )
    #echo "$# manual conversions: ${CONVERSIONRUNS[@]}"
fi

SEQDATADIR=$DATADIR
FASTQDATADIR=$OUTDIR/fastq
FASTQCDATADIR=$OUTDIR/fastqc

echo
echo "**************************************"
echo "* bcl2fastq conversion job generator *"
echo "**************************************"
echo
echo "You can terminate this script by pressing Ctrl-C"
echo

# Ask the user if they want to change the defaults

echo -n "sequencer data parent directory (Hit <Enter> for default $SEQDATADIR) : "
read INPUT
if [ $? -ne 0 ]; then exit; fi
if [ -n "$INPUT" ]; then
  SEQDATADIR=$INPUT
fi

echo -n "fastq data parent directory (Hit <Enter> for default $FASTQDATADIR) : "
read INPUT
if [ $? -ne 0 ]; then exit; fi
if [ -n "$INPUT" ]; then
  FASTQDATADIR=$INPUT
fi

echo -n "fastqc data parent directory (Hit <Enter> for default $FASTQCDATADIR) : "
read INPUT
if [ $? -ne 0 ]; then exit; fi
if [ -n "$INPUT" ]; then
  FASTQCDATADIR=$INPUT
fi


# create an array of sequencer runs that need to be converted
if [ ${#CONVERSIONRUNS[@]} -eq 0 ]; then 
    echo "searching for automated conversions"
    for d in ${SEQDATADIR}/*; do
        #echo checking in $d
        run_name=${d##*/}
        if [ -d $d ] && [ -e $d/SampleSheet.csv ] ; then
            # if the run hasn't been processed yet, add to CONVERSIONRUNS
            if [ ! -d $FASTQDATADIR/$run_name ]; then
                echo "$run_name hasn't been converted yet"
                while : ; do
                    echo -n "Convert $run_name ? [Y/n] "
                    read INPUT
                    if [ $? -ne 0 ]; then exit; fi
                    if [ -z "$INPUT" ] || [ "$INPUT" = "y" ] || [ "$INPUT" = "Y" ]; then
                        CONVERSIONRUNS=("${CONVERSIONRUNS[@]}" "$run_name")
                        break
                    elif [ "$INPUT" = "n" ] || [ "$INPUT" = "N" ]; then
                        break
                    else
                        echo "Please enter 'y' or 'n'"
                    fi
                done
            else
                echo "$run_name is already converted"
            fi
        else
            echo "$run_name has no SampleSheet.csv"
        fi
    done
fi

if [ ${#CONVERSIONRUNS[@]} -eq 0 ]; then 
    echo "No runs to convert! Exiting..."
    exit 0
fi

echo "${#CONVERSIONRUNS[@]} conversions to be processed: ${CONVERSIONRUNS[@]}"
echo -n "Press <Enter> to continue or Ctrl-C to abort. "
read INPUT


# NOTE: the following JOB constants can be modified if required
# define the name of the PBS job
JOBNAME="bcl2fastq"
# use a whole node
JOBCPUS=16
# for each CPU allocate 2G RAM
MEM=$(($JOBCPUS * 2))
JOBMEM="${MEM}G"
JOBWALLTIME="12:00:00"


# create and submit jobs for each chosen data run directory
for RUN in ${CONVERSIONRUNS[@]}; do
  BCLDIR="${SEQDATADIR}/${RUN}"
  FASTQDIR="${FASTQDATADIR}/${RUN}"
  FASTQCDIR="${FASTQCDATADIR}/${RUN}"
  if [ ! -d "$BCLDIR" ]; then
      echo "Error: sequencer run directory does not exist: $BCLDIR"
  else
    if [ ! -f "$BCLDIR/SampleSheet.csv" ]; then
      echo "WARNING: sequencer run directory does NOT contain a SampleSheet.csv!"
      echo -n "Press <Enter> to continue without SampleSheet.csv or CTRL-C to abort. "
      read INPUT
    fi
  fi

  if [ -e $RUN.sub ]; then
      echo "removing old submission script..."
      rm -f $RUN.sub
  fi
  cat << EOF | tee $RUN.sub >> /dev/null
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
if [ -d "$FASTQCDIR" ]; then
    rm -rf "$FASTQCDIR"
fi
mkdir "$FASTQCDIR"
if [ -d "$FASTQCDIR" ]; then
    fastqc -t $JOBCPUS -o "$FASTQCDIR" "$FASTQDIR"/*.fastq.gz
else
    echo "unable to create fastqc directory.. no fastqc tests run!"
fi

echo "done!"
echo "fastq data is in: $FASTQDIR"
echo "fastqc results are in: $FASTQCDIR"

EOF

    echo
    echo "PBS job submission script has been created: $RUN.sub"
    echo
    echo "Submitting conversion job..."
    #echo "qsub $RUN.sub"
    qsub $RUN.sub
    echo
done

echo
echo "All conversion jobs have been submitted."
echo "To see your jobs' status: qstat -u `whoami`"
echo

