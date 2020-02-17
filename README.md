# Misc Data Processing

This repository contains miscellaneous code, that has been used for processing or converting data.

Some of it may be useful to others, some of it may be too specific to be of direct use,
but may provide an example of how to do something.

## Go

* `rollingavg.go` rolling average calculator
* `test.csv` test CSV for use with `rollingavg.go`

## Perl

* `ec2resources.map` example resource map for use with `pbs_usage.pl`
* `pbs_usage.pl` extract information from PBS and module logs and create usage report

## Python

* `fasta2genes.py` extract genes from fasta file(s) and write to gene files
* `csv2sqlite.py` converts a CSV file to an Sqlite3 database

## R

* `audio-entropy`
  * `audio_entropy.r` audio entropy calculator from wav files
* `csv-reformat`
  * `reformat_csv_in_dir.r` function to reformat all csv files in directory and rename output files
  * `csv_reformat.r` function to reformat a csv file, see source for description
* `civic_cancer_genes.Rmd` extract gene-mutations and their cancer type from a JSON-based Civic database, and save to a CSV file.

## Shell

* `bcl2fastq_script.sh` older version of script to process bcl files from sequencer
* `bcl2fastq_genjobs.sh` generate PBS jobs for converting sequencer bcl files to fastq format
