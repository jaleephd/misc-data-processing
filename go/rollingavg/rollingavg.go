// rollingavg.go: provide rolling averages from a CSV file
// Version 0.1 by Justin Lee <jm.lee@qut.edu.au>, 15 Jan 2016
//
// reads in a csv file containing a header row followed by rows of
//     X, Y, Z, Date Time
// for each row calculate forward looking rolling averages for cols A and B
// output a CSV containing header row followed by  rows of
//     X, Y, Z, Date Time, rolling-Avg-A, rolling-Avg-A
//
// Synopsis: rollingavg [-version] [-v] [-n nrows] [-f inputfile] [-o outputfile] 
// files default to stdin and stdout, nrows to 23


package main


import (
	"flag"
	"log"
	"os"
	"io"
	"bufio"
	"encoding/csv"
	"fmt"
	"strconv"
)

const APP_VERSION = "0.1"

// The flag package provides a default help printer via -h switch
var versionFlag bool
var verboseFlag bool
var infilename string
var outfilename string
var nrows int


func init() {
	flag.BoolVar(&versionFlag, "version", false, "Print the version number.")
	flag.BoolVar(&verboseFlag, "v", false, "verbose output for debugging")
	flag.IntVar(&nrows, "n", 23, "number of rows (interval) for moving average")
	flag.StringVar(&infilename, "f", "", "CSV containing data to process")
	flag.StringVar(&outfilename, "o", "", "output CSV containing processed")
	log.SetFlags(log.LstdFlags | log.Llongfile)
}


func main() {
	flag.Parse() // Scan the arguments list
	if versionFlag {
		fmt.Println("Version:", APP_VERSION)
	}

	if verboseFlag {
		fmt.Println("rolling average over CSV rows.")
		fmt.Println("input filename: ", infilename)
		fmt.Println("output filename: ", outfilename)
		fmt.Println("interval: ", nrows)
	}

	infl := os.Stdin
	oufl := os.Stdout
	var err error

	if infilename != "" {
		infl, err = os.Open(infilename)
		if err != nil {
			log.Fatalln("error opening source csv:", err)
		}
		defer infl.Close()
	}
	infile := csv.NewReader(bufio.NewReader(infl))

	if outfilename != "" {
		oufl, err = os.Create(outfilename)
		if err != nil {
			log.Fatalln("error creating destination csv:", err)
		}
		defer oufl.Close()
	}
	outfile := csv.NewWriter(bufio.NewWriter(oufl))

	cols := processHeader(infile, outfile)
	if verboseFlag {
		fmt.Printf("read header record containing %d columns\n", cols)
	}

	genRollingAvg(infile, outfile, nrows)

	outfile.Flush()
	if err := outfile.Error(); err != nil {
		log.Fatalln("error writing csv:", err)
	}
}


// append 2 floating average cols to the original header and write to CSV file
func processHeader(incsv *csv.Reader, outcsv *csv.Writer) (cols int) {
	record, err := incsv.Read()
	if err != nil {
		log.Fatal(err)
	}

	if verboseFlag {
		fmt.Println("read header record: ", record)
	}

	cols = len(record)
	outrec := append(record, "Average A", "Average B", "Result")

	if verboseFlag {
		fmt.Println("write header record: ", outrec)
	}

	if err = outcsv.Write(outrec); err != nil {
		log.Fatalln("error writing record to csv:", err)
	}
	return
}


// generate a forward looking rolling average from incsv rows, write to outcsv
func genRollingAvg(incsv *csv.Reader, outcsv *csv.Writer, interval int) {
	// use circular buffer to keep track of previous values
	// for running average
	cbufA := make([]float64, interval)
	cbufB := make([]float64, interval)
	rows := make([][]string, interval)
	suma := 0.0
	sumb := 0.0
	n := 0
	for {
		record, err := incsv.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalln("error reading record from csv:", err)
		}

		if verboseFlag {
			fmt.Printf("read record [%d]: %s\n", n, record)
		}

		a, err := strconv.ParseFloat(record[0], 64)
		if err != nil {
			log.Fatalln("invalid column value in csv:", err)
		}
		b, err := strconv.ParseFloat(record[1], 64)
		if err != nil {
			log.Fatalln("invalid column value in csv:", err)
		}

		// cbuf will be zero initialised so this works when n<interval
		i := n % interval
		suma -= cbufA[i]
		sumb -= cbufB[i]
		suma += a
		sumb += b
		cbufA[i] = a
		cbufB[i] = b
		rows[i] = record

		if verboseFlag {
			fmt.Printf("record [%d]: i=%d suma=%f sumb=%f\n", n, i, suma, sumb)
		}

		n++
		if n >= interval {
			if verboseFlag {
				fmt.Printf("write record [%d]: ", n-interval)
			}
			ravga := suma/float64(interval)
			ravgb := sumb/float64(interval)
			res := "0"
			if ravga < -1 && ravgb < -1500 {
				res = "1"
			}
			outputCSVrow(outcsv, rows[n%interval], strconv.FormatFloat(ravga, 'f', -1, 64), strconv.FormatFloat(ravgb, 'f', -1, 64), res)
		}
	}

	if verboseFlag {
		fmt.Printf("processed %d records\n", n)
	}

	// NOTE:
	// if need to output the remaining records, do it here
	// how to deal with their rolling averages???
}


// append the floating averages to the original record and write to CSV file
func outputCSVrow(outcsv *csv.Writer, record []string, avga string, avgb string, res string) {
	outrec := append(record, avga, avgb, res)

	if verboseFlag {
		fmt.Println("write record: ", outrec)
	}

	if err := outcsv.Write(outrec); err != nil {
		log.Fatalln("error writing record to csv:", err)
	}
}


