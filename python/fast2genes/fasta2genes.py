"""
fasta2genes: extract genes from fasta file(s) and write to gene files

Usage: python2 fasta2genes.py [-h] [-d] -i indexfile fastafile [fastafile2 ..]
Description:
             takes a gene indexing info file in format, eg:
                charset EOG808PJJ = 1 - 120 ;
             and 1 or more fasta files in format:
                >species_name
                sequence-data
             extracts valid genes from species in fasta file
             writes out/appends to files named for each gene
             (as genename.fasta) containing entries for each species
             having that gene, in format:
                >species_name
                subsequence-data
             if optional debug flag (-d) specified, no files are written

Author: Justin Lee <jm.lee@qut.edu.au>, QUT HPC group, Oct 2015

"""

import readline
import sys
import getopt
from collections import namedtuple


# structure for storing gene information provided in index file
GeneInfo = namedtuple('GeneInfo', 'name start end')
# structure for providing species info contained in fasta file
SpeciesInfo = namedtuple('SpeciesInfo', 'name, sequence')

# global flag for printing debug info
debug = False


##############################################################


# given a gene name and an array of SpeciesInfo records
# create/append to gene file (name.fasta) all species with this gene
def writeGenesSpecies(genename, species_list):
    with open(genename+".fasta", 'a') as genefile:
        # output all species in array as:
        # >species_name
        # gene_sequence
        for species in species_list:
            genefile.write(">%s\n" % species.name)
            genefile.write("%s\n" % species.sequence)


# given a gene name and an array of SpeciesInfo records
# print gene name and all species with this gene
def printGenesSpecies(genename, species_list):
    print "\n-------------------"
    print "gene {}".format(genename)
    for species in species_list:
        print ">{}".format(species.name)
        print species.sequence


# look for 'X' and if gene doesn't start with them, or it contains some valid part
# then it's considered to be valid
def isValidGene(gene):
    # if gene starts with an invalid letter, then check further
    if gene.sequence.startswith('X'):
        if gene.sequence.count('X') == len(gene.sequence):
            if debug: print "gene %s is INVALID - no valid letters in seq: %s" % (gene.name, gene.sequence)
            return False
        if debug: print "gene %s starts with invalid letter in seq: %s" % (gene.name, gene.sequence)
    if debug: print "gene %s is valid" % gene.name
    return True


# extract a gene from a species' sequence
# based on supplied gidx GeneInfo record
# convert all '?'s and any leading or trailing '-'s to 'X's
def extractGene(species, gidx):
    # gene indexing is 1-N vs string 0:N-1
    # gene indexing is a-b vs slice is from a:b-1 (upto not including b)
    # this gives string slice start-1:end-1+1
    seq = species.sequence[gidx.start-1:gidx.end]
    if debug: print "gene seq = ", seq
    genelen = gidx.end - gidx.start + 1
    if debug: print "gene len = ", genelen
    # note: we only use one invalid character: 'X'
    seq = seq.replace('?', 'X')
    #if debug: print "gene seq updated = ", seq
    # replace all end '-'s with 'X's 
    seq = seq.lstrip('-')
    #if debug: print "lstriped seq = ", seq
    endseq = 'X' * (genelen - len(seq))
    seq = endseq + seq
    seq = seq.rstrip('-')
    #if debug: print "rstriped seq = ", seq
    endseq = 'X' * (genelen - len(seq))
    seq = seq + endseq
    if debug: print "cleaned seq = ", seq
    return SpeciesInfo(species.name, seq)


# extracts all genes from a species
# and if the species' gene sequence is valid,
# adds the species and it's sub-sequence for the gene
# to the gene's array of SpeciesInfo
def addSpeciesToGeneArray(species, gene_indexes, genelist):
    if debug: print 'processing species:', species.name
    # for each gene,
    for g in gene_indexes:
        # extract gene from species (as SpeciesInfo with gene seq)
        gene = extractGene(species, g)
        # check if species contains valid gene
        if isValidGene(gene):
            # if it does, add to gene's array
            if debug: print "adding gene:", gene.name
            genelist[g.name].append(gene)
    if debug: print 'done processing species:', species.name, '\n'


# takes a fasta file and reads all the species from it into an array
# of SpeciesInfo records (name, sequence), which is returned to caller
# WARNING: no sanity checks of fasta file are done!!!
def readSpecies(fasta_filename):
    species_list = [] # array of (name, sequence)
    with open(fasta_filename, 'r') as fasta_file:
        # lines are in format:
        #   '>'Species_name
        #   sequence
        for line in fasta_file: 
            line = line.rstrip('\n')
            if line.startswith('>'):
                sname = line[1:]
            else:
                # this line contains the sequence for above name
                species_list.append(SpeciesInfo(sname, line))
                if debug: print 'added species {}: {}'.format(len(species_list), sname)
    return species_list


# takes a single fasta file, and a list of GeneInfo gene names and indexes
# reads all the species from the fasta file, and adds species to genes
# if gene sequence is valid. Lastly writes a set of gene files, containing
# member species and their gene sequence
def processFastaFile(fasta_filename, gene_indexes):

    # create a dictionary (key: gene name) of arrays (of species info)
    gene_species = {}
    for g in gene_indexes:
        gene_species[g.name] = []

    # parse fasta file, to extract array of SpeciesInfo (name, sequence)
    if debug: print 'reading species in file:', fasta_filename
    species_list = readSpecies(fasta_filename)

    # for each species, extract gene sequences and add to gene array
    if debug: print '\nextracting genes from species...\n'
    for species in species_list:
        addSpeciesToGeneArray(species, gene_indexes, gene_species)
    if debug: print 'done extracting genes from species.\n'

    # write the genes (with their matching species) out to files
    # (or print if debugging)
    if debug: print 'writing genes for processed species...'
    for genename in gene_species:
        if debug:
            printGenesSpecies(genename, gene_species[genename])
        else:
            writeGenesSpecies(genename, gene_species[genename])
    if debug: print '\ndone processing fasta file:', fasta_filename, '\n'


# scans the file to get gene name and gene indexes
# for parsing fasta files
# returns an array of GeneInfo records (name, start, end)
def readGeneIndexes(index_filename):
    if debug: print '\nprocessing gene details in index file:', index_filename
    gene_list = []
    with open(index_filename, 'r') as idx_file:
        # lines are in format: 'charset' gene_name '=' start '-' end ';'
        for line in idx_file:
            line = line.rstrip('\n')
            fields=line.split()
            gname=fields[1]
            gstart = int(fields[3])
            gend = int(fields[5])
            gene_list.append(GeneInfo(gname, gstart, gend))
            if debug: print 'added gene {}: {}'.format(len(gene_list), gname)
    if debug: print 'done processing index file:', index_filename, '\n'
    return gene_list


##############################################################


def main(argv):

    global debug
    debug = False

    paramstr = "[-h] [-d] -i indexfile fastafile [fastafile2 ..]"
    usagestr = "Usage: {} {}".format(sys.argv[0], paramstr)

    index_file = ""

    try:
        options, remainder = getopt.gnu_getopt(argv[1:],"hdi:")
    except getopt.GetoptError:
        print usagestr
        sys.exit(2)

    for opt, arg in options:
        if opt == '-h':
            print usagestr
            sys.exit()
        elif opt == '-d':
            debug = True
        elif opt == '-i':
            index_file = arg
        else:
            assert False, "unhandled option"

    #print "number of remaining args = {}; args = {}".format(len(remainder), str(remainder))
    if (len(remainder) < 1 or len(index_file) < 1):
        print usagestr
        sys.exit(2)

    # parse the index file to get gene name, start index and length
    # stored in an array of GeneInfo
    gene_indexes = readGeneIndexes(index_file)

    # process each of the provided fasta files
    for fastafn in remainder:
        processFastaFile(fastafn, gene_indexes)


if __name__ == "__main__":
     sys.exit(main(sys.argv))

