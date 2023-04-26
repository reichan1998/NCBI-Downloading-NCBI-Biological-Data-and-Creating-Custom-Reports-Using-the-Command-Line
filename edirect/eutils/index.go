// ===========================================================================
//
//                            PUBLIC DOMAIN NOTICE
//            National Center for Biotechnology Information (NCBI)
//
//  This software/database is a "United States Government Work" under the
//  terms of the United States Copyright Act. It was written as part of
//  the author's official duties as a United States Government employee and
//  thus cannot be copyrighted. This software/database is freely available
//  to the public for use. The National Library of Medicine and the U.S.
//  Government do not place any restriction on its use or reproduction.
//  We would, however, appreciate having the NCBI and the author cited in
//  any work or product based on this material.
//
//  Although all reasonable efforts have been taken to ensure the accuracy
//  and reliability of the software and data, the NLM and the U.S.
//  Government do not and cannot warrant the performance or results that
//  may be obtained by using this software or data. The NLM and the U.S.
//  Government disclaim all warranties, express or implied, including
//  warranties of performance, merchantability or fitness for any particular
//  purpose.
//
// ===========================================================================
//
// File Name:  index.go
//
// Author:  Jonathan Kans
//
// ==========================================================================

package eutils

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"html"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
)

// PUBMED INDEXED AND INVERTED FILE FORMATS

// Local archive indexing reads PubmedArticle XML records and produces IdxDocument records.
// Title and Title/Abstract fields include term positions as XML attributes:

/*

  ...
  <IdxDocument>
    <IdxUid>2539356</IdxUid>
    <IdxSearchFields>
      <YEAR>1989</YEAR>
      <JOUR>J Bacteriol</JOUR>
      <JOUR>Journal of bacteriology</JOUR>
      <VOL>171</VOL>
      <ISS>4</ISS>
      <PAGE>1904</PAGE>
      ...
      <AUTH>Kans JA</AUTH>
      <AUTH>Casadaban MJ</AUTH>
      <TITL pos="7">immunity</TITL>
      <TITL pos="1">nucleotide</TITL>
      <TITL pos="3">required</TITL>
      <TITL pos="2">sequences</TITL>
      <TITL pos="5">tn3</TITL>
      <TITL pos="6">transposition</TITL>
      <TIAB pos="126">acting</TIAB>
      <TIAB pos="188">additional</TIAB>
      <TIAB pos="146">base</TIAB>
      <TIAB pos="125">cis</TIAB>
      <TIAB pos="172,178,187">conferred</TIAB>
      ...
      <PAIR>nucleotide sequences</PAIR>
      <PAIR>sequences required</PAIR>
      <PAIR>tn3 transposition</PAIR>
      <PAIR>transposition immunity</PAIR>
      <PROP>Published In Print</PROP>
      <PROP>Journal Article</PROP>
      <PROP>Research Support, U.S. Gov&#39;t, P.H.S.</PROP>
      <PROP>Has Abstract</PROP>
      ...
      <MESH>Plasmids</MESH>
      <MESH>Recombination, Genetic</MESH>
    </IdxSearchFields>
  </IdxDocument>
  ...

*/

// Inversion reads a set of indexed documents and generated InvDocument records:

/*

  ...
  <InvDocument>
    <InvKey>tn3</InvKey>
    <InvIDs>
      <TIAB pos="5,102,117,129,157,194">2539356</TIAB>
      <TITL pos="5">2539356</TITL>
    </InvIDs>
  </InvDocument>
  <InvDocument>
    <InvKey>tn3 transposition</InvKey>
    <InvIDs>
      <PAIR>2539356</PAIR>
    </InvIDs>
  </InvDocument>
  <InvDocument>
    <InvKey>transposition</InvKey>
    <InvIDs>
      <TIAB pos="6,122">2539356</TIAB>
      <TITL pos="6">2539356</TITL>
    </InvIDs>
  </InvDocument>
  <InvDocument>
    <InvKey>transposition immunity</InvKey>
    <InvIDs>
      <PAIR>2539356</PAIR>
    </InvIDs>
  </InvDocument>
  ...

*/

// Separate inversion runs are merged and used to produce term lists and postings file.
// These can then be searched by passing commands to EDirect's "phrase-search" script.

// ENTREZ2INDEX COMMAND GENERATOR

// MakeE2Commands generates extraction commands to create input for Entrez2Index
func MakeE2Commands(tform, db string, isPipe bool) []string {

	currentYear := strconv.Itoa(time.Now().Year())

	var acc []string

	if !isPipe {
		if !deStop {
			acc = append(acc, "-stops")
		}
		if doStem {
			acc = append(acc, "-stems")
		}
	}

	if db == "" || db == "pubmed" {

		acc = append(acc, "-set", "IdxDocumentSet", "-rec", "IdxDocument")
		acc = append(acc, "-pattern", "PubmedArticle", "-UID", "MedlineCitation/PMID")
		acc = append(acc, "-wrp", "IdxUid", "-element", "&UID", "-clr", "-rst", "-tab", "")

		acc = append(acc, "-group", "PubmedArticle", "-pkg", "IdxSearchFields")

		// identifier field - UID

		acc = append(acc, "-block", "PubmedArticle", "-wrp", "UID", "-pad", "&UID")

		// date fields - YEAR, PDAT, and RDAT

		acc = append(acc, "-block", "PubmedArticle", "-wrp", "YEAR", "-year", "PubDate/*")
		acc = append(acc, "-block", "PubmedArticle", "-unit", "PubDate", "-wrp", "PDAT", "-reg", "/", "-exp", " ", "-date", "*")
		acc = append(acc, "-block", "PubmedArticle", "-unit", "DateRevised", "-wrp", "RDAT", "-reg", "/", "-exp", " ", "-date", "*")

		// citation fields - JOUR, VOL, ISS, PAGE, and LANG

		acc = append(acc, "-block", "MedlineJournalInfo", "-wrp", "JOUR", "-element", "MedlineTA", "NlmUniqueID", "ISSNLinking")
		acc = append(acc, "-block", "Article/Journal", "-wrp", "JOUR", "-jour", "Title", "ISOAbbreviation", "-element", "ISSN")
		acc = append(acc, "-wrp", "VOL", "-element", "Volume", "-wrp", "ISS", "-element", "Issue")
		acc = append(acc, "-block", "Article/Pagination", "-wrp", "PAGE", "-page", "MedlinePgn")
		acc = append(acc, "-block", "Article/Language", "-wrp", "LANG", "-element", "Language")

		// author fields - ANUM, AUTH, FAUT, LAUT, CSRT, INUM, and INVR

		// only count human authors, not consortia
		acc = append(acc, "-block", "AuthorList", "-wrp", "ANUM", "-num", "Author/LastName")
		// use -position to get first author
		acc = append(acc, "-block", "AuthorList/Author", "-position", "first")
		acc = append(acc, "-wrp", "FAUT", "-sep", " ", "-author", "LastName,Initials")
		// expect consortium to be last in the author list, so explore each author, and if last name is present,
		// overwrite the LAST variable with the current person's name
		acc = append(acc, "-block", "AuthorList/Author", "-if", "LastName", "-sep", " ", "-LAST", "LastName,Initials")
		// then explore on (single instance) PubmedArticle to print one copy of variable containing the last author's name
		acc = append(acc, "-block", "PubmedArticle", "-if", "&LAST", "-wrp", "LAUT", "-author", "&LAST")
		// separate field for consortia
		acc = append(acc, "-block", "AuthorList/Author", "-wrp", "CSRT", "-prose", "CollectiveName")
		// now get all human authors and investigators
		acc = append(acc, "-block", "AuthorList/Author", "-wrp", "AUTH", "-sep", " ", "-author", "LastName,Initials")
		acc = append(acc, "-block", "InvestigatorList/Investigator", "-wrp", "INVR", "-sep", " ", "-author", "LastName,Initials")
		// optionally index number of investigators
		// acc = append(acc, "-block", "InvestigatorList", "-wrp", "INUM", "-num", "Investigator/LastName")

		// title and abstract fields - TIAB, TITL, and PAIR

		// positional indices for TITL and TIAB fields
		acc = append(acc, "-block", "PubmedArticle", "-article", "ArticleTitle")
		acc = append(acc, "-block", "PubmedArticle", "-indices", "ArticleTitle,Abstract/AbstractText")
		// overlapping adjacent word pairs (or isolated singletons) separated by stop words
		acc = append(acc, "-block", "PubmedArticle", "-wrp", "PAIR", "-pairx", "ArticleTitle")

		// property fields - PROP and PTYP

		acc = append(acc, "-block", "PublicationType", "-wrp", "PTYP", "-element", "PublicationType")

		acc = append(acc, "-block", "CommentsCorrections", "-wrp", "PROP", "-prop", "@RefType")
		acc = append(acc, "-block", "PublicationStatus", "-wrp", "PROP", "-prop", "PublicationStatus")
		acc = append(acc, "-block", "Abstract", "-if", "AbstractText", "-wrp", "PROP", "-lbl", "Has Abstract")
		acc = append(acc, "-block", "MedlineCitation", "-if", "CoiStatement", "-wrp", "PROP", "-lbl", "Conflict of Interest Statement")
		// dates
		acc = append(acc, "-block", "Journal", "-if", "MedlineDate", "-wrp", "PROP", "-lbl", "Medline Date")
		acc = append(acc, "-subset", "MedlineDate", "-if", "%MedlineDate", "-lt", "4", "-wrp", "PROP", "-lbl", "Bad Date")
		acc = append(acc, "-block", "PubDate", "-if", "Year", "-and", "%Year", "-lt", "4", "-wrp", "PROP", "-lbl", "Bad Date")
		acc = append(acc, "-block", "PubMedPubDate", "-if", "%Year", "-lt", "4", "-wrp", "PROP", "-lbl", "Bad Date")
		acc = append(acc, "-block", "JournalIssue", "-if", "@CitedMedium", "-is-not", "Internet")
		acc = append(acc, "-subset", "PubDate", "-if", "Year", "-gt", currentYear, "-wrp", "PROP", "-lbl", "Future Date")
		acc = append(acc, "-block", "PubMedPubDate", "-if", "Year", "-gt", currentYear, "-and")
		acc = append(acc, "@PubStatus", "-is-not", "pmc-release", "-wrp", "PROP", "-lbl", "Future Date")
		// version
		acc = append(acc, "-block", "MedlineCitation", "-if", "PMID@Version", "-gt", "1", "-wrp", "PROP", "-lbl", "Versioned")

		// optionally index record size to find annotation outliers (e.g., PMID 33766997)
		// acc = append(acc, "-block", "PubmedArticle", "-wrp", "SIZE", "-len", "*")

		// if Extras/meshtree.txt is available, index CODE, TREE, and SUBH fields, and MESH for term list
		if tform != "" {
			acc = append(acc, "-block", "PubmedArticle", "-meshcode")
			acc = append(acc, "MeshHeading/DescriptorName@UI,Chemical/NameOfSubstance@UI,SupplMeshName@UI")
			acc = append(acc, "-block", "MeshHeading/QualifierName", "-wrp", "SUBH", "-element", "QualifierName")
			// only populating MESH for live term list, since query will redirect to wildcard on TREE
			acc = append(acc, "-block", "MeshHeading/DescriptorName", "-wrp", "MESH", "-element", "DescriptorName")
		}

	} else if db == "pmc" {

		acc = append(acc, "-set", "IdxDocumentSet", "-rec", "IdxDocument")
		acc = append(acc, "-pattern", "PMCExtract", "-UID", "PMCExtract/UID")
		acc = append(acc, "-wrp", "IdxUid", "-element", "&UID", "-clr", "-rst", "-tab", "")

		acc = append(acc, "-group", "PMCExtract", "-pkg", "IdxSearchFields")

		// identifier field - UID

		acc = append(acc, "-block", "PMCExtract", "-wrp", "UID", "-pad", "&UID")

		// date field - YEAR

		acc = append(acc, "-block", "PMCExtract", "-wrp", "YEAR", "-element", "YEAR")

		// citation field - JOUR

		acc = append(acc, "-block", "PMCExtract", "-wrp", "JOUR", "-element", "JOUR")
		acc = append(acc, "-block", "PMCExtract", "-wrp", "JOUR", "-element", "SRC")

		// author field - AUTH

		acc = append(acc, "-block", "PMCExtract", "-subset", "Auth", "-wrp", "AUTH", "-sep", " ", "-element", "LastName,Initials")

		// positional indices for title, abstract, and other text fields - TITL, ABST, TEXT

		acc = append(acc, "-block", "PMCExtract", "-article", "TITLE/TEXT")
		acc = append(acc, "-block", "PMCExtract", "-abstract", "ABSTRACT/TEXT")
		acc = append(acc, "-block", "PMCExtract", "-paragraph", "TEXT")

	} else if db == "taxonomy" {

		acc = append(acc, "-set", "IdxDocumentSet", "-rec", "IdxDocument")
		acc = append(acc, "-pattern", "TaxNode", "-UID", "TaxNode/TaxID")
		acc = append(acc, "-wrp", "IdxUid", "-element", "&UID", "-clr", "-rst", "-tab", "")

		acc = append(acc, "-group", "TaxNode", "-pkg", "IdxSearchFields")

		// identifier field - UID

		acc = append(acc, "-block", "TaxNode", "-wrp", "UID", "-pad", "&UID")

		// name fields - SCIN, COMN, TXSY

		acc = append(acc, "-block", "TaxNode", "-wrp", "SCIN", "-element", "ScientificName")
		acc = append(acc, "-block", "TaxNode", "-wrp", "COMN", "-element", "CommonName")
		acc = append(acc, "-block", "TaxNode", "-wrp", "COMN", "-element", "GenBankCommon")
		acc = append(acc, "-block", "TaxNode", "-wrp", "TXSY", "-element", "Synonym")

		// division field - TXDV

		acc = append(acc, "-block", "TaxNode", "-wrp", "TXDV", "-element", "Division")

		// lineage field - LNGE

		acc = append(acc, "-block", "TaxNode", "-wrp", "LNGE", "-element", "Lineage")

		// genetic fields - GC, MGC, PGC, HGC

		acc = append(acc, "-block", "TaxNode", "-wrp", "GC", "-element", "Nuclear")
		acc = append(acc, "-block", "TaxNode", "-wrp", "MGC", "-element", "Mitochondrial")
		acc = append(acc, "-block", "TaxNode", "-wrp", "PGC", "-element", "Plastid")
		acc = append(acc, "-block", "TaxNode", "-wrp", "HGC", "-element", "Hydrogenosome")
	}

	return acc
}

// UPDATE CACHED INVERTED FILES IN INDICES DIRECTORY FROM LOCAL ARCHIVE FOLDERS

// remove cached inverted index components with:
//
//   find /Volumes/cachet/Indices -name \*.inv.gz -type f -delete

// examineFolder collects two-digit subdirectories, xml files, and e2x files
func examineFolder(base, path string) ([]string, []string, []string) {

	dir := filepath.Join(base, path)

	contents, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, nil
	}

	isTwoDigits := func(str string) bool {

		if len(str) != 2 {
			return false
		}

		ch := str[0]
		if ch < '0' || ch > '9' {
			return false
		}

		ch = str[1]
		if ch < '0' || ch > '9' {
			return false
		}

		return true
	}

	var dirs []string
	var xmls []string
	var e2xs []string

	for _, item := range contents {
		name := item.Name()
		if name == "" {
			continue
		}
		if item.IsDir() {
			if isTwoDigits(name) {
				dirs = append(dirs, name)
			}
		} else if strings.HasSuffix(name, ".xml.gz") {
			xmls = append(xmls, name)
		} else if strings.HasSuffix(name, ".e2x.gz") {
			e2xs = append(e2xs, name)
		}
	}

	return dirs, xmls, e2xs
}

// gzFileToString reads selected gzipped file, uncompressing and saving contents as string
func gzFileToString(fpath string) string {

	file, err := os.Open(fpath)
	if err != nil {
		return ""
	}
	defer file.Close()

	var rdr io.Reader

	gz, err := gzip.NewReader(file)
	if err != nil {
		return ""
	}
	defer gz.Close()
	rdr = gz

	byt, err := io.ReadAll(rdr)
	if err != nil {
		return ""
	}

	str := string(byt)
	if str == "" {
		return ""
	}

	if !strings.HasSuffix(str, "\n") {
		str += "\n"
	}

	return str
}

func stringToGzFile(base, path, file, str string) {

	if str == "" {
		return
	}

	dpath := filepath.Join(base, path)
	if dpath == "" {
		return
	}
	err := os.MkdirAll(dpath, os.ModePerm)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		return
	}
	fpath := filepath.Join(dpath, file)
	if fpath == "" {
		return
	}

	// overwrites and truncates existing file
	fl, err := os.Create(fpath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		return
	}

	// for small files, use regular gzip
	zpr, err := gzip.NewWriterLevel(fl, gzip.BestSpeed)
	if err != nil {
		fmt.Fprintf(os.Stderr, "\nERROR: Unable to create compressor\n")
		os.Exit(1)
	}

	wrtr := bufio.NewWriter(zpr)

	// write contents
	wrtr.WriteString(str)
	if !strings.HasSuffix(str, "\n") {
		wrtr.WriteString("\n")
	}

	err = wrtr.Flush()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		return
	}

	err = zpr.Close()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		return
	}

	// fl.Sync()

	err = fl.Close()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		return
	}
}

// e2IndexConsumer callbacks have access to application-specific data as closures
type e2IndexConsumer func(inp <-chan XMLRecord) <-chan XMLRecord

// IncrementalIndex creates or updates missing cached .e2x.gz indexed files,
// e.g., /Index/02/53/025393.e2x.gz for /Archive/02/53/93/*.xml.gz
func IncrementalIndex(archiveBase, indexBase, db, pfx string, csmr e2IndexConsumer) <-chan string {

	if csmr == nil {
		return nil
	}

	if archiveBase == "" {

		// if not passed as an argument, obtain archive base path from environment variable
		base := os.Getenv("EDIRECT_PUBMED_MASTER")
		if base != "" {
			if !strings.HasSuffix(base, "/") {
				base += "/"
			}

			archiveBase = base + "Archive"
		}

		if archiveBase == "" {
			fmt.Fprintf(os.Stderr, "\nERROR: EDIRECT_PUBMED_MASTER environment variable is not set\n\n")
			os.Exit(1)
		}
	}

	if indexBase == "" {

		// if not passed as an argument, obtain index base path from environment variable
		base := os.Getenv("EDIRECT_PUBMED_WORKING")
		if base != "" {
			if !strings.HasSuffix(base, "/") {
				base += "/"
			}

			indexBase = base + "Index"
		}

		if indexBase == "" {
			fmt.Fprintf(os.Stderr, "\nERROR: EDIRECT_PUBMED_WORKING environment variable is not set\n\n")
			os.Exit(1)
		}
	}

	// check to make sure local archive is mounted
	_, err := os.Stat(archiveBase)
	if err != nil && os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "\nERROR: Local archive and search index is not mounted\n\n")
		os.Exit(1)
	}

	// check to make sure local index is mounted
	_, err = os.Stat(indexBase)
	if err != nil && os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "\nERROR: Local incremental index is not mounted\n\n")
		os.Exit(1)
	}

	// visitArchiveFolders sends an Archive leaf folder path plus the file base names
	// contained in it down a channel, e.g., "02/53/93/", "2539300", "2539301", ..., for
	// Archive/02/53/93/*.xml.gz
	visitArchiveFolders := func(archiveBase string) <-chan []string {

		out := make(chan []string, ChanDepth())
		if out == nil {
			fmt.Fprintf(os.Stderr, "\nERROR: Unable to create archive explorer channel\n")
			os.Exit(1)
		}

		// recursive definition
		var visitSubFolders func(base, path string, out chan<- []string)

		// visitSubFolders recurses to leaf directories
		visitSubFolders = func(base, path string, out chan<- []string) {

			dirs, xmls, _ := examineFolder(base, path)

			// recursively explore subdirectories
			if dirs != nil {
				for _, dr := range dirs {
					sub := filepath.Join(path, dr)
					visitSubFolders(base, sub, out)
				}
				return
			}

			// looking for leaf Archive directory with at least one *.xml.gz file
			if xmls == nil {
				return
			}

			// remove ".xml.gz" suffixes, leaving unpadded PMID
			for i, file := range xmls {
				pos := strings.Index(file, ".")
				if pos >= 0 {
					file = file[:pos]
				}
				xmls[i] = file
			}

			if len(xmls) > 1 {
				// sort fields in alphabetical or numeric order
				sort.Slice(xmls, func(i, j int) bool {
					// numeric sort on strings checks lengths first
					if IsAllDigits(xmls[i]) && IsAllDigits(xmls[j]) {
						lni := len(xmls[i])
						lnj := len(xmls[j])
						// shorter string is numerically less, assuming no leading zeros
						if lni < lnj {
							return true
						}
						if lni > lnj {
							return false
						}
					}
					// same length or non-numeric, can now do string comparison on contents
					return xmls[i] < xmls[j]
				})
			}

			var res []string
			res = append(res, path)
			res = append(res, xmls...)

			out <- res
		}

		visitArchiveSubset := func(base string, out chan<- []string) {

			defer close(out)

			dirs, _, _ := examineFolder(base, "")

			// iterate through top directories
			for _, top := range dirs {
				// skip Sentinels folder
				if IsAllDigits(top) && len(top) == 2 {
					visitSubFolders(base, top, out)
				}
			}
		}

		// launch single archive visitor goroutine
		go visitArchiveSubset(archiveBase, out)

		return out
	}

	// filterIndexFolders checks for presence of an Index file for an archive folder,
	// only passing those files that need to be (re)indexed
	filterIndexFolders := func(indexBase string, inp <-chan []string) <-chan XMLRecord {

		out := make(chan XMLRecord, ChanDepth())
		if out == nil {
			fmt.Fprintf(os.Stderr, "\nERROR: Unable to create index filter channel\n")
			os.Exit(1)
		}

		filterIndexSubset := func(indBase string, inp <-chan []string, out chan<- XMLRecord) {

			defer close(out)

			idx := 0

			for data := range inp {

				// path is first element in slice
				path := data[0]
				// "02/53/93/"

				// followed by xml file base names (PMIDs)
				pmids := data[1:]

				indPath := path[:6]
				// "02/53/"

				indFile := strings.Replace(path, "/", "", -1)
				// "025393"

				target := filepath.Join(indBase, indPath, indFile+".e2x.gz")

				_, err := os.Stat(target)
				if err == nil {
					// skip if first-level incremental Entrez index file exists for current set of 100 archive records
					continue
				}

				for _, pmid := range pmids {
					// increment index so unshuffler can restore order of resuls
					idx++

					// send PMID (unindexed file base name) down channel
					out <- XMLRecord{Index: idx, Ident: indFile, Text: pmid}
				}
			}
		}

		// launch single index filter goroutine
		go filterIndexSubset(indexBase, inp, out)

		return out
	}

	cleanIndexFiles := func(inp <-chan XMLRecord) <-chan XMLRecord {

		out := make(chan XMLRecord, ChanDepth())
		if out == nil {
			fmt.Fprintf(os.Stderr, "\nERROR: Unable to create index cleaner channel\n")
			os.Exit(1)
		}

		indexCleaner := func(wg *sync.WaitGroup, indBase string, inp <-chan XMLRecord, out chan<- XMLRecord) {

			defer wg.Done()

			re := regexp.MustCompile(">[ \n\r\t]*<")

			for curr := range inp {

				str := curr.Text

				if str == "" {
					continue
				}

				// clean up white space between stop tag and next start tag
				str = re.ReplaceAllString(str, ">\n<")

				if !strings.HasSuffix(str, "\n") {
					str += "\n"
				}

				out <- XMLRecord{Index: curr.Index, Ident: curr.Ident, Text: str}
			}
		}

		var wg sync.WaitGroup

		// launch multiple index cleaner goroutines
		for i := 0; i < NumServe(); i++ {
			wg.Add(1)
			go indexCleaner(&wg, indexBase, inp, out)
		}

		// launch separate anonymous goroutine to wait until all index cleaners are done
		go func() {
			wg.Wait()
			close(out)
		}()

		return out
	}

	combineIndexFiles := func(indexBase string, dotMax int, inp <-chan XMLRecord) <-chan string {

		out := make(chan string, ChanDepth())
		if out == nil {
			fmt.Fprintf(os.Stderr, "\nERROR: Unable to create index combiner channel\n")
			os.Exit(1)
		}

		// mutex to protect access to rollingCount and rollingColumn variables
		var vlock sync.Mutex

		rollingCount := 0
		rollingColumn := 0

		countSuccess := func() {

			vlock.Lock()

			rollingCount++
			if rollingCount >= dotMax {
				rollingCount = 0
				// print dot (progress monitor)
				fmt.Fprintf(os.Stderr, ".")
				rollingColumn++
				if rollingColumn > 49 {
					// print newline after 50 dots
					fmt.Fprintf(os.Stderr, "\n")
					rollingColumn = 0
				}
			}

			vlock.Unlock()
		}

		indexCombiner := func(indBase string, inp <-chan XMLRecord, out chan<- string) {

			defer close(out)

			currentIdent := ""

			var buffer strings.Builder

			verbose := false
			// set verbose flag from environment variable
			env := os.Getenv("EDIRECT_PUBMED_VERBOSE")
			if env == "Y" || env == "y" {
				verbose = true
			}

			for curr := range inp {

				str := curr.Text

				if str == "" {
					continue
				}

				ident := curr.Ident

				if ident != currentIdent && currentIdent != "" {
					txt := buffer.String()
					indPath, _ := IndexTrie(currentIdent + "00")
					stringToGzFile(indBase, indPath, currentIdent+".e2x.gz", txt)
					buffer.Reset()

					if verbose {
						fmt.Fprintf(os.Stderr, "IDX %s/%s%s.e2x.gz\n", indBase, indPath, currentIdent)
					} else {
						// progress monitor
						countSuccess()
					}

					out <- currentIdent
				}

				currentIdent = ident

				buffer.WriteString(str)

			}

			if currentIdent != "" {
				txt := buffer.String()
				indPath, _ := IndexTrie(currentIdent + "00")
				stringToGzFile(indBase, indPath, currentIdent+".e2x.gz", txt)
				buffer.Reset()

				if verbose {
					fmt.Fprintf(os.Stderr, "IDX %s/%s%s.e2x.gz\n", indBase, indPath, currentIdent)
				}
			}

			if rollingColumn > 0 {
				vlock.Lock()
				fmt.Fprintf(os.Stderr, "\n")
				vlock.Unlock()
			}
		}

		// launch single index combiner goroutine
		go indexCombiner(indexBase, inp, out)

		return out
	}

	// show a dot every 200 .e2x files, generated from up to 20000 .xml files
	dotMax := 200
	if db == "pmc" {
		dotMax = 50
	} else if db == "taxonomy" {
		dotMax = 500
	}

	vrfq := visitArchiveFolders(archiveBase)
	vifq := filterIndexFolders(indexBase, vrfq)
	strq := CreateFetchers(archiveBase, "", pfx, ".xml", true, vifq)
	// callback passes cmds and transform values as closures to xtract createConsumers
	tblq := csmr(strq)
	// clean up XML (no measured benefit to adding next record size prefix)
	sifq := cleanIndexFiles(tblq)
	// restore original order, so indexed results are grouped by archive folder
	unsq := CreateXMLUnshuffler(sifq)
	cifq := combineIndexFiles(indexBase, dotMax, unsq)

	if vrfq == nil || vifq == nil || strq == nil || tblq == nil || sifq == nil || unsq == nil || cifq == nil {
		return nil
	}

	return cifq
}

// InvertIndexedFile reads IdxDocument XML strings and writes a combined InvDocument XML record
func InvertIndexedFile(inp <-chan string) <-chan string {

	if inp == nil {
		return nil
	}

	indexDispenser := func(inp <-chan string) <-chan []string {

		if inp == nil {
			return nil
		}

		out := make(chan []string, ChanDepth())
		if out == nil {
			fmt.Fprintf(os.Stderr, "\nERROR: Unable to create dispenser channel\n")
			os.Exit(1)
		}

		type Inverter struct {
			ilock sync.Mutex
			// map for inverted index
			inverted map[string][]string
		}

		inverters := make(map[rune]*Inverter)

		prefixes := "01234567890abcdefghijklmnopqrstuvwxyz"

		for _, ch := range prefixes {
			inverters[ch] = &Inverter{inverted: make(map[string][]string)}
		}

		// add single posting
		addPost := func(fld, term, pos, uid string) {

			ch := rune(term[0])
			inv := inverters[ch]

			// protect map with mutex
			inv.ilock.Lock()

			data, ok := inv.inverted[term]
			if !ok {
				data = make([]string, 0, 4)
				// first entry on new slice is term
				data = append(data, term)
			}
			data = append(data, fld)
			data = append(data, uid)
			data = append(data, pos)
			// always need to update inverted, since data may be reallocated
			inv.inverted[term] = data

			inv.ilock.Unlock()
		}

		// xmlDispenser prepares UID, term, and position strings for inversion
		xmlDispenser := func(wg *sync.WaitGroup, inp <-chan string, out chan<- []string) {

			defer wg.Done()

			currUID := ""

			doDispense := func(tag, attr, content string) {

				if tag == "IdxUid" {
					currUID = content
				} else {

					content = html.UnescapeString(content)

					// expand Greek letters, anglicize characters in other alphabets
					if IsNotASCII(content) {

						content = TransformAccents(content, true, true)

						if HasAdjacentSpacesOrNewline(content) {
							content = CompressRunsOfSpaces(content)
						}

						content = UnicodeToASCII(content)

						if HasFlankingSpace(content) {
							content = strings.TrimSpace(content)
						}
					}

					content = strings.ToLower(content)

					// remove punctuation from term
					content = strings.Map(func(c rune) rune {
						if !unicode.IsLetter(c) && !unicode.IsDigit(c) && c != ' ' && c != '-' && c != '_' {
							return -1
						}
						return c
					}, content)

					content = strings.Replace(content, "_", " ", -1)
					content = strings.Replace(content, "-", " ", -1)

					if HasAdjacentSpacesOrNewline(content) {
						content = CompressRunsOfSpaces(content)
					}

					if HasFlankingSpace(content) {
						content = strings.TrimSpace(content)
					}

					if content != "" && currUID != "" {
						addPost(tag, content, attr, currUID)
					}
				}
			}

			// read partitioned XML from producer channel
			for str := range inp {
				StreamValues(str[:], "IdxDocument", doDispense)
			}
		}

		var wg sync.WaitGroup

		// launch multiple dispenser goroutines
		for i := 0; i < NumServe(); i++ {
			wg.Add(1)
			go xmlDispenser(&wg, inp, out)
		}

		// launch separate anonymous goroutine to wait until all dispensers are done
		go func() {
			wg.Wait()

			// send results to inverters
			for _, ch := range prefixes {
				inv := inverters[ch]
				for _, data := range inv.inverted {
					out <- data

					runtime.Gosched()
				}
			}

			close(out)
		}()

		return out
	}

	indexInverter := func(inp <-chan []string) <-chan XMLRecord {

		if inp == nil {
			return nil
		}

		out := make(chan XMLRecord, ChanDepth())
		if out == nil {
			fmt.Fprintf(os.Stderr, "\nERROR: Unable to create inverter channel\n")
			os.Exit(1)
		}

		// xmlInverter sorts and prints one posting list
		xmlInverter := func(wg *sync.WaitGroup, inp <-chan []string, out chan<- XMLRecord) {

			defer wg.Done()

			var buffer strings.Builder

			printPosting := func(key string, data []string) string {

				fields := make(map[string]map[string]string)

				for len(data) > 1 {
					fld := data[0]
					uid := data[1]
					att := data[2]
					positions, ok := fields[fld]
					if !ok {
						positions = make(map[string]string)
						fields[fld] = positions
					}
					// store position attribute string by uid
					positions[uid] = att
					// skip to next position
					data = data[3:]
				}

				buffer.Reset()

				buffer.WriteString("<InvDocument>\n")
				buffer.WriteString("<InvKey>")
				buffer.WriteString(key)
				buffer.WriteString("</InvKey>\n")
				buffer.WriteString("<InvIDs>\n")

				// sort fields in alphabetical order
				var keys []string
				for ky := range fields {
					keys = append(keys, ky)
				}
				sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

				for _, fld := range keys {

					positions := fields[fld]

					var arry []string

					for item := range positions {
						arry = append(arry, item)
					}

					if len(arry) > 1 {
						sort.Slice(arry, func(i, j int) bool {
							// numeric sort on strings checks lengths first
							lni := len(arry[i])
							lnj := len(arry[j])
							// shorter string is numerically less, assuming no leading zeros
							if lni < lnj {
								return true
							}
							if lni > lnj {
								return false
							}
							// same length, can now do string comparison on contents
							return arry[i] < arry[j]
						})
					}

					// print list of UIDs, skipping duplicates
					prev := ""
					for _, uid := range arry {
						if uid == prev {
							continue
						}

						buffer.WriteString("<")
						buffer.WriteString(fld)
						atr := positions[uid]
						if atr != "" {
							buffer.WriteString(" ")
							buffer.WriteString(atr)
						}
						buffer.WriteString(">")
						buffer.WriteString(uid)
						buffer.WriteString("</")
						buffer.WriteString(fld)
						buffer.WriteString(">\n")

						prev = uid
					}
				}

				buffer.WriteString("</InvIDs>\n")
				buffer.WriteString("</InvDocument>\n")

				str := buffer.String()

				return str
			}

			for inv := range inp {

				key := inv[0]
				data := inv[1:]

				str := printPosting(key, data)

				out <- XMLRecord{Ident: key, Text: str}

				runtime.Gosched()
			}
		}

		var wg sync.WaitGroup

		// launch multiple inverter goroutines
		for i := 0; i < NumServe(); i++ {
			wg.Add(1)
			go xmlInverter(&wg, inp, out)
		}

		// launch separate anonymous goroutine to wait until all inverters are done
		go func() {
			wg.Wait()
			close(out)
		}()

		return out
	}

	indexResolver := func(inp <-chan XMLRecord) <-chan string {

		if inp == nil {
			return nil
		}

		out := make(chan string, ChanDepth())
		if out == nil {
			fmt.Fprintf(os.Stderr, "\nERROR: Unable to create resolver channel\n")
			os.Exit(1)
		}

		// xmlResolver prints inverted postings alphabetized by identifier prefix
		xmlResolver := func(inp <-chan XMLRecord, out chan<- string) {

			// close channel when all records have been processed
			defer close(out)

			// map for inverted index
			inverted := make(map[string]string)

			// drain channel, populate map for alphabetizing
			for curr := range inp {

				inverted[curr.Ident] = curr.Text
			}

			var ordered []string

			for item := range inverted {
				ordered = append(ordered, item)
			}

			if len(ordered) > 1 {
				sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
			}

			// iterate through alphabetized results
			for _, curr := range ordered {

				txt := inverted[curr]

				// send result to output
				out <- txt

				runtime.Gosched()
			}
		}

		// launch single resolver goroutine
		go xmlResolver(inp, out)

		return out
	}

	idsq := indexDispenser(inp)
	invq := indexInverter(idsq)
	idrq := indexResolver(invq)

	if idsq == nil || invq == nil || idrq == nil {
		return nil
	}

	return idrq
}

// IncrementalInvert creates or updates missing cached .inv.gz inverted index files
func IncrementalInvert(indexBase, invertBase, db string) <-chan string {

	if indexBase == "" || invertBase == "" {

		// if not passed as an argument, obtain index base path from environment variable
		base := os.Getenv("EDIRECT_PUBMED_WORKING")
		if base != "" {
			if !strings.HasSuffix(base, "/") {
				base += "/"
			}

			indexBase = base + "Index"
			invertBase = base + "Invert"
		}

		if indexBase == "" {
			fmt.Fprintf(os.Stderr, "\nERROR: EDIRECT_PUBMED_WORKING environment variable is not set\n\n")
			os.Exit(1)
		}
	}

	// check to make sure local index directory is mounted
	_, err := os.Stat(indexBase)
	if err != nil && os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "\nERROR: Local index directory is not mounted\n\n")
		os.Exit(1)
	}

	// check to make sure local invert directory is mounted
	_, err = os.Stat(invertBase)
	if err != nil && os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "\nERROR: Local invert directory is not mounted\n\n")
		os.Exit(1)
	}

	out := make(chan string, ChanDepth())
	if out == nil {
		fmt.Fprintf(os.Stderr, "\nERROR: Unable to create inverter channel\n")
		os.Exit(1)
	}

	indexFetchers := func(inp <-chan string) <-chan string {

		if inp == nil {
			return nil
		}

		out := make(chan string, ChanDepth())
		if out == nil {
			fmt.Fprintf(os.Stderr, "\nERROR: Unable to create index fetcher channel\n")
			os.Exit(1)
		}

		// e2xFetcher reads indexed XML from file
		e2xFetcher := func(wg *sync.WaitGroup, inp <-chan string, out chan<- string) {

			// report when more records to process
			defer wg.Done()

			for file := range inp {

				txt := gzFileToString(file)

				runtime.Gosched()

				out <- txt
			}
		}

		var wg sync.WaitGroup

		// launch multiple fetcher goroutines
		for i := 0; i < NumServe(); i++ {
			wg.Add(1)
			go e2xFetcher(&wg, inp, out)
		}

		// launch separate anonymous goroutine to wait until all fetchers are done
		go func() {
			wg.Wait()
			close(out)
		}()

		return out
	}

	// invert one set of 250 indexed files, each representing 1000 pubmed or pmc records
	invertIndexFiles := func(target string, filenames []string) {

		s2cq := SliceToChan(filenames)
		idfq := indexFetchers(s2cq)
		// indexDispenser | indexInverter | indexResolver
		iifq := InvertIndexedFile(idfq)

		var buffer strings.Builder

		for str := range iifq {
			buffer.WriteString(str)
		}

		txt := buffer.String()
		if txt == "" {
			fmt.Fprintf(os.Stderr, "Empty %s\n", target)
			return
		}

		// save to target file
		stringToGzFile(invertBase, "", target, txt)
	}

	visitIndexSubset := func(indexBase, invertBase string, out chan<- string) {

		defer close(out)

		num := 0

		fileBase := "pubmed"
		if db == "pmc" {
			fileBase = "pmc"
		} else if db == "taxonomy" {
			fileBase = "taxonomy"
		}

		visitSubFolders := func(path, fr, to string) {

			// increment output file number
			num++

			sfx := fmt.Sprintf("%03d", num)
			fname := fileBase + sfx + ".inv.gz"

			target := filepath.Join(invertBase, fname)

			// incremental inverted index file is removed when records in relevant range are archived or deleted
			_, err := os.Stat(target)
			if err == nil {
				// if inverted index file exists, no need to recreate
				return
			}

			mids, _, _ := examineFolder(indexBase, path)

			var filenames []string

			for _, fld := range mids {

				// filter second-level directories by range
				if fld < fr {
					continue
				}
				if fld > to {
					break
				}

				sub := filepath.Join(path, fld)
				_, _, e2xs := examineFolder(indexBase, sub)

				for _, file := range e2xs {

					// path for indexed file within current leaf folder
					fpath := filepath.Join(indexBase, sub, file)

					filenames = append(filenames, fpath)
				}
			}

			if len(filenames) < 1 {
				return
			}

			invertIndexFiles(fname, filenames)

			out <- fname
		}

		// get top-level directories
		dirs, _, _ := examineFolder(indexBase, "")

		// iterate through top directories
		for _, dr := range dirs {

			// skip Sentinels folder
			if !IsAllDigits(dr) {
				continue
			}

			// collect inverted index files from groups of pubmed or pmc .xml files
			if db == "" || db == "pubmed" {
				// 250000 pubmed records
				visitSubFolders(dr, "00", "24")
				visitSubFolders(dr, "25", "49")
				visitSubFolders(dr, "50", "74")
				visitSubFolders(dr, "75", "99")
			} else if db == "pmc" {
				// 10000 pmc records
				visitSubFolders(dr, "00", "09")
				visitSubFolders(dr, "10", "19")
				visitSubFolders(dr, "20", "29")
				visitSubFolders(dr, "30", "39")
				visitSubFolders(dr, "40", "49")
				visitSubFolders(dr, "50", "59")
				visitSubFolders(dr, "60", "69")
				visitSubFolders(dr, "70", "79")
				visitSubFolders(dr, "80", "89")
				visitSubFolders(dr, "90", "99")
			} else if db == "taxonomy" {
				// 250000 taxonomy records
				visitSubFolders(dr, "00", "24")
				visitSubFolders(dr, "25", "49")
				visitSubFolders(dr, "50", "74")
				visitSubFolders(dr, "75", "99")
			}
		}
	}

	// launch single index visitor goroutine
	go visitIndexSubset(indexBase, invertBase, out)

	return out
}
