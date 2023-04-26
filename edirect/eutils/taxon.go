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
// File Name:  taxon.go
//
// Author:  Jonathan Kans
//
// ==========================================================================

package eutils

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// TaxNames has alternative names for a given node
type TaxNames struct {
	Common     []string
	GenBank    []string
	Synonym    []string
	Equivalent []string
	Includes   []string
	Authority  []string
	Other      []string
}

// TaxLevels has fields for well-established taxonomic ranks
type TaxLevels struct {
	Species      string
	Genus        string
	Family       string
	Order        string
	Class        string
	Phylum       string
	Kingdom      string
	Superkingdom string
}

// TaxCodes holds nuclear, mitochondrial, plastid, and hydrogenosome genetic codes
type TaxCodes struct {
	Nuclear       string
	Mitochondrial string
	Plastid       string
	Hydrogenosome string
}

// TaxFlags has miscellaneous flags
type TaxFlags struct {
	InheritDiv   bool
	InheritNuc   bool
	InheritMito  bool
	InheritPlast bool
	InheritHydro bool
}

// TaxNode is the master structure for archived, indexed taxonomy record
type TaxNode struct {
	TaxID      string
	Scientific string
	Rank       string
	Division   string
	Lineage    string
	Names      TaxNames
	Levels     TaxLevels
	Codes      TaxCodes
	Flags      TaxFlags
	ParentID   string
}

// CreateTaxonRecords reads taxonomy files and create TaxNode records
func CreateTaxonRecords(path string) int {

	recordCount := 0

	taxNodeMap := make(map[string]*TaxNode)

	divCodes := make(map[string]string)
	divNames := make(map[string]string)

	readDivisions := func(fname string) {

		fpath := filepath.Join(path, fname)
		inFile, err := os.Open(fpath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Unable to open division file %s - %s\n", fpath, err.Error())
			os.Exit(1)
		}

		scant := bufio.NewScanner(inFile)

		lineNum := 0

		for scant.Scan() {

			line := scant.Text()
			lineNum++
			cols := strings.Split(line, "\t")
			if len(cols) != 8 {
				fmt.Fprintf(os.Stderr, "ERROR: %d Columns on Row %d of File %s\n", len(cols), lineNum, fname)
				continue
			}

			divID := cols[0]
			divCode := cols[2]
			divName := cols[4]

			if divID == "" || divCode == "" || divName == "" {
				continue
			}

			// looks up with 1-digit number in node table
			divCodes[divID] = divCode
			// looks up with 3-letter abbreviation
			divNames[divCode] = divName
		}

		inFile.Close()
	}

	readNameTable := func(fname string) {

		fpath := filepath.Join(path, fname)
		inFile, err := os.Open(fpath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Unable to open taxonomy name file %s - %s\n", fpath, err.Error())
			os.Exit(1)
		}

		scant := bufio.NewScanner(inFile)

		lineNum := 0

		for scant.Scan() {

			line := scant.Text()
			lineNum++
			cols := strings.Split(line, "\t")
			if len(cols) != 8 {
				fmt.Fprintf(os.Stderr, "ERROR: %d Columns on Row %d of File %s\n", len(cols), lineNum, fname)
				continue
			}

			taxID := cols[0]
			taxName := cols[2]
			nameClass := cols[6]

			if taxID == "" || taxName == "" || nameClass == "" {
				continue
			}

			tn, ok := taxNodeMap[taxID]
			if !ok {
				tn = &TaxNode{TaxID: taxID}
				taxNodeMap[taxID] = tn
			}
			if tn == nil {
				continue
			}

			// take address in order to modify data in map
			nam := &tn.Names

			switch nameClass {
			case "scientific name":
				tn.Scientific = taxName
			case "common name":
				nam.Common = append(nam.Common, taxName)
			case "genbank common name":
				nam.GenBank = append(nam.GenBank, taxName)
			case "synonym":
				nam.Synonym = append(nam.Synonym, taxName)
			case "equivalent name":
				nam.Equivalent = append(nam.Equivalent, taxName)
			case "includes":
				nam.Includes = append(nam.Includes, taxName)
			case "authority":
				nam.Authority = append(nam.Authority, taxName)
			default:
				nam.Other = append(nam.Other, taxName)
			}
		}

		inFile.Close()
	}

	readFullLineage := func(fname string) {

		fpath := filepath.Join(path, fname)
		inFile, err := os.Open(fpath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Unable to open full lineage file %s - %s\n", fpath, err.Error())
			os.Exit(1)
		}

		scant := bufio.NewScanner(inFile)

		lineNum := 0

		for scant.Scan() {

			line := scant.Text()
			lineNum++
			cols := strings.Split(line, "\t")
			if len(cols) != 6 {
				fmt.Fprintf(os.Stderr, "ERROR: %d Columns on Row %d of File %s\n", len(cols), lineNum, fname)
				continue
			}

			taxID := cols[0]
			taxName := cols[2]
			lineage := cols[4]

			if taxID == "" || taxName == "" || lineage == "" {
				continue
			}

			tn, ok := taxNodeMap[taxID]
			if !ok {
				continue
			}

			sci := strings.ToLower(tn.Scientific)
			sci = TransformAccents(sci, true, false)
			txn := strings.ToLower(taxName)
			txn = TransformAccents(txn, true, false)

			if sci != txn {
				fmt.Fprintf(os.Stderr, "ERROR: FullTax Mismatch in TaxID %s\n%s - %s\n", tn.TaxID, sci, txn)
				continue
			}

			lineage = strings.TrimSuffix(lineage, "; ")
			tn.Lineage = lineage
		}

		inFile.Close()
	}

	readRankedLineage := func(fname string) {

		fpath := filepath.Join(path, fname)
		inFile, err := os.Open(fpath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Unable to open ranked lineage file %s - %s\n", fpath, err.Error())
			os.Exit(1)
		}

		scant := bufio.NewScanner(inFile)

		lineNum := 0

		for scant.Scan() {

			line := scant.Text()
			lineNum++
			cols := strings.Split(line, "\t")
			if len(cols) != 20 {
				fmt.Fprintf(os.Stderr, "ERROR: %d Columns on Row %d of File %s\n", len(cols), lineNum, fname)
				continue
			}

			taxID := cols[0]
			taxName := cols[2]
			species := cols[4]
			genus := cols[6]
			family := cols[8]
			order := cols[10]
			class := cols[12]
			phylum := cols[14]
			kingdom := cols[16]
			superkingdom := cols[18]

			if taxID == "" || taxName == "" {
				continue
			}

			tn, ok := taxNodeMap[taxID]
			if !ok {
				continue
			}

			sci := strings.ToLower(tn.Scientific)
			sci = TransformAccents(sci, true, false)
			txn := strings.ToLower(taxName)
			txn = TransformAccents(txn, true, false)
			if sci != txn {
				fmt.Fprintf(os.Stderr, "ERROR: RankTax Mismatch in TaxID %s\n%s - %s\n", tn.TaxID, sci, txn)
				continue
			}

			// take address in order to modify data in map
			rnk := &tn.Levels

			rnk.Species = species
			rnk.Genus = genus
			rnk.Family = family
			rnk.Order = order
			rnk.Class = class
			rnk.Phylum = phylum
			rnk.Kingdom = kingdom
			rnk.Superkingdom = superkingdom
		}

		inFile.Close()
	}

	readNodeTable := func(fname string) {

		fpath := filepath.Join(path, fname)
		inFile, err := os.Open(fpath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Unable to open node file %s - %s\n", fpath, err.Error())
			os.Exit(1)
		}

		scant := bufio.NewScanner(inFile)

		lineNum := 0

		for scant.Scan() {

			line := scant.Text()
			lineNum++
			cols := strings.Split(line, "\t")
			if len(cols) != 36 {
				fmt.Fprintf(os.Stderr, "ERROR: %d Columns on Row %d of File %s\n", len(cols), lineNum, fname)
				continue
			}

			taxID := cols[0]
			parentID := cols[2]
			rank := cols[4]
			divID := cols[8]
			inheritDiv := cols[10]
			nucCode := cols[12]
			inheritNuc := cols[14]
			mitoCode := cols[16]
			inheritMito := cols[18]
			plastCode := cols[26]
			inheritPlast := cols[28]
			hydroCode := cols[32]
			inheritHydro := cols[34]

			if taxID == "" {
				continue
			}

			tn, ok := taxNodeMap[taxID]
			if !ok {
				continue
			}

			tn.Rank = rank

			if divID != "" && divID != "0" {
				code, ok := divCodes[divID]
				if ok && code != "" && code != "0" {
					tn.Division = code
				}
			}

			// normalize to empty
			if nucCode == "0" {
				nucCode = ""
			}
			if mitoCode == "0" {
				mitoCode = ""
			}
			if plastCode == "0" {
				plastCode = ""
			}
			if hydroCode == "0" {
				hydroCode = ""
			}

			// take address in order to modify data in map
			gcs := &tn.Codes

			gcs.Nuclear = nucCode
			gcs.Mitochondrial = mitoCode
			gcs.Plastid = plastCode
			gcs.Hydrogenosome = hydroCode

			// take address in order to modify data in map
			flg := &tn.Flags

			if inheritDiv == "1" {
				flg.InheritDiv = true
			}
			if inheritNuc == "1" {
				flg.InheritNuc = true
			}
			if inheritMito == "1" {
				flg.InheritMito = true
			}
			if inheritPlast == "1" {
				flg.InheritPlast = true
			}
			if inheritHydro == "1" {
				flg.InheritHydro = true
			}

			tn.ParentID = parentID
		}

		inFile.Close()
	}

	readDivisions("division.dmp")
	readNameTable("names.dmp")
	readFullLineage("fullnamelineage.dmp")
	readRankedLineage("rankedlineage.dmp")
	readNodeTable("nodes.dmp")

	var keys []string
	for _, tn := range taxNodeMap {
		keys = append(keys, tn.TaxID)
	}
	sort.Slice(keys, func(i, j int) bool {
		// numeric sort on strings checks lengths first
		if IsAllDigits(keys[i]) && IsAllDigits(keys[j]) {
			lni := len(keys[i])
			lnj := len(keys[j])
			// shorter string is numerically less, assuming no leading zeros
			if lni < lnj {
				return true
			}
			if lni > lnj {
				return false
			}
		}
		// same length or non-numeric, can now do string comparison on contents
		return keys[i] < keys[j]
	})

	var buffer strings.Builder
	count := 0
	okay := false

	wrtr := bufio.NewWriter(os.Stdout)

	printOne := func(spaces, name, value string) {

		if value != "" {
			if strings.Contains(value, "<") || strings.Contains(value, ">") || strings.Contains(value, "&") {
				// reencode < and > to avoid breaking XML
				value = rfix.Replace(value)
			}
			buffer.WriteString(spaces)
			buffer.WriteString("<")
			buffer.WriteString(name)
			buffer.WriteString(">")
			buffer.WriteString(value)
			buffer.WriteString("</")
			buffer.WriteString(name)
			buffer.WriteString(">\n")
		}
	}

	printMany := func(spaces, name string, values []string) {

		for _, str := range values {
			printOne(spaces, name, str)
		}
	}

	first := true

	for _, key := range keys {
		if first {
			buffer.WriteString("<TaxNodeSet>\n")
			first = false
		}

		buffer.WriteString("<TaxNode>\n")

		tn := taxNodeMap[key]

		printOne("  ", "TaxID", tn.TaxID)
		printOne("  ", "Rank", tn.Rank)
		printOne("  ", "Scientific", tn.Scientific)
		printOne("  ", "Division", tn.Division)
		printOne("  ", "Lineage", tn.Lineage)

		nam := &tn.Names

		printMany("  ", "Common", nam.Common)
		printMany("  ", "GenBank", nam.GenBank)
		printMany("  ", "Synonym", nam.Synonym)
		printMany("  ", "Equivalent", nam.Equivalent)
		printMany("  ", "Includes", nam.Includes)
		printMany("  ", "Authority", nam.Authority)
		printMany("  ", "Other", nam.Other)

		rnk := tn.Levels

		printOne("  ", "Species", rnk.Species)
		printOne("  ", "Genus", rnk.Genus)
		printOne("  ", "Family", rnk.Family)
		printOne("  ", "Order", rnk.Order)
		printOne("  ", "Class", rnk.Class)
		printOne("  ", "Phylum", rnk.Phylum)
		printOne("  ", "Kingdom", rnk.Kingdom)
		printOne("  ", "Superkingdom", rnk.Superkingdom)

		gcs := tn.Codes

		printOne("  ", "Nuclear", gcs.Nuclear)
		printOne("  ", "Mitochondrial", gcs.Mitochondrial)
		printOne("  ", "Plastid", gcs.Plastid)
		printOne("  ", "Hydrogenosome", gcs.Hydrogenosome)

		flg := tn.Flags

		if flg.InheritDiv {
			printOne("  ", "InheritsDiv", "1")
		}
		if flg.InheritNuc {
			printOne("  ", "InheritsNuc", "1")
		}
		if flg.InheritMito {
			printOne("  ", "InheritsMito", "1")
		}
		if flg.InheritPlast {
			printOne("  ", "InheritsPlast", "1")
		}
		if flg.InheritHydro {
			printOne("  ", "InheritsHydro", "1")
		}

		printOne("  ", "ParentID", tn.ParentID)

		buffer.WriteString("</TaxNode>\n")

		recordCount++
		count++

		if count >= 1000 {
			count = 0
			txt := buffer.String()
			if txt != "" {
				// print current buffer
				wrtr.WriteString(txt[:])
			}
			buffer.Reset()
		}

		okay = true
	}

	if okay {
		if !first {
			buffer.WriteString("</TaxNodeSet>\n")
		}
		txt := buffer.String()
		if txt != "" {
			// print current buffer
			wrtr.WriteString(txt[:])
		}
	}
	buffer.Reset()

	wrtr.Flush()

	return recordCount
}
