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
// File Name:  extern.go
//
// Author:  Jonathan Kans
//
// ==========================================================================

package eutils

import (
	"bufio"
	"fmt"
	"github.com/klauspost/pgzip"
	"html"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
)

// EXTERNAL INDEXED FILE GENERATORS

// CreateExternalIndexer handles NLP-extracted terms, GeneRIFs, and NIH OCC links
func CreateExternalIndexer(args []string, zipp bool, in io.Reader) int {

	recordCount := 0

	transform := make(map[string]string)

	synonyms := make(map[string]string)

	readMappingTable := func(tf string, tbl map[string]string) {

		inFile, err := os.Open(tf)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Unable to open transformation file %s - %s\n", tf, err.Error())
			os.Exit(1)
		}

		scant := bufio.NewScanner(inFile)

		// populate transformation map
		for scant.Scan() {

			line := scant.Text()
			frst, scnd := SplitInTwoLeft(line, "\t")

			tbl[frst] = scnd
		}

		inFile.Close()
	}

	// BIOCONCEPTS INDEXER

	// create intermediate table for {chemical|disease|gene}2pubtatorcentral.gz (undocumented)
	if args[0] == "-bioconcepts" {

		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "\nERROR: Insufficient arguments for -bioconcepts\n")
			os.Exit(1)
		}

		// read transformation file
		tf := args[1]
		readMappingTable(tf, transform)

		var buffer strings.Builder
		count := 0
		okay := false

		wrtr := bufio.NewWriter(os.Stdout)

		scanr := bufio.NewScanner(in)

		currpmid := ""

		// read lines of PMIDs and extracted concepts
		for scanr.Scan() {

			line := scanr.Text()

			cols := strings.Split(line, "\t")
			if len(cols) != 5 {
				continue
			}

			pmid := cols[0]
			if currpmid != pmid {
				// end current block
				currpmid = pmid

				if pmid == "" {
					continue
				}

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

			addItemtoIndex := func(fld, val string) {

				buffer.WriteString(pmid)
				buffer.WriteString("\t")
				buffer.WriteString(fld)
				buffer.WriteString("\t")
				buffer.WriteString(val)
				buffer.WriteString("\n")
			}

			typ := cols[1]
			val := cols[2]
			switch typ {
			case "Gene":
				genes := strings.Split(val, ";")
				for _, gene := range genes {
					if gene == "None" {
						continue
					}
					addItemtoIndex("GENE", gene)
					gn, ok := transform[gene]
					if !ok || gn == "" {
						continue
					}
					addItemtoIndex("PREF", gn)
					addItemtoIndex("GENE", gn)
				}
			case "Disease":
				if strings.HasPrefix(val, "MESH:") {
					diszs := strings.Split(val[5:], "|")
					for _, disz := range diszs {
						addItemtoIndex("DISZ", disz)
						dn, ok := transform[disz]
						if !ok || dn == "" {
							continue
						}
						addItemtoIndex("DISZ", dn)
					}
				} else if strings.HasPrefix(val, "OMIM:") {
					omims := strings.Split(val[5:], "|")
					for _, omim := range omims {
						// was OMIM, now fused with DISZ, tag OMIM identifiers with M prefix
						addItemtoIndex("DISZ", "M"+omim)
					}
				}
			case "Chemical":
				if strings.HasPrefix(val, "MESH:") {
					chems := strings.Split(val[5:], "|")
					for _, chem := range chems {
						addItemtoIndex("CHEM", chem)
						ch, ok := transform[chem]
						if !ok || ch == "" {
							continue
						}
						addItemtoIndex("CHEM", ch)
					}
				} else if strings.HasPrefix(val, "CHEBI:") {
					chebs := strings.Split(val[6:], "|")
					for _, cheb := range chebs {
						// was CEBI, now fused with CHEM, tag CHEBI identifiers with H prefix
						addItemtoIndex("CHEM", "H"+cheb)
					}
				}
			case "Species":
			case "Mutation":
			case "CellLine":
			default:
			}
		}

		if okay {
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

	// GENERIF INDEXER

	// create intermediate table for generifs_basic.gz (undocumented)
	if args[0] == "-generif" || args[0] == "-generifs" {

		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "\nERROR: Insufficient arguments for -generif\n")
			os.Exit(1)
		}

		// read gene transformation file
		tf := args[1]
		readMappingTable(tf, transform)

		if len(args) > 2 {
			// read optional gene synonym file
			tf := args[2]
			readMappingTable(tf, synonyms)
		}

		var buffer strings.Builder
		count := 0
		okay := false

		wrtr := bufio.NewWriter(os.Stdout)

		scanr := bufio.NewScanner(in)

		currpmid := ""

		// skip first line with column heading names
		for scanr.Scan() {

			line := scanr.Text()
			cols := strings.Split(line, "\t")
			if len(cols) != 5 {
				fmt.Fprintf(os.Stderr, "Unexpected number of columns (%d) in generifs_basic.gz\n", len(cols))
				os.Exit(1)
			}
			if len(cols) != 5 || cols[0] != "#Tax ID" {
				fmt.Fprintf(os.Stderr, "Unrecognized contents in generifs_basic.gz\n")
				os.Exit(1)
			}
			break
		}

		// read lines of PMIDs and gene references
		for scanr.Scan() {

			line := scanr.Text()

			cols := strings.Split(line, "\t")
			if len(cols) != 5 {
				continue
			}

			val := cols[2]
			pmids := strings.Split(val, ",")
			for _, pmid := range pmids {
				if currpmid != pmid {
					// end current block
					currpmid = pmid

					if pmid == "" {
						continue
					}

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

				addItemtoIndex := func(fld, val string) {

					buffer.WriteString(pmid)
					buffer.WriteString("\t")
					buffer.WriteString(fld)
					buffer.WriteString("\t")
					buffer.WriteString(val)
					buffer.WriteString("\n")
				}

				gene := cols[1]
				addItemtoIndex("GENE", gene)
				gn, ok := transform[gene]
				if ok && gn != "" {
					addItemtoIndex("GRIF", gn)
					addItemtoIndex("PREF", gn)
					addItemtoIndex("GENE", gn)
				}
				sn, ok := synonyms[gene]
				if ok && sn != "" {
					syns := strings.Split(sn, "|")
					for _, syn := range syns {
						addItemtoIndex("GSYN", syn)
						addItemtoIndex("GENE", syn)
					}
				}
			}
		}

		if okay {
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

	// GENEINFO INDEXER

	// create intermediate XML object for several gene_info.gz fields (undocumented)
	if args[0] == "-geneinfo" {

		var buffer strings.Builder
		count := 0
		okay := false

		wrtr := bufio.NewWriter(os.Stdout)

		scanr := bufio.NewScanner(in)

		// skip first line with column heading names
		for scanr.Scan() {

			line := scanr.Text()
			cols := strings.Split(line, "\t")
			if len(cols) != 16 {
				fmt.Fprintf(os.Stderr, "Unexpected number of columns (%d) in gene_info.gz\n", len(cols))
				os.Exit(1)
			}
			if len(cols) != 16 || cols[0] != "#tax_id" {
				fmt.Fprintf(os.Stderr, "Unrecognized contents in gene_info.gz\n")
				os.Exit(1)
			}
			break
		}

		buffer.WriteString("<Set>\n")

		// read lines of gene information
		for scanr.Scan() {

			line := scanr.Text()

			cols := strings.Split(line, "\t")
			if len(cols) != 16 {
				continue
			}

			gene := cols[2]
			// skip NEWLINE entries
			if gene == "NEWENTRY" {
				continue
			}

			id := cols[1]
			ltag := cols[3]
			syns := cols[4]
			desc := cols[8]
			auth := cols[10]

			buffer.WriteString("  <Rec>\n")

			buffer.WriteString("    <Id>" + id + "</Id>\n")
			buffer.WriteString("    <Gene>" + html.EscapeString(gene) + "</Gene>\n")

			if ltag != "-" {
				buffer.WriteString("    <Ltag>" + html.EscapeString(ltag) + "</Ltag>\n")
			}
			if syns != "-" {
				buffer.WriteString("    <Syns>" + html.EscapeString(syns) + "</Syns>\n")
			}
			if desc != "-" {
				buffer.WriteString("    <Desc>" + html.EscapeString(desc) + "</Desc>\n")
			}
			if auth != "-" {
				buffer.WriteString("    <Auth>" + html.EscapeString(auth) + "</Auth>\n")
			}

			buffer.WriteString("  </Rec>\n")

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

		buffer.WriteString("</Set>\n")

		if okay {
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

	// NIH OPEN CITATION COLLECTION INDEXER

	// create intermediate XML object for open_citation_collection.csv fields (undocumented)
	if args[0] == "-nihocc" {

		var buffer strings.Builder
		count := 0
		okay := false

		wrtr := bufio.NewWriter(os.Stdout)

		scanr := bufio.NewScanner(in)

		// skip first line with column heading names
		for scanr.Scan() {

			line := scanr.Text()
			if line != "citing,referenced" {
				fmt.Fprintf(os.Stderr, "Unrecognized header '%s' in open_citation_collection.csv\n", line)
				os.Exit(1)
			}
			break
		}

		// read lines of PMID link information
		for scanr.Scan() {

			line := scanr.Text()

			cols := strings.Split(line, ",")
			if len(cols) != 2 {
				continue
			}

			fst := cols[0]
			scd := cols[1]

			if scd == "0" {
				continue
			}

			buffer.WriteString(fst)
			buffer.WriteString("\t")
			buffer.WriteString("CITED")
			buffer.WriteString("\t")
			buffer.WriteString(PadNumericID(scd))
			buffer.WriteString("\n")

			buffer.WriteString(scd)
			buffer.WriteString("\t")
			buffer.WriteString("CITES")
			buffer.WriteString("\t")
			buffer.WriteString(PadNumericID(fst))
			buffer.WriteString("\n")

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

	// THEME INDEXER

	/*
	   A+    Agonism, activation                      N     Inhibits
	   A-    Antagonism, blocking                     O     Transport, channels
	   B     Binding, ligand                          Pa    Alleviates, reduces
	   C     Inhibits cell growth                     Pr    Prevents, suppresses
	   D     Drug targets                             Q     Production by cell population
	   E     Affects expression/production            Rg    Regulation
	   E+    Increases expression/production          Sa    Side effect/adverse event
	   E-    Decreases expression/production          T     Treatment/therapy
	   G     Promotes progression                     Te    Possible therapeutic effect
	   H     Same protein or complex                  U     Causal mutations
	   I     Signaling pathway                        Ud    Mutations affecting disease course
	   J     Role in disease pathogenesis             V+    Activates, stimulates
	   K     Metabolism, pharmacokinetics             W     Enhances response
	   L     Improper regulation linked to disease    X     Overexpression in disease
	   Md    Biomarkers (diagnostic)                  Y     Polymorphisms alter risk
	   Mp    Biomarkers (progression)                 Z     Enzyme activity
	*/

	// create intermediate table for chemical-gene-disease themes (undocumented)
	if args[0] == "-theme" || args[0] == "-themes" {

		if len(args) < 4 {
			fmt.Fprintf(os.Stderr, "\nERROR: Insufficient arguments for -theme\n")
			os.Exit(1)
		}

		one := args[1]
		two := args[2]
		tag := args[3]

		// for disambiguating B, E, E+, and J themes, in CHDI, CHGE, GEDI, and GEGE data sets
		sfx := ""
		if len(tag) > 0 {
			switch tag[0] {
			case 'C':
				sfx = "c"
			case 'G':
				sfx = "g"
			}
		}

		fl, err := os.Open(one)
		if err != nil {
			fmt.Fprintf(os.Stderr, "\nERROR: Unable to open input file '%s'\n", one)
			os.Exit(1)
		}

		scanr := bufio.NewScanner(fl)

		var columns []string
		numCols := 0

		// read first line with column heading names
		if scanr.Scan() {

			line := scanr.Text()
			line = strings.Replace(line, "+", "p", -1)
			line = strings.Replace(line, "-", "m", -1)
			columns = strings.Split(line, "\t")
			numCols = len(columns)

			if numCols < 3 {
				fmt.Fprintf(os.Stderr, "Unexpected number of columns (%d) in part-i file\n", numCols)
				os.Exit(1)
			}
			if columns[0] != "path" {
				fmt.Fprintf(os.Stderr, "Unrecognized contents in part-i file\n")
				os.Exit(1)
			}
		}

		var scores []int

		for i := 0; i < numCols; i++ {
			scores = append(scores, 0)
		}

		mapper := make(map[string]string)

		scorer := make(map[string]int)

		// read lines of dependency paths, scores for each theme
		for scanr.Scan() {

			line := scanr.Text()

			cols := strings.Split(line, "\t")
			if len(cols) != numCols {
				fmt.Fprintf(os.Stderr, "Mismatched -theme columns in '%s'\n", line)
				continue
			}

			for i := 0; i < numCols; i++ {
				scores[i] = 0
			}

			sum := 0
			// increment by 2 to ignore flagship indicator fields
			for i := 1; i < numCols; i += 2 {
				str := cols[i]
				str, _ = SplitInTwoLeft(str, ".")
				val, err := strconv.Atoi(str)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Unrecognized number '%s'\n", str)
					continue
				}
				scores[i] = val
				sum += val
			}
			if sum == 0 {
				continue
			}

			path := cols[0]
			path = strings.ToLower(path)
			themes := ""
			comma := ""
			for i := 1; i < numCols; i += 2 {
				// find scores over cutoff
				if scores[i]*3 > sum {
					theme := columns[i]
					themes += comma
					themes += theme
					comma = ","
					scorer[path+"_"+theme] = scores[i] * 100 / sum
				}
			}
			if themes == "" {
				continue
			}
			// populate theme lookup table
			mapper[path] = themes
		}

		fl.Close()

		fl, err = os.Open(two)
		if err != nil {
			fmt.Fprintf(os.Stderr, "\nERROR: Unable to open input file '%s'\n", two)
			os.Exit(1)
		}

		var buffer strings.Builder
		count := 0
		okay := false

		wrtr := bufio.NewWriter(os.Stdout)

		scanr = bufio.NewScanner(fl)

		// read lines of PMIDs and dependency paths
		for scanr.Scan() {

			line := scanr.Text()

			cols := strings.Split(line, "\t")
			if len(cols) != 14 {
				fmt.Fprintf(os.Stderr, "Mismatched -theme columns in '%s'\n", line)
				continue
			}

			pmid := cols[0]
			path := cols[12]
			path = strings.ToLower(path)
			themes, ok := mapper[path]
			if !ok {
				continue
			}

			fwd := tag[0:2] + tag[2:4]
			rev := tag[2:4] + tag[0:2]

			cleanCol := func(str string) string {
				if str == "" || str == "null" {
					return ""
				}
				// remove database prefixes, tag OMIM and CHEBI identifiers with M and H prefixes
				if strings.HasPrefix(str, "MESH:") {
					str = strings.TrimPrefix(str, "MESH:")
				} else if strings.HasPrefix(str, "OMIM:") {
					str = "M" + strings.TrimPrefix(str, "OMIM:")
				} else if strings.HasPrefix(str, "CHEBI:") {
					str = "H" + strings.TrimPrefix(str, "CHEBI:")
				}
				idx := strings.Index(str, "(")
				if idx > 0 {
					// remove parenthetical Tax suffix
					str = str[:idx]
				}
				str = strings.ToLower(str)
				return str
			}

			splitCol := func(str string) []string {
				// multiple genes may be separated by semicolons
				if strings.Index(str, ";") >= 0 {
					return strings.Split(str, ";")
				}
				// mesh, omim, and chebi may be separated by vertical bars
				return strings.Split(str, "|")
			}

			frst := splitCol(cols[8])
			scnd := splitCol(cols[9])

			printItem := func(pmid, fld, item string) {
				if pmid == "" || fld == "" || item == "" {
					return
				}
				buffer.WriteString(pmid)
				buffer.WriteString("\t")
				buffer.WriteString(fld)
				buffer.WriteString("\t")
				buffer.WriteString(item)
				buffer.WriteString("\n")
			}

			thms := strings.Split(themes, ",")
			for _, theme := range thms {
				if theme == "" {
					continue
				}

				printItem(pmid, "THME", theme)

				// disambiguate B, E, E+, and J themes that appear in two data sets
				switch theme {
				case "B", "E", "J":
					printItem(pmid, "THME", theme+sfx)
				case "Ep":
					printItem(pmid, "THME", "E"+sfx+"p")
				case "Em":
					printItem(pmid, "THME", "E"+sfx+"m")
				}
			}

			for _, frs := range frst {
				fst := cleanCol(frs)
				if fst == "" {
					continue
				}

				for _, snd := range scnd {
					scd := cleanCol(snd)
					if scd == "" {
						continue
					}

					printItem(pmid, "CONV", fwd)

					printItem(pmid, "CONV", rev)

					printItem(pmid, "CONV", fst+" "+scd)

					printItem(pmid, "CONV", scd+" "+fst)

					printItem(pmid, "CONV", fwd+" "+fst+" "+scd)

					printItem(pmid, "CONV", rev+" "+scd+" "+fst)

					for _, theme := range thms {
						if theme == "" {
							continue
						}

						score := scorer[path+"_"+theme]
						pct := strconv.Itoa(score)

						printItem(pmid, "CONV", theme+" "+fwd)

						printItem(pmid, "CONV", theme+" "+rev)

						printItem(pmid, "CONV", theme+" "+fst+" "+scd+" "+pct)

						printItem(pmid, "CONV", theme+" "+scd+" "+fst+" "+pct)

						printItem(pmid, "CONV", theme+" "+fwd+" "+fst+" "+scd+" "+pct)

						printItem(pmid, "CONV", theme+" "+rev+" "+scd+" "+fst+" "+pct)
					}
				}
			}

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
			txt := buffer.String()
			if txt != "" {
				// print current buffer
				wrtr.WriteString(txt[:])
			}
		}
		buffer.Reset()

		wrtr.Flush()

		fl.Close()

		return recordCount
	}

	// DEPENDENCY PATH INDEXER

	// create intermediate table for chemical-gene-disease dependency paths (undocumented)
	if args[0] == "-dpath" || args[0] == "-dpaths" {

		var buffer strings.Builder
		count := 0
		okay := false

		replr := strings.NewReplacer(
			">", "_gtrthan_",
			"<", "_lssthan_",
			"/", "_slash_",
			"%", "_prcnt_",
			":", "_colln_",
			"+", "_pluss_",
			"!", "_exclam_",
			"?", "_qmark_",
			"'", "_squot_",
			"(", "_lparen_",
			")", "_rparen_",
		)
		if replr == nil {
			fmt.Fprintf(os.Stderr, "Unable to create replacer\n")
			os.Exit(1)
		}

		wrtr := bufio.NewWriter(os.Stdout)

		scanr := bufio.NewScanner(in)

		// read lines of PMIDs and dependency paths
		for scanr.Scan() {

			line := scanr.Text()

			cols := strings.Split(line, "\t")
			if len(cols) != 14 {
				fmt.Fprintf(os.Stderr, "Mismatched -dpath columns in '%s'\n", line)
				continue
			}

			pmid := cols[0]
			path := cols[12]
			path = strings.ToLower(path)

			// rescue known characters
			tmp := CompressRunsOfSpaces(path)
			tmp = strings.TrimSpace(tmp)

			tmp = " " + tmp + " "

			tmp = replr.Replace(tmp)

			tmp = CompressRunsOfSpaces(tmp)
			tmp = strings.TrimSpace(tmp)

			// final cleanup
			tmp = strings.Replace(tmp, "|", "_", -1)
			tmp = strings.Replace(tmp, "__", "_", -1)

			pths := strings.Split(tmp, " ")
			for _, pth := range pths {
				if pth == "" {
					continue
				}
				buffer.WriteString(pmid)
				buffer.WriteString("\t")
				buffer.WriteString("PATH")
				buffer.WriteString("\t")
				buffer.WriteString(pth)
				buffer.WriteString("\n")
			}

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

	// THESIS INDEXER

	// create .e2x file for bioconcepts, geneRIFs, and themes and their dependency paths (undocumented)
	if args[0] == "-thesis" {

		// e.g., -thesis 250000 "$target" "biocchem"
		if len(args) < 4 {
			fmt.Fprintf(os.Stderr, "\nERROR: Insufficient arguments for -thesis\n")
			os.Exit(1)
		}

		chunk, err := strconv.Atoi(args[1])
		if err != nil {
			fmt.Fprintf(os.Stderr, "Unrecognized count - '%s'\n", err.Error())
			os.Exit(1)
		}
		target := strings.TrimSuffix(args[2], "/")
		prefix := args[3]

		suffix := "e2x"
		sfx := suffix
		if zipp {
			sfx += ".gz"
		}

		fnum := 0

		scanr := bufio.NewScanner(os.Stdin)

		processChunk := func() bool {

			// map for combined index
			indexed := make(map[string][]string)

			writeChunk := func() {

				var (
					fl   *os.File
					wrtr *bufio.Writer
					zpr  *pgzip.Writer
					err  error
				)

				fnum++
				fpath := fmt.Sprintf("%s/%s%03d.%s", target, prefix, fnum, sfx)
				fl, err = os.Create(fpath)
				if err != nil {
					fmt.Fprintf(os.Stderr, "%s\n", err.Error())
					return
				}
				defer fl.Close()

				pth := fmt.Sprintf("%s%03d.%s", prefix, fnum, suffix)
				os.Stderr.WriteString(pth + "\n")

				var out io.Writer

				out = fl

				if zipp {

					zpr, err = pgzip.NewWriterLevel(fl, pgzip.BestSpeed)
					if err != nil {
						fmt.Fprintf(os.Stderr, "%s\n", err.Error())
						return
					}

					out = zpr
				}

				wrtr = bufio.NewWriter(out)
				if wrtr == nil {
					fmt.Fprintf(os.Stderr, "Unable to create bufio.NewWriter\n")
					return
				}

				var buffer strings.Builder
				count := 0

				buffer.WriteString("<IdxDocumentSet>\n")

				// sort fields in alphabetical order
				var keys []string
				for ky := range indexed {
					keys = append(keys, ky)
				}

				if len(keys) > 1 {
					sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
				}

				for _, idx := range keys {

					item, ok := indexed[idx]
					if !ok {
						continue
					}

					uid := item[0]
					data := item[1:]

					if uid == "" || len(data) < 1 {
						continue
					}

					// do not sort data now that it has field and value pairs

					buffer.WriteString("  <IdxDocument>\n")
					buffer.WriteString("    <IdxUid>")
					buffer.WriteString(uid)
					buffer.WriteString("</IdxUid>\n")
					buffer.WriteString("    <IdxSearchFields>\n")

					prevf := ""
					prevv := ""
					for len(data) > 0 {
						fld := data[0]
						val := data[1]
						data = data[2:]

						if fld == prevf && val == prevv {
							continue
						}

						buffer.WriteString("      <")
						buffer.WriteString(fld)
						buffer.WriteString(">")
						buffer.WriteString(val)
						buffer.WriteString("</")
						buffer.WriteString(fld)
						buffer.WriteString(">\n")

						prevf = fld
						prevv = val
					}

					buffer.WriteString("    </IdxSearchFields>\n")
					buffer.WriteString("  </IdxDocument>\n")

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
				}

				buffer.WriteString("</IdxDocumentSet>\n")

				txt := buffer.String()
				if txt != "" {
					// print current buffer
					wrtr.WriteString(txt[:])
				}
				buffer.Reset()

				wrtr.Flush()

				if zpr != nil {
					zpr.Close()
				}
			}

			lineCount := 0
			okay := false

			// read lines of dependency paths, scores for each theme
			for scanr.Scan() {

				line := scanr.Text()

				cols := strings.Split(line, "\t")
				if len(cols) != 3 {
					fmt.Fprintf(os.Stderr, "Mismatched -thesis columns in '%s'\n", line)
					continue
				}

				uid := cols[0]
				fd := cols[1]
				val := cols[2]
				if uid == "" || fd == "" || val == "" {
					continue
				}

				val = strings.ToLower(val)
				// convert angle brackets in chemical names
				val = html.EscapeString(val)

				data, ok := indexed[uid]
				if !ok {
					data = make([]string, 0, 3)
					// first entry on new slice is uid
					data = append(data, uid)
				}
				data = append(data, fd)
				data = append(data, val)
				// always need to update indexed, since data may be reallocated
				indexed[uid] = data

				okay = true

				lineCount++
				if lineCount > chunk {
					break
				}
			}

			if okay {
				writeChunk()
				return true
			}

			return false
		}

		for processChunk() {
			// loop until scanner runs out of lines
		}

		return recordCount
	}

	return 0
}
