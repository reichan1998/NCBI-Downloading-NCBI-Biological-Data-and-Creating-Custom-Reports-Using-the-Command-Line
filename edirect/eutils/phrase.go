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
// File Name:  phrase.go
//
// Author:  Jonathan Kans
//
// ==========================================================================

package eutils

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/surgebase/porter2"
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
	"unicode"
)

type alias struct {
	table    map[string]string
	lock     sync.Mutex
	fpath    string
	isLoaded bool
}

// loadAliasTable should be called within a lock on the alias.lock mutex
func (a *alias) loadAliasTable(reverse, commas bool) {

	if a == nil || a.fpath == "" {
		return
	}

	if a.isLoaded {
		return
	}

	file, ferr := os.Open(a.fpath)

	if file != nil && ferr == nil {
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			str := scanner.Text()
			if str == "" {
				continue
			}
			cols := strings.SplitN(str, "\t", 2)
			if len(cols) != 2 {
				continue
			}

			cleanTerm := func(str string) string {
				str = CleanupQuery(str, false, true)
				parts := strings.FieldsFunc(str, func(c rune) bool {
					return (!unicode.IsLetter(c) && !unicode.IsDigit(c) && c != ',') || c > 127
				})
				str = strings.Join(parts, " ")
				if commas {
					str = strings.Replace(str, ",", " ", -1)
				}
				str = strings.ToLower(str)
				str = strings.TrimSpace(str)
				str = CompressRunsOfSpaces(str)
				return str
			}

			one := cleanTerm(cols[0])
			two := cleanTerm(cols[1])
			if reverse {
				// fmt.Fprintf(os.Stderr, "R '%s' -> '%s'\n", two, one)
				a.table[two] = one
			} else {
				// fmt.Fprintf(os.Stderr, "F '%s' -> '%s'\n", one, two)
				a.table[one] = two
			}
		}
	}

	file.Close()

	// set even if loading failed to prevent multiple attempts
	a.isLoaded = true
}

var journalAliases = map[string]string{
	"pnas":                  "proc natl acad sci u s a",
	"journal of immunology": "journal of immunology baltimore md 1950",
	"biorxiv":               "biorxiv the preprint server for biology",
	"biorxivorg":            "biorxiv the preprint server for biology",
}

var ptypAliases = map[string]string{
	"clinical trial phase 1":     "clinical trial phase i",
	"clinical trial phase 2":     "clinical trial phase ii",
	"clinical trial phase 3":     "clinical trial phase iii",
	"clinical trial phase 4":     "clinical trial phase iv",
	"clinical trial phase one":   "clinical trial phase i",
	"clinical trial phase two":   "clinical trial phase ii",
	"clinical trial phase three": "clinical trial phase iii",
	"clinical trial phase four":  "clinical trial phase iv",
}

var (
	meshName alias
	meshTree alias
)

func printTermCount(base, term, field string) int {

	data, _ := getPostingIDs(base, term, field, true, false)
	size := len(data)
	fmt.Fprintf(os.Stdout, "%d\t%s\n", size, term)

	return size
}

func printTermCounts(base, term, field string) int {

	pdlen := len(PostingDir(term))

	if len(term) < pdlen {
		fmt.Fprintf(os.Stderr, "\nERROR: Term count argument must be at least %d characters\n", pdlen)
		os.Exit(1)
	}

	if strings.Contains(term[:pdlen], "*") {
		fmt.Fprintf(os.Stderr, "\nERROR: Wildcard asterisk must not be in first %d characters\n", pdlen)
		os.Exit(1)
	}

	dpath, key := PostingPath(base, field, term, false)
	if dpath == "" {
		return 0
	}

	// schedule asynchronous fetching
	mi := readMasterIndexFuture(dpath, key, field)

	tl := readTermListFuture(dpath, key, field)

	// fetch master index and term list
	indx := <-mi

	trms := <-tl

	if indx == nil || len(indx) < 1 {
		return 0
	}

	if trms == nil || len(trms) < 1 {
		return 0
	}

	// master index is padded with phantom term and postings position
	numTerms := len(indx) - 1

	strs := make([]string, numTerms)
	if strs == nil || len(strs) < 1 {
		return 0
	}

	retlength := int32(len("\n"))

	// populate array of strings from term list
	for i, j := 0, 1; i < numTerms; i++ {
		from := indx[i].TermOffset
		to := indx[j].TermOffset - retlength
		j++
		txt := string(trms[from:to])
		strs[i] = txt
	}

	// change protecting underscore to space
	term = strings.Replace(term, "_", " ", -1)

	// flank pattern with start-of-string and end-of-string symbols
	pat := "^" + term + "$"

	// change asterisk in query to dot + star for regular expression
	pat = strings.Replace(pat, "*", ".*", -1)

	re, err := regexp.Compile(pat)

	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		return 0
	}

	count := 0

	for R, str := range strs {
		if re.MatchString(str) {
			offset := indx[R].PostOffset
			size := indx[R+1].PostOffset - offset
			fmt.Fprintf(os.Stdout, "%d\t%s\n", size/4, str)
			count++
		}
	}

	return count
}

func printTermPositions(base, term, field string) int {

	data, ofst := getPostingIDs(base, term, field, false, false)
	size := len(data)
	fmt.Fprintf(os.Stdout, "\n%d\t%s\n\n", size, term)

	for i := 0; i < len(data); i++ {
		fmt.Fprintf(os.Stdout, "%12d\t", data[i])
		pos := ofst[i]
		sep := ""
		for j := 0; j < len(pos); j++ {
			fmt.Fprintf(os.Stdout, "%s%d", sep, pos[j])
			sep = ","
		}
		fmt.Fprintf(os.Stdout, "\n")
	}

	return size
}

// QUERY EVALUATION FUNCTION

func evaluateQuery(base, dbase, phrase string, clauses []string, noStdout, isLink bool) (int, []int32) {

	if clauses == nil || clauses[0] == "" {
		return 0, nil
	}

	count := 0

	// flag set if no tildes, indicates no proximity tests in query
	noProx := true
	for _, tkn := range clauses {
		if strings.HasPrefix(tkn, "~") {
			noProx = false
		}
	}

	phrasePositions := func(pn, pm []int16, dlt int16) []int16 {

		var arry []int16

		ln, lm := len(pn), len(pm)

		q, r := 0, 0

		vn, vm := pn[q], pm[r]
		vnd := vn + dlt

		for {
			if vnd > vm {
				r++
				if r == lm {
					break
				}
				vm = pm[r]
			} else if vnd < vm {
				q++
				if q == ln {
					break
				}
				vn = pn[q]
				vnd = vn + dlt
			} else {
				// store position of first word in current growing phrase
				arry = append(arry, vn)
				q++
				r++
				if q == ln || r == lm {
					break
				}
				vn = pn[q]
				vm = pm[r]
				vnd = vn + dlt
			}
		}

		return arry
	}

	proximityPositions := func(pn, pm []int16, dlt int16) []int16 {

		var arry []int16

		ln, lm := len(pn), len(pm)

		q, r := 0, 0

		vn, vm := pn[q], pm[r]
		vnd := vn + dlt

		for {
			if vnd < vm {
				q++
				if q == ln {
					break
				}
				vn = pn[q]
				vnd = vn + dlt
			} else if vn < vm {
				// store position of first word in downstream phrase that passes proximity test
				arry = append(arry, vm)
				q++
				r++
				if q == ln || r == lm {
					break
				}
				vn = pn[q]
				vm = pm[r]
				vnd = vn + dlt
			} else {
				r++
				if r == lm {
					break
				}
				vm = pm[r]
			}
		}

		return arry
	}

	eval := func(str string) ([]int32, [][]int16, int) {

		// extract optional [FIELD] qualifier
		field := "TIAB"
		if dbase == "pmc" {
			field = "TEXT"
		}

		if strings.HasSuffix(str, "]") {
			pos := strings.Index(str, "[")
			if pos >= 0 {
				field = str[pos:]
				field = strings.TrimPrefix(field, "[")
				field = strings.TrimSuffix(field, "]")
				str = str[:pos]
				str = strings.TrimSpace(str)
			}
			switch field {
			case "NORM":
				field = "TIAB"
			case "STEM", "TIAB", "TITL", "ABST", "TEXT":
			case "PIPE":
				// esearch -db pubmed -query "complement system proteins [MESH]" -pub clinical |
				// efetch -format uid | phrase-search -query "[PIPE] AND L [THME]"
				var data []int32
				// read UIDs from stdin
				uidq := CreateUIDReader(os.Stdin)
				for ext := range uidq {

					val, err := strconv.Atoi(ext.Text)
					if err != nil {
						fmt.Fprintf(os.Stderr, "\nERROR: Unrecognized UID %s\n", ext.Text)
						os.Exit(1)
					}

					data = append(data, int32(val))
				}
				// sort UIDs before returning
				sort.Slice(data, func(i, j int) bool { return data[i] < data[j] })
				return data, nil, 0
			default:
				str = strings.Replace(str, " ", "_", -1)
			}
		}

		words := strings.Fields(str)

		if words == nil || len(words) < 1 {
			return nil, nil, 0
		}

		// if no tilde proximity tests, and not building up phrase from multiple words,
		// no need to use more expensive position tests when calculating intersection
		if noProx && len(words) == 1 {
			term := words[0]
			if strings.HasPrefix(term, "+") {
				return nil, nil, 0
			}
			term = strings.Replace(term, "_", " ", -1)
			data, _ := getPostingIDs(base, term, field, true, isLink)
			count++
			return data, nil, 1
		}

		dist := 0

		var intersect []Arrays

		var futures []<-chan Arrays

		// schedule asynchronous fetching
		for _, term := range words {

			term = strings.Replace(term, "_", " ", -1)

			if strings.HasPrefix(term, "+") {
				dist += strings.Count(term, "+")
				// run of stop words or explicit plus signs skip past one or more words in phrase
				continue
			}

			fetch := postingIDsFuture(base, term, field, dist, isLink)

			futures = append(futures, fetch)

			dist++
		}

		runtime.Gosched()

		for _, chn := range futures {

			// fetch postings data
			fut := <-chn

			if len(fut.Data) < 1 {
				// bail if word not present
				return nil, nil, 0
			}

			// append posting and positions
			intersect = append(intersect, fut)

			runtime.Gosched()
		}

		if len(intersect) < 1 {
			return nil, nil, 0
		}

		// start phrase with first word
		data, ofst, dist := intersect[0].Data, intersect[0].Ofst, intersect[0].Dist+1

		if len(intersect) == 1 {
			return data, ofst, dist
		}

		for i := 1; i < len(intersect); i++ {

			// add subsequent words, keep starting positions of phrases that contain all words in proper position
			data, ofst = extendPositionalIDs(data, ofst, intersect[i].Data, intersect[i].Ofst, intersect[i].Dist, phrasePositions)
			if len(data) < 1 {
				// bail if phrase not present
				return nil, nil, 0
			}
			dist = intersect[i].Dist + 1
		}

		count += len(intersect)

		// return UIDs and all positions of current phrase
		return data, ofst, dist
	}

	prevTkn := ""

	nextToken := func() string {

		if len(clauses) < 1 {
			return ""
		}

		// remove next token from slice
		tkn := clauses[0]
		clauses = clauses[1:]

		if tkn == "(" && prevTkn != "" && prevTkn != "&" && prevTkn != "|" && prevTkn != "!" {
			fmt.Fprintf(os.Stderr, "\nERROR: Tokens '%s' and '%s' should be separated by AND, OR, or NOT\n", prevTkn, tkn)
			os.Exit(1)
		}

		if prevTkn == ")" && tkn != "" && tkn != "&" && tkn != "|" && tkn != "!" && tkn != ")" {
			fmt.Fprintf(os.Stderr, "\nERROR: Tokens '%s' and '%s' should be separated by AND, OR, or NOT\n", prevTkn, tkn)
			os.Exit(1)
		}

		prevTkn = tkn

		return tkn
	}

	// recursive definitions
	var fact func() ([]int32, [][]int16, int, string)
	var prox func() ([]int32, string)
	var excl func() ([]int32, string)
	var term func() ([]int32, string)
	var expr func() ([]int32, string)

	fact = func() ([]int32, [][]int16, int, string) {

		var (
			data  []int32
			ofst  [][]int16
			delta int
			tkn   string
		)

		tkn = nextToken()

		if tkn == "(" {
			// recursively process expression in parentheses
			data, tkn = expr()
			if tkn == ")" {
				tkn = nextToken()
			} else {
				fmt.Fprintf(os.Stderr, "\nERROR: Expected ')' but received '%s'\n", tkn)
				os.Exit(1)
			}
		} else if tkn == ")" {
			fmt.Fprintf(os.Stderr, "\nERROR: Unexpected ')' token\n")
			os.Exit(1)
		} else if tkn == "&" || tkn == "|" || tkn == "!" {
			fmt.Fprintf(os.Stderr, "\nERROR: Unexpected operator '%s' in expression\n", tkn)
			os.Exit(1)
		} else if tkn == "" {
			fmt.Fprintf(os.Stderr, "\nERROR: Unexpected end of expression in '%s'\n", phrase)
			os.Exit(1)
		} else {
			// evaluate current phrase
			data, ofst, delta = eval(tkn)
			tkn = nextToken()
		}

		return data, ofst, delta, tkn
	}

	prox = func() ([]int32, string) {

		var (
			next []int32
			noff [][]int16
			ndlt int
		)

		data, ofst, delta, tkn := fact()
		if len(data) < 1 {
			return nil, tkn
		}

		for strings.HasPrefix(tkn, "~") {
			dist := strings.Count(tkn, "~")
			next, noff, ndlt, tkn = fact()
			if len(next) < 1 {
				return nil, tkn
			}
			// next phrase must be within specified distance after the previous phrase
			data, ofst = extendPositionalIDs(data, ofst, next, noff, delta+dist, proximityPositions)
			if len(data) < 1 {
				return nil, tkn
			}
			delta = ndlt
		}

		return data, tkn
	}

	excl = func() ([]int32, string) {

		var next []int32

		data, tkn := prox()
		for tkn == "!" {
			next, tkn = prox()
			data = excludeIDs(data, next)
		}

		return data, tkn
	}

	term = func() ([]int32, string) {

		var next []int32

		data, tkn := excl()
		for tkn == "&" {
			next, tkn = excl()
			data = intersectIDs(data, next)
		}

		return data, tkn
	}

	expr = func() ([]int32, string) {

		var next []int32

		data, tkn := term()
		for tkn == "|" {
			next, tkn = term()
			data = combineIDs(data, next)
		}

		return data, tkn
	}

	// enter recursive descent parser
	result, tkn := expr()

	if tkn != "" {
		fmt.Fprintf(os.Stderr, "\nERROR: Unexpected token '%s' at end of expression\n", tkn)
		os.Exit(1)
	}

	// sort final result
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })

	if noStdout {
		return count, result
	}

	// use buffers to speed up uid printing
	var buffer strings.Builder

	wrtr := bufio.NewWriter(os.Stdout)

	for _, pmid := range result {
		val := strconv.Itoa(int(pmid))
		buffer.WriteString(val[:])
		buffer.WriteString("\n")
	}

	txt := buffer.String()
	if txt != "" {
		// print buffer
		wrtr.WriteString(txt[:])
	}

	wrtr.Flush()

	runtime.Gosched()

	return count, nil
}

// QUERY PARSING FUNCTIONS

func prepareQuery(str string) string {

	if str == "" {
		return ""
	}

	if strings.HasPrefix(str, "[PIPE]") {
		str = "stdin " + str
	}

	str = html.UnescapeString(str)

	str = CleanupQuery(str, false, true)

	str = strings.Replace(str, "~ ~", "~~", -1)
	str = strings.Replace(str, "~ ~", "~~", -1)

	str = strings.TrimSpace(str)

	// temporarily flank with spaces to detect misplaced operators at ends
	str = " " + str + " "

	str = strings.Replace(str, " AND ", " & ", -1)
	str = strings.Replace(str, " OR ", " | ", -1)
	str = strings.Replace(str, " NOT ", " ! ", -1)

	str = strings.Replace(str, "(", " ( ", -1)
	str = strings.Replace(str, ")", " ) ", -1)
	str = strings.Replace(str, "&", " & ", -1)
	str = strings.Replace(str, "|", " | ", -1)
	str = strings.Replace(str, "!", " ! ", -1)

	// ensure that bracketed fields are flanked by spaces
	str = strings.Replace(str, "[", " [", -1)
	str = strings.Replace(str, "]", "] ", -1)

	// remove temporary flanking spaces
	str = strings.TrimSpace(str)

	str = strings.ToLower(str)

	str = strings.Replace(str, "_", " ", -1)

	hasPlusOrMinus := func(str string) bool {

		for _, ch := range str {
			if ch == '-' || ch == '+' {
				return true
			}
		}

		return false
	}

	fixThemeCases := func(str string) string {

		if !strings.Contains(str, "[thme]") && !strings.Contains(str, "[conv]") {
			return str
		}

		var arry []string

		terms := strings.Fields(str)

		for _, item := range terms {

			switch item {
			case "a+":
				arry = append(arry, "ap")
			case "e+":
				arry = append(arry, "ep")
			case "ec+":
				arry = append(arry, "ecp")
			case "eg+":
				arry = append(arry, "egp")
			case "v+":
				arry = append(arry, "vp")
			case "a-":
				arry = append(arry, "am")
			case "e-":
				arry = append(arry, "em")
			case "ec-":
				arry = append(arry, "ecm")
			default:
				arry = append(arry, item)
			}
		}

		// reconstruct string from transformed words
		str = strings.Join(arry, " ")

		return str
	}

	if hasPlusOrMinus(str) {
		str = fixThemeCases(str)
	}

	if HasHyphenOrApostrophe(str) {
		str = FixSpecialCases(str)
	}

	str = strings.Replace(str, "-", " ", -1)

	str = strings.Replace(str, "'", "", -1)

	// allow links like pubmed_cited and pubmed_cites
	str = strings.Replace(str, "[pubmed ", "[pubmed_", -1)

	// break terms at punctuation, and at non-ASCII characters, allowing brackets for field names,
	// along with Boolean control symbols, underscore for protected terms, asterisk to indicate
	// truncation wildcard, tilde for maximum proximity, and plus sign for exactly one wildcard word
	terms := strings.FieldsFunc(str, func(c rune) bool {
		return (!unicode.IsLetter(c) && !unicode.IsDigit(c) &&
			c != '_' && c != '*' && c != '~' && c != '+' &&
			c != '$' && c != '&' && c != '|' && c != '!' &&
			c != '(' && c != ')' && c != '[' && c != ']') || c > 127
	})

	// rejoin into processed sentence
	tmp := strings.Join(terms, " ")

	tmp = CompressRunsOfSpaces(tmp)
	tmp = strings.TrimSpace(tmp)

	return tmp
}

func prepareExact(str, sfx string, deStop bool) string {

	if str == "" {
		return ""
	}

	if str == "[Not Available]." || str == "Health." {
		return ""
	}

	str = CleanupQuery(str, true, true)

	str = strings.Replace(str, "(", " ", -1)
	str = strings.Replace(str, ")", " ", -1)

	str = strings.Replace(str, "_", " ", -1)

	if HasHyphenOrApostrophe(str) {
		str = FixSpecialCases(str)
	}

	str = strings.Replace(str, "-", " ", -1)

	// remove trailing punctuation from each word
	var arry []string

	terms := strings.Fields(str)
	for _, item := range terms {
		max := len(item)
		for max > 1 {
			ch := item[max-1]
			if ch != '.' && ch != ',' && ch != ':' && ch != ';' {
				break
			}
			// trim trailing period, comma, colon, and semicolon
			item = item[:max-1]
			// continue checking for runs of punctuation at end
			max--
		}
		if item == "" {
			continue
		}
		arry = append(arry, item)
	}

	// rejoin into string
	cleaned := strings.Join(arry, " ")

	// break clauses at punctuation other than space or underscore, and at non-ASCII characters
	clauses := strings.FieldsFunc(cleaned, func(c rune) bool {
		return (!unicode.IsLetter(c) && !unicode.IsDigit(c) && c != ' ' && c != '_') || c > 127
	})

	// space replaces plus sign to separate runs of unpunctuated words
	phrases := strings.Join(clauses, " ")

	var chain []string

	// break phrases into individual words
	words := strings.Fields(phrases)

	for _, item := range words {

		// skip at site of punctuation break
		if item == "+" {
			chain = append(chain, "+")
			continue
		}

		// skip terms that are all digits
		if IsAllDigitsOrPeriod(item) {
			chain = append(chain, "+")
			continue
		}

		// optional stop word removal
		if deStop && IsStopWord(item) {
			chain = append(chain, "+")
			continue
		}

		// index single normalized term
		chain = append(chain, item)
	}

	// rejoin into processed sentence
	tmp := strings.Join(chain, " ")

	tmp = strings.Replace(tmp, "+ +", "++", -1)
	tmp = strings.Replace(tmp, "+ +", "++", -1)

	tmp = CompressRunsOfSpaces(tmp)
	tmp = strings.TrimSpace(tmp)

	if tmp != "" && !strings.HasSuffix(tmp, "]") {
		tmp += " " + sfx
	}

	return tmp
}

func processStopWords(str string, deStop bool) string {

	if str == "" {
		return ""
	}

	var chain []string

	terms := strings.Fields(str)

	nextField := func(terms []string) (string, int) {

		for j, item := range terms {
			if strings.HasPrefix(item, "[") && strings.HasSuffix(item, "]") {
				return strings.ToUpper(item), j + 1
			}
		}

		return "", 0
	}

	// replace unwanted and stop words with plus sign
	for len(terms) > 0 {

		item := terms[0]
		terms = terms[1:]

		fld, j := nextField(terms)

		// with addition of TITL field, switch from TIAB to NORM
		if fld == "[NORM]" {
			fld = "[TIAB]"
		}

		stps := false
		rlxd := false
		if fld == "[TITL]" || fld == "[TIAB]" || fld == "[ABST]" || fld == "[TEXT]" {
			stps = true
		} else if fld == "[STEM]" {
			stps = true
			rlxd = true
		} else if fld == "" {
			stps = true
		}

		addOneTerm := func(itm string) {

			if stps {
				if IsAllDigitsOrPeriod(itm) {
					// skip terms that are all digits
					chain = append(chain, "+")
				} else if deStop && IsStopWord(itm) {
					// skip if stop word, breaking phrase chain
					chain = append(chain, "+")
				} else if rlxd {
					isWildCard := strings.HasSuffix(itm, "*")
					if isWildCard {
						// temporarily remove trailing asterisk
						itm = strings.TrimSuffix(itm, "*")
					}

					itm = porter2.Stem(itm)
					itm = strings.TrimSpace(itm)

					if isWildCard {
						// do wildcard search in stemmed term list
						itm += "*"
					}
					chain = append(chain, itm)
				} else {
					// record single unmodified term
					chain = append(chain, itm)
				}
			} else {
				// do not treat non-TIAB terms as stop words
				chain = append(chain, itm)
			}
		}

		if j == 0 {
			// index single normalized term
			addOneTerm(item)
			continue
		}

		for j > 0 {

			addOneTerm(item)

			j--
			item = terms[0]
			terms = terms[1:]
		}

		if fld != "" {
			chain = append(chain, fld)
		}
	}

	// rejoin into processed sentence
	tmp := strings.Join(chain, " ")

	tmp = strings.Replace(tmp, "+ +", "++", -1)
	tmp = strings.Replace(tmp, "+ +", "++", -1)

	tmp = strings.Replace(tmp, "~ +", "~+", -1)
	tmp = strings.Replace(tmp, "+ ~", "+~", -1)

	for strings.Contains(tmp, "~+") {
		tmp = strings.Replace(tmp, "~+", "~~", -1)
	}
	for strings.Contains(tmp, "+~") {
		tmp = strings.Replace(tmp, "+~", "~~", -1)
	}

	tmp = CompressRunsOfSpaces(tmp)
	tmp = strings.TrimSpace(tmp)

	return tmp
}

func partitionQuery(str string) []string {

	if str == "" {
		return nil
	}

	str = CompressRunsOfSpaces(str)
	str = strings.TrimSpace(str)

	str = " " + str + " "

	// flank all operators with caret
	str = strings.Replace(str, " ( ", " ^ ( ^ ", -1)
	str = strings.Replace(str, " ) ", " ^ ) ^ ", -1)
	str = strings.Replace(str, " & ", " ^ & ^ ", -1)
	str = strings.Replace(str, " | ", " ^ | ^ ", -1)
	str = strings.Replace(str, " ! ", " ^ ! ^ ", -1)
	str = strings.Replace(str, " ~", " ^ ~", -1)
	str = strings.Replace(str, "~ ", "~ ^ ", -1)

	str = CompressRunsOfSpaces(str)
	str = strings.TrimSpace(str)

	str = strings.Replace(str, "^ ^", "^", -1)

	if strings.HasPrefix(str, "^ ") {
		str = str[2:]
	}
	if strings.HasSuffix(str, " ^") {
		max := len(str)
		str = str[:max-2]
	}

	str = strings.Replace(str, "~ ^ +", "~+", -1)
	str = strings.Replace(str, "+ ^ ~", "+~", -1)

	str = strings.Replace(str, "~ +", "~+", -1)
	str = strings.Replace(str, "+ ~", "+~", -1)

	for strings.Contains(str, "~+") {
		str = strings.Replace(str, "~+", "~~", -1)
	}
	for strings.Contains(str, "+~") {
		str = strings.Replace(str, "+~", "~~", -1)
	}

	// split into non-broken phrase segments or operator symbols
	tmp := strings.Split(str, " ^ ")

	return tmp
}

func setFieldQualifiers(clauses []string, rlxd bool) []string {

	var res []string

	if clauses == nil {
		return nil
	}

	for _, str := range clauses {

		// pass control symbols unchanged
		if str == "(" || str == ")" || str == "&" || str == "|" || str == "!" || strings.HasPrefix(str, "~") {
			res = append(res, str)
			continue
		}

		// pass angle bracket content delimiters (for -phrase, -require, -exclude)
		if str == "<" || str == ">" {
			res = append(res, str)
			continue
		}

		if strings.HasSuffix(str, " [YEAR]") {

			slen := len(str)
			str = str[:slen-7]

			// regular 4-digit year
			if len(str) == 4 && IsAllDigitsOrPeriod(str) {
				res = append(res, str+" [YEAR]")
				continue
			}

			// check for year wildcard
			if len(str) == 4 && str[3] == '*' && IsAllDigitsOrPeriod(str[:3]) {

				fmt.Fprintf(os.Stderr, "\nERROR: Wildcards not supported for years - use ####:#### range instead\n")
				os.Exit(1)
			}

			// allow year month day to look for unexpected annotation
			if len(str) > 9 {
				res = append(res, str+" [YEAR]")
				continue
			}

			// check for year range
			if len(str) == 9 && str[4] == ' ' && IsAllDigitsOrPeriod(str[:4]) && IsAllDigitsOrPeriod(str[5:]) {
				start, err := strconv.Atoi(str[:4])
				if err != nil {
					fmt.Fprintf(os.Stderr, "\nERROR: Unable to recognize starting year '%s'\n", str[:4])
					os.Exit(1)
				}
				stop, err := strconv.Atoi(str[5:])
				if err != nil {
					fmt.Fprintf(os.Stderr, "\nERROR: Unable to recognize stopping year '%s'\n", str[5:])
					os.Exit(1)
				}
				if start > stop {
					continue
				}
				// expand year range into individual year-by-year queries
				pfx := "("
				sfx := ")"
				for start <= stop {
					res = append(res, pfx)
					pfx = "|"
					yr := strconv.Itoa(start)
					res = append(res, yr+" [year]")
					start++
				}
				res = append(res, sfx)
				continue
			}

			fmt.Fprintf(os.Stderr, "\nERROR: Unable to recognize year expression '%s'\n", str)
			os.Exit(1)

		} else if strings.HasSuffix(str, " [ANUM]") ||
			strings.HasSuffix(str, " [INUM]") ||
			strings.HasSuffix(str, " [FNUM]") ||
			strings.HasSuffix(str, " [TLEN]") ||
			strings.HasSuffix(str, " [TNUM]") {

			slen := len(str)
			bdy := str[:slen-7]
			fld := str[slen-7:]
			bdy = strings.TrimSpace(bdy)
			fld = strings.TrimSpace(fld)

			// look for remnant of colon separating two integers
			lft, rgt := SplitInTwoLeft(bdy, " ")
			lft = strings.TrimSpace(lft)
			rgt = strings.TrimSpace(rgt)

			if lft == "" && rgt == "" {
				fmt.Fprintf(os.Stderr, "\nERROR: Unable to recognize expression '%s'\n", str)
				os.Exit(1)
			}

			// regular integer
			if rgt == "" {
				// check for wildcard
				if strings.HasSuffix(lft, "*") {

					fmt.Fprintf(os.Stderr, "\nERROR: Wildcards not supported - use #:# range instead\n")
					os.Exit(1)
				}
				if IsAllDigits(lft) {
					res = append(res, str)
					continue
				}
				fmt.Fprintf(os.Stderr, "\nERROR: Field %s must be an integer\n", fld)
				os.Exit(1)
			}

			// check for integer range
			if !IsAllDigits(lft) || !IsAllDigits(rgt) {
				fmt.Fprintf(os.Stderr, "\nERROR: Unable to recognize expression '%s'\n", str)
				os.Exit(1)
			}

			start, err := strconv.Atoi(lft)
			if err != nil {
				fmt.Fprintf(os.Stderr, "\nERROR: Unable to recognize starting number '%s'\n", lft)
				os.Exit(1)
			}
			stop, err := strconv.Atoi(rgt)
			if err != nil {
				fmt.Fprintf(os.Stderr, "\nERROR: Unable to recognize ending number '%s'\n", rgt)
				os.Exit(1)
			}
			if start > stop {
				// put into proper order
				start, stop = stop, start
			}
			// expand range into individual number-by-number queries
			fld = strings.ToLower(fld)
			pfx := "("
			sfx := ")"
			for start <= stop {
				res = append(res, pfx)
				pfx = "|"
				yr := strconv.Itoa(start)
				res = append(res, yr+" "+fld)
				start++
			}
			res = append(res, sfx)
			continue

		} else if strings.HasSuffix(str, " [TREE]") {

			slen := len(str)
			str = str[:slen-7]

			// pad if top-level mesh tree wildcard uses four character trie
			if len(str) == 4 && str[3] == '*' {
				key := str[:2]
				num, ok := TrieLen[key]
				if ok && num > 3 {
					str = str[0:3] + " " + "*"
				}
			}

			str = strings.Replace(str, " ", ".", -1)
			tmp := str
			tmp = strings.TrimSuffix(tmp, "*")
			if len(tmp) > 2 && unicode.IsLower(rune(tmp[0])) && IsAllDigitsOrPeriod(tmp[1:]) {
				str = strings.Replace(str, ".", " ", -1)
				res = append(res, str+" [TREE]")
				continue
			}

			fmt.Fprintf(os.Stderr, "\nERROR: Unable to recognize mesh code expression '%s'\n", str)
			os.Exit(1)

		} else if strings.HasSuffix(str, " [JOUR]") {

			slen := len(str)
			str = str[:slen-7]

			// check hard-coded journal alias map (would be better to use map from Data/jourindx.txt)
			alias, ok := journalAliases[str]
			if ok {
				res = append(res, alias+" [JOUR]")
				continue
			}

			// no alias found, use as is
			res = append(res, str+" [JOUR]")
			continue

		} else if strings.HasSuffix(str, " [PTYP]") {

			slen := len(str)
			str = str[:slen-7]

			// convert clinical trial phase with arabic numeral or english word to roman numeral
			alias, ok := ptypAliases[str]
			if ok {
				res = append(res, alias+" [PTYP]")
				continue
			}

			// no alias found, use as is
			res = append(res, str+" [PTYP]")
			continue

		} else if strings.HasSuffix(str, " [MESH]") {

			slen := len(str)
			str = str[:slen-7]

			// load mesh tables within mutexes
			meshName.lock.Lock()
			if !meshName.isLoaded {
				meshName.loadAliasTable(true, true)
			}
			meshName.lock.Unlock()

			meshTree.lock.Lock()
			if !meshTree.isLoaded {
				meshTree.loadAliasTable(false, false)
			}
			meshTree.lock.Unlock()

			// check mesh alias tables
			if meshName.isLoaded && meshTree.isLoaded {
				code, ok := meshName.table[str]
				if ok {
					cluster, ok := meshTree.table[code]
					if ok {
						if strings.Index(cluster, ",") < 0 {
							res = append(res, cluster+"* [TREE]")
							continue
						}
						trees := strings.Split(cluster, ",")
						// expand multiple trees in OR group
						pfx := "("
						sfx := ")"
						for _, tr := range trees {
							res = append(res, pfx)
							pfx = "|"
							tr = strings.TrimSpace(tr)
							res = append(res, tr+"* [TREE]")
						}
						res = append(res, sfx)
						continue
					} else {
						res = append(res, code+" [CODE]")
						continue
					}
				}
			}

			// skip if MeSH term not yet indexed in tree
			continue
		}

		// remove leading and trailing plus signs and spaces
		for strings.HasPrefix(str, "+") || strings.HasPrefix(str, " ") {
			str = str[1:]
		}
		for strings.HasSuffix(str, "+") || strings.HasSuffix(str, " ") {
			slen := len(str)
			str = str[:slen-1]
		}

		res = append(res, str)
	}

	return res
}

// SEARCH TERM LISTS FOR PHRASES OR NORMALIZED TERMS, OR MATCH BY PATTERN

// ProcessSearch evaluates query, returns list of PMIDs to stdout
func ProcessSearch(base, dbase, phrase string, xact, titl, rlxd, isLink, deStop bool) int {

	if phrase == "" {
		return 0
	}

	if base == "" {
		// obtain path from environment variable within rchive as a convenience
		base = os.Getenv("EDIRECT_PUBMED_MASTER")
		if base != "" {
			if !strings.HasSuffix(base, "/") {
				base += "/"
			}
			base += "Postings"
		}
	}

	if titl {
		phrase = prepareExact(phrase, "[titl]", deStop)
	} else if xact {
		if dbase == "pmc" {
			phrase = prepareExact(phrase, "[text]", deStop)
		} else {
			phrase = prepareExact(phrase, "[tiab]", deStop)
		}
	} else {
		phrase = prepareQuery(phrase)
	}

	phrase = processStopWords(phrase, deStop)

	clauses := partitionQuery(phrase)

	clauses = setFieldQualifiers(clauses, rlxd)

	count, _ := evaluateQuery(base, dbase, phrase, clauses, false, isLink)

	return count
}

// ProcessQuery evaluates query, returns list of PMIDs in array
func ProcessQuery(base, dbase, phrase string, xact, titl, rlxd, isLink, deStop bool) []int32 {

	if phrase == "" {
		return nil
	}

	if base == "" {
		// obtain path from environment variable within rchive as a convenience
		base = os.Getenv("EDIRECT_PUBMED_MASTER")
		if base != "" {
			if !strings.HasSuffix(base, "/") {
				base += "/"
			}
			base += "Postings"
		}
	}

	if titl {
		phrase = prepareExact(phrase, "[titl]", deStop)
	} else if xact {
		if dbase == "pmc" {
			phrase = prepareExact(phrase, "[text]", deStop)
		} else {
			phrase = prepareExact(phrase, "[tiab]", deStop)
		}
	} else {
		phrase = prepareQuery(phrase)
	}

	phrase = processStopWords(phrase, deStop)

	clauses := partitionQuery(phrase)

	clauses = setFieldQualifiers(clauses, rlxd)

	_, arry := evaluateQuery(base, dbase, phrase, clauses, true, isLink)

	return arry
}

// ProcessMock shows individual steps in processing query for evaluation
func ProcessMock(base, dbase, phrase string, xact, titl, rlxd, deStop bool) int {

	if phrase == "" {
		return 0
	}

	fmt.Fprintf(os.Stdout, "processSearch:\n\n%s\n\n", phrase)

	if titl {
		phrase = prepareExact(phrase, "[titl]", deStop)

		fmt.Fprintf(os.Stdout, "prepareExact:\n\n%s\n\n", phrase)
	} else if xact {
		if dbase == "pmc" {
			phrase = prepareExact(phrase, "[text]", deStop)
		} else {
			phrase = prepareExact(phrase, "[tiab]", deStop)
		}

		fmt.Fprintf(os.Stdout, "prepareExact:\n\n%s\n\n", phrase)
	} else {
		phrase = prepareQuery(phrase)

		fmt.Fprintf(os.Stdout, "prepareQuery:\n\n%s\n\n", phrase)
	}

	phrase = processStopWords(phrase, deStop)

	fmt.Fprintf(os.Stdout, "processStopWords:\n\n%s\n\n", phrase)

	clauses := partitionQuery(phrase)

	fmt.Fprintf(os.Stdout, "partitionQuery:\n\n")
	for _, tkn := range clauses {
		fmt.Fprintf(os.Stdout, "%s\n", tkn)
	}
	fmt.Fprintf(os.Stdout, "\n")

	clauses = setFieldQualifiers(clauses, rlxd)

	fmt.Fprintf(os.Stdout, "setFieldQualifiers:\n\n")
	for _, tkn := range clauses {
		fmt.Fprintf(os.Stdout, "%s\n", tkn)
	}
	fmt.Fprintf(os.Stdout, "\n")

	return 0
}

// ProcessCount prints document count for each term, also supports terminal wildcards
func ProcessCount(base, dbase, phrase string, plrl, psns, rlxd, deStop bool) int {

	if phrase == "" {
		return 0
	}

	if base == "" {
		// obtain path from environment variable within rchive as a convenience
		base = os.Getenv("EDIRECT_PUBMED_MASTER")
		if base != "" {
			if !strings.HasSuffix(base, "/") {
				base += "/"
			}
			base += "Postings"
		}
	}

	phrase = prepareQuery(phrase)

	phrase = processStopWords(phrase, deStop)

	clauses := partitionQuery(phrase)

	clauses = setFieldQualifiers(clauses, rlxd)

	if clauses == nil {
		return 0
	}

	count := 0

	splitIntoWords := func(str string) []string {

		if str == "" {
			return nil
		}

		var arry []string

		parts := strings.Split(str, "+")

		for _, segment := range parts {

			segment = strings.TrimSpace(segment)

			if segment == "" {
				continue
			}

			words := strings.Fields(segment)

			for _, item := range words {
				if strings.HasPrefix(item, "~") {
					continue
				}
				arry = append(arry, item)
			}
		}

		return arry
	}

	parseField := func(str string) (string, string) {

		field := "TIAB"
		if dbase == "pmc" {
			field = "TEXT"
		}

		if strings.HasSuffix(str, "]") {
			pos := strings.Index(str, "[")
			if pos >= 0 {
				field = str[pos:]
				field = strings.TrimPrefix(field, "[")
				field = strings.TrimSuffix(field, "]")
				str = str[:pos]
				str = strings.TrimSpace(str)
			}
			switch field {
			case "NORM":
				field = "TIAB"
			case "STEM", "TIAB", "TITL", "ABST", "TEXT":
			case "PIPE":
			default:
				str = strings.Replace(str, " ", "_", -1)
			}
		}

		return field, str
	}

	checkTermCounts := func(txt string) {

		field, str := parseField(txt)

		var words []string

		words = splitIntoWords(str)

		if words == nil || len(words) < 1 {
			return
		}

		for _, term := range words {

			term = strings.Replace(term, "_", " ", -1)

			if psns {
				count += printTermPositions(base, term, field)
			} else if plrl {
				count += printTermCounts(base, term, field)
			} else {
				count += printTermCount(base, term, field)
			}
		}
	}

	for _, item := range clauses {

		// skip control symbols
		if item == "(" || item == ")" || item == "&" || item == "|" || item == "!" {
			continue
		}

		checkTermCounts(item)
	}

	runtime.Gosched()

	return count
}

// TermCounts prints document counts for terms by subdirectory
func TermCounts(dpath, key, field string) int {

	if dpath == "" {
		return 0
	}

	// schedule asynchronous fetching
	mi := readMasterIndexFuture(dpath, key, field)

	tl := readTermListFuture(dpath, key, field)

	// fetch master index and term list
	indx := <-mi

	trms := <-tl

	if indx == nil || len(indx) < 1 {
		return 0
	}

	if trms == nil || len(trms) < 1 {
		return 0
	}

	// master index is padded with phantom term and postings position
	numTerms := len(indx) - 1

	strs := make([]string, numTerms)
	if strs == nil || len(strs) < 1 {
		return 0
	}

	retlength := int32(len("\n"))

	// populate array of strings from term list
	for i, j := 0, 1; i < numTerms; i++ {
		from := indx[i].TermOffset
		to := indx[j].TermOffset - retlength
		j++
		txt := string(trms[from:to])
		strs[i] = txt
	}

	count := 0

	for R, str := range strs {
		offset := indx[R].PostOffset
		size := indx[R+1].PostOffset - offset
		fmt.Fprintf(os.Stdout, "%d\t%s\n", size/4, str)
		count++
	}

	return count
}

// ProcessLinks reads a list of PMIDs, merges resulting links
func ProcessLinks(base, fld string) {

	if fld == "" {
		return
	}

	if base == "" {
		// obtain path from environment variable as a convenience
		base = os.Getenv("EDIRECT_PUBMED_MASTER")
		if base != "" {
			if !strings.HasSuffix(base, "/") {
				base += "/"
			}
			base += "Postings"
		}
	}

	// createLinkGrouper reads from UID reader and groups PMIDs under the same LinksTrie
	createLinkGrouper := func(base, fld string, inp <-chan XMLRecord) <-chan []string {

		if base == "" || fld == "" || inp == nil {
			return nil
		}

		out := make(chan []string, ChanDepth())
		if out == nil {
			fmt.Fprintf(os.Stderr, "\nERROR: Unable to create link grouper channel\n")
			os.Exit(1)
		}

		linkGrouper := func(base, fld string, inp <-chan XMLRecord, out chan<- []string) {

			// report when grouper has no more records to process
			defer close(out)

			var arry []string

			currPfx := ""

			for ext := range inp {

				uid := ext.Text
				_, pfx := LinksTrie(uid, true)

				if pfx != currPfx && currPfx != "" {

					if arry != nil {
						// send group of PMIDs with the same line trie down the channel
						out <- arry
					}

					// empty the slice
					arry = nil
				}

				arry = append(arry, uid)

				currPfx = pfx
			}

			// send final results
			if arry != nil {
				// send group of PMIDs with the same line trie down the channel
				out <- arry
			}
		}

		// launch single link grouper goroutine
		go linkGrouper(base, fld, inp, out)

		return out
	}

	// mutex for link results
	var llock sync.RWMutex

	// map for combining link results
	combinedLinks := make(map[int]bool)

	createLinkMergers := func(prom, field string, inp <-chan []string) <-chan string {

		if prom == "" || field == "" || inp == nil {
			return nil
		}

		out := make(chan string, ChanDepth())
		if out == nil {
			fmt.Fprintf(os.Stderr, "\nERROR: Unable to create link merger channel\n")
			os.Exit(1)
		}

		// linkMerge processes a set of terms from the same master index area
		linkMerge := func(wg *sync.WaitGroup, prom, field string, inp <-chan []string, out chan<- string) {

			// report when this matcher has no more records to process
			defer wg.Done()

			if inp == nil || out == nil {
				return
			}

			for terms := range inp {

				key := terms[0]

				dir, ky := LinksTrie(key, true)
				if dir == "" {
					continue
				}
				dpath := filepath.Join(prom, field, dir)
				if dpath == "" {
					continue
				}

				// schedule asynchronous fetching
				mi := readMasterIndexFuture(dpath, ky, field)

				tl := readTermListFuture(dpath, ky, field)

				// fetch master index and term list
				indx := <-mi

				trms := <-tl

				if indx == nil || len(indx) < 1 {
					continue
				}

				if trms == nil || len(trms) < 1 {
					continue
				}

				// master index is padded with phantom term and postings position
				numTerms := len(indx) - 1

				strs := make([]string, numTerms)
				if strs == nil || len(strs) < 1 {
					continue
				}

				retlength := int32(len("\n"))

				// populate array of strings from term list
				for i, j := 0, 1; i < numTerms; i++ {
					from := indx[i].TermOffset
					to := indx[j].TermOffset - retlength
					j++
					txt := string(trms[from:to])
					strs[i] = txt
				}

				postingsLoop := func(dpath, ky, field string) {

					inFile, _ := commonOpenFile(dpath, ky+"."+field+".pst")
					if inFile == nil {
						return
					}

					defer inFile.Close()

					for _, term := range terms {

						term = PadNumericID(term)

						// binary search in term list
						L, R := 0, numTerms-1
						for L < R {
							mid := (L + R) / 2
							if strs[mid] < term {
								L = mid + 1
							} else {
								R = mid
							}
						}

						linkLoop := func(offset, size int32) {

							data := make([]int32, size/4)
							if data == nil || len(data) < 1 {
								return
							}

							_, err := inFile.Seek(int64(offset), io.SeekStart)
							if err != nil {
								fmt.Fprintf(os.Stderr, "%s\n", err.Error())
								return
							}

							// read relevant postings list section
							err = binary.Read(inFile, binary.LittleEndian, data)
							if err != nil {
								fmt.Fprintf(os.Stderr, "%s\n", err.Error())
								return
							}

							if data == nil || len(data) < 1 {
								return
							}

							llock.Lock()

							for _, uid := range data {
								combinedLinks[int(uid)] = true
							}

							llock.Unlock()
						}

						// regular search requires exact match from binary search
						if R < numTerms && strs[R] == term {

							offset := indx[R].PostOffset
							size := indx[R+1].PostOffset - offset

							linkLoop(offset, size)
						}
					}
				}

				postingsLoop(dpath, ky, field)

				out <- ky
			}
		}

		var wg sync.WaitGroup

		// launch multiple link merger goroutines
		for i := 0; i < NumServe(); i++ {
			wg.Add(1)
			go linkMerge(&wg, prom, field, inp, out)
		}

		// launch separate anonymous goroutine to wait until all mergers are done
		go func() {
			wg.Wait()
			close(out)
		}()

		return out
	}

	// read text PMIDs from stdin
	uidq := CreateUIDReader(os.Stdin)

	grpq := createLinkGrouper(base, fld, uidq)

	lnkq := createLinkMergers(base, fld, grpq)

	// drain channel
	for range lnkq {
	}

	// sort id keys in alphabetical order
	var keys []int
	for ky := range combinedLinks {
		keys = append(keys, ky)
	}

	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	// use buffers to speed up PMID printing
	var buffer strings.Builder

	wrtr := bufio.NewWriter(os.Stdout)

	for _, uid := range keys {
		pmid := strconv.Itoa(uid)
		buffer.WriteString(pmid)
		buffer.WriteString("\n")
	}

	txt := buffer.String()
	if txt != "" {
		// print buffer
		wrtr.WriteString(txt[:])
	}

	wrtr.Flush()

	runtime.Gosched()
}

// initialize empty journal and MeSH maps before non-init functions are called
func init() {

	meshName.table = make(map[string]string)
	meshTree.table = make(map[string]string)

	nv := os.Getenv("EDIRECT_PUBMED_MASTER")
	if nv != "" {
		if !strings.HasSuffix(nv, "/") {
			nv += "/"
		}
		meshName.fpath = filepath.Join(nv, "Data", "meshname.txt")
		meshTree.fpath = filepath.Join(nv, "Data", "meshtree.txt")
	}
}
