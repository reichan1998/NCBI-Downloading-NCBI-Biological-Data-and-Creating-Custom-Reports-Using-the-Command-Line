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
// File Name:  xplore.go
//
// Author:  Jonathan Kans
//
// ==========================================================================

package eutils

import (
	"encoding/base64"
	"fmt"
	"github.com/fatih/color"
	"github.com/surgebase/porter2"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"html"
	"math"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode"
)

// SUPPORT CODE FOR xtract PROGRAM FUNCTIONS

// TYPED CONSTANTS

// LevelType is the integer type for exploration arguments
type LevelType int

// LevelType keys for exploration arguments
const (
	_ LevelType = iota
	UNIT
	SUBSET
	SECTION
	BLOCK
	BRANCH
	GROUP
	DIVISION
	PATH
	PATTERN
)

// IndentType is the integer type for XML formatting
type IndentType int

// IndentType keys for XML formatting
const (
	SINGULARITY IndentType = iota
	COMPACT
	FLUSH
	INDENT
	SUBTREE
	WRAPPED
)

// OpType is the integer type for operations
type OpType int

// OpType keys for operations
const (
	UNSET OpType = iota
	ELEMENT
	FIRST
	LAST
	BACKWARD
	ENCODE
	DECODE
	UPPER
	LOWER
	CHAIN
	TITLE
	MIRROR
	ALNUM
	BASIC
	PLAIN
	SIMPLE
	AUTHOR
	PROSE
	ORDER
	YEAR
	MONTH
	DATE
	PAGE
	AUTH
	INITIALS
	JOUR
	PROP
	TRIM
	WCT
	DOI
	TRANSLATE
	REPLACE
	TERMS
	WORDS
	PAIRS
	PAIRX
	REVERSE
	LETTERS
	CLAUSES
	INDICES
	ARTICLE
	ABSTRACT
	PARAGRAPH
	STEMMED
	MESHCODE
	MATRIX
	CLASSIFY
	HISTOGRAM
	ACCENTED
	TEST
	SCAN
	PFX
	SFX
	SEP
	TAB
	RET
	LBL
	TAG
	ATT
	ATR
	CLS
	SLF
	END
	CLR
	PFC
	DEQ
	PLG
	ELG
	FWD
	AWD
	WRP
	ENC
	PKG
	RST
	DEF
	REG
	EXP
	COLOR
	POSITION
	SELECT
	IF
	UNLESS
	MATCH
	AVOID
	AND
	OR
	EQUALS
	CONTAINS
	INCLUDES
	ISWITHIN
	STARTSWITH
	ENDSWITH
	ISNOT
	ISBEFORE
	ISAFTER
	MATCHES
	RESEMBLES
	ISEQUALTO
	DIFFERSFROM
	GT
	GE
	LT
	LE
	EQ
	NE
	NUM
	LEN
	SUM
	ACC
	MIN
	MAX
	INC
	DEC
	SUB
	AVG
	DEV
	MED
	MUL
	DIV
	MOD
	LG2
	LGE
	LOG
	BIN
	OCT
	HEX
	BIT
	PAD
	RAW
	ZEROBASED
	ONEBASED
	UCSCBASED
	REVCOMP
	NUCLEIC
	FASTA
	NCBI2NA
	NCBI4NA
	MOLWT
	HGVS
	ELSE
	VARIABLE
	ACCUMULATOR
	VALUE
	QUESTION
	TILDE
	STAR
	DOT
	PRCNT
	DOLLAR
	ATSIGN
	COUNT
	LENGTH
	DEPTH
	INDEX
	UNRECOGNIZED
)

// ArgumentType is the integer type for argument classification
type ArgumentType int

// ArgumentType keys for argument classification
const (
	_ ArgumentType = iota
	EXPLORATION
	CONDITIONAL
	EXTRACTION
	CUSTOMIZATION
)

// RangeType is the integer type for element range choices
type RangeType int

// RangeType keys for element range choices
const (
	NORANGE RangeType = iota
	STRINGRANGE
	VARIABLERANGE
	INTEGERRANGE
)

// SeqEndType is used for -ucsc-based decisions
type SeqEndType int

// SeqEndType keys for -ucsc-based decisions
const (
	_ SeqEndType = iota
	ISSTART
	ISSTOP
	ISPOS
)

// SequenceType is used to record XML tag and position for -ucsc-based
type SequenceType struct {
	Based int
	Which SeqEndType
}

// MUTEXES

var hlock sync.Mutex

var slock sync.RWMutex

// ARGUMENT MAPS

var argTypeIs = map[string]ArgumentType{
	"-unit":         EXPLORATION,
	"-Unit":         EXPLORATION,
	"-subset":       EXPLORATION,
	"-Subset":       EXPLORATION,
	"-section":      EXPLORATION,
	"-Section":      EXPLORATION,
	"-block":        EXPLORATION,
	"-Block":        EXPLORATION,
	"-branch":       EXPLORATION,
	"-Branch":       EXPLORATION,
	"-group":        EXPLORATION,
	"-Group":        EXPLORATION,
	"-division":     EXPLORATION,
	"-Division":     EXPLORATION,
	"-path":         EXPLORATION,
	"-Path":         EXPLORATION,
	"-pattern":      EXPLORATION,
	"-Pattern":      EXPLORATION,
	"-position":     CONDITIONAL,
	"-select":       CONDITIONAL,
	"-if":           CONDITIONAL,
	"-unless":       CONDITIONAL,
	"-match":        CONDITIONAL,
	"-avoid":        CONDITIONAL,
	"-and":          CONDITIONAL,
	"-or":           CONDITIONAL,
	"-equals":       CONDITIONAL,
	"-contains":     CONDITIONAL,
	"-includes":     CONDITIONAL,
	"-is-within":    CONDITIONAL,
	"-starts-with":  CONDITIONAL,
	"-ends-with":    CONDITIONAL,
	"-is-not":       CONDITIONAL,
	"-is-before":    CONDITIONAL,
	"-is-after":     CONDITIONAL,
	"-matches":      CONDITIONAL,
	"-resembles":    CONDITIONAL,
	"-is-equal-to":  CONDITIONAL,
	"-differs-from": CONDITIONAL,
	"-gt":           CONDITIONAL,
	"-ge":           CONDITIONAL,
	"-lt":           CONDITIONAL,
	"-le":           CONDITIONAL,
	"-eq":           CONDITIONAL,
	"-ne":           CONDITIONAL,
	"-element":      EXTRACTION,
	"-first":        EXTRACTION,
	"-last":         EXTRACTION,
	"-backward":     EXTRACTION,
	"-encode":       EXTRACTION,
	"-decode":       EXTRACTION,
	"-decode64":     EXTRACTION,
	"-upper":        EXTRACTION,
	"-lower":        EXTRACTION,
	"-chain":        EXTRACTION,
	"-title":        EXTRACTION,
	"-mirror":       EXTRACTION,
	"-alnum":        EXTRACTION,
	"-basic":        EXTRACTION,
	"-plain":        EXTRACTION,
	"-simple":       EXTRACTION,
	"-author":       EXTRACTION,
	"-prose":        EXTRACTION,
	"-order":        EXTRACTION,
	"-year":         EXTRACTION,
	"-month":        EXTRACTION,
	"-date":         EXTRACTION,
	"-page":         EXTRACTION,
	"-auth":         EXTRACTION,
	"-initials":     EXTRACTION,
	"-jour":         EXTRACTION,
	"-prop":         EXTRACTION,
	"-trim":         EXTRACTION,
	"-wct":          EXTRACTION,
	"-doi":          EXTRACTION,
	"-translate":    EXTRACTION,
	"-replace":      EXTRACTION,
	"-terms":        EXTRACTION,
	"-words":        EXTRACTION,
	"-pairs":        EXTRACTION,
	"-pairx":        EXTRACTION,
	"-reverse":      EXTRACTION,
	"-letters":      EXTRACTION,
	"-clauses":      EXTRACTION,
	"-indices":      EXTRACTION,
	"-article":      EXTRACTION,
	"-abstract":     EXTRACTION,
	"-paragraph":    EXTRACTION,
	"-stemmed":      EXTRACTION,
	"-meshcode":     EXTRACTION,
	"-matrix":       EXTRACTION,
	"-classify":     EXTRACTION,
	"-histogram":    EXTRACTION,
	"-accented":     EXTRACTION,
	"-test":         EXTRACTION,
	"-scan":         EXTRACTION,
	"-num":          EXTRACTION,
	"-len":          EXTRACTION,
	"-sum":          EXTRACTION,
	"-acc":          EXTRACTION,
	"-min":          EXTRACTION,
	"-max":          EXTRACTION,
	"-inc":          EXTRACTION,
	"-dec":          EXTRACTION,
	"-sub":          EXTRACTION,
	"-avg":          EXTRACTION,
	"-dev":          EXTRACTION,
	"-med":          EXTRACTION,
	"-mul":          EXTRACTION,
	"-div":          EXTRACTION,
	"-mod":          EXTRACTION,
	"-lg2":          EXTRACTION,
	"-lge":          EXTRACTION,
	"-log":          EXTRACTION,
	"-bin":          EXTRACTION,
	"-oct":          EXTRACTION,
	"-hex":          EXTRACTION,
	"-bit":          EXTRACTION,
	"-pad":          EXTRACTION,
	"-raw":          EXTRACTION,
	"-0-based":      EXTRACTION,
	"-zero-based":   EXTRACTION,
	"-1-based":      EXTRACTION,
	"-one-based":    EXTRACTION,
	"-ucsc":         EXTRACTION,
	"-ucsc-based":   EXTRACTION,
	"-ucsc-coords":  EXTRACTION,
	"-bed-based":    EXTRACTION,
	"-bed-coords":   EXTRACTION,
	"-revcomp":      EXTRACTION,
	"-nucleic":      EXTRACTION,
	"-fasta":        EXTRACTION,
	"-ncbi2na":      EXTRACTION,
	"-ncbi4na":      EXTRACTION,
	"-molwt":        EXTRACTION,
	"-hgvs":         EXTRACTION,
	"-else":         EXTRACTION,
	"-pfx":          CUSTOMIZATION,
	"-sfx":          CUSTOMIZATION,
	"-sep":          CUSTOMIZATION,
	"-tab":          CUSTOMIZATION,
	"-ret":          CUSTOMIZATION,
	"-lbl":          CUSTOMIZATION,
	"-tag":          CUSTOMIZATION,
	"-att":          CUSTOMIZATION,
	"-atr":          CUSTOMIZATION,
	"-cls":          CUSTOMIZATION,
	"-slf":          CUSTOMIZATION,
	"-end":          CUSTOMIZATION,
	"-clr":          CUSTOMIZATION,
	"-pfc":          CUSTOMIZATION,
	"-deq":          CUSTOMIZATION,
	"-plg":          CUSTOMIZATION,
	"-elg":          CUSTOMIZATION,
	"-fwd":          CUSTOMIZATION,
	"-awd":          CUSTOMIZATION,
	"-wrp":          CUSTOMIZATION,
	"-enc":          CUSTOMIZATION,
	"-pkg":          CUSTOMIZATION,
	"-rst":          CUSTOMIZATION,
	"-def":          CUSTOMIZATION,
	"-reg":          CUSTOMIZATION,
	"-exp":          CUSTOMIZATION,
	"-color":        CUSTOMIZATION,
}

var opTypeIs = map[string]OpType{
	"-element":      ELEMENT,
	"-first":        FIRST,
	"-last":         LAST,
	"-backward":     BACKWARD,
	"-encode":       ENCODE,
	"-decode":       DECODE,
	"-decode64":     DECODE,
	"-upper":        UPPER,
	"-lower":        LOWER,
	"-chain":        CHAIN,
	"-title":        TITLE,
	"-mirror":       MIRROR,
	"-alnum":        ALNUM,
	"-basic":        BASIC,
	"-plain":        PLAIN,
	"-simple":       SIMPLE,
	"-author":       AUTHOR,
	"-prose":        PROSE,
	"-order":        ORDER,
	"-year":         YEAR,
	"-month":        MONTH,
	"-date":         DATE,
	"-page":         PAGE,
	"-auth":         AUTH,
	"-initials":     INITIALS,
	"-jour":         JOUR,
	"-prop":         PROP,
	"-trim":         TRIM,
	"-wct":          WCT,
	"-doi":          DOI,
	"-translate":    TRANSLATE,
	"-replace":      REPLACE,
	"-terms":        TERMS,
	"-words":        WORDS,
	"-pairs":        PAIRS,
	"-pairx":        PAIRX,
	"-reverse":      REVERSE,
	"-letters":      LETTERS,
	"-clauses":      CLAUSES,
	"-indices":      INDICES,
	"-article":      ARTICLE,
	"-abstract":     ABSTRACT,
	"-paragraph":    PARAGRAPH,
	"-stemmed":      STEMMED,
	"-meshcode":     MESHCODE,
	"-matrix":       MATRIX,
	"-classify":     CLASSIFY,
	"-histogram":    HISTOGRAM,
	"-accented":     ACCENTED,
	"-test":         TEST,
	"-scan":         SCAN,
	"-pfx":          PFX,
	"-sfx":          SFX,
	"-sep":          SEP,
	"-tab":          TAB,
	"-ret":          RET,
	"-lbl":          LBL,
	"-tag":          TAG,
	"-att":          ATT,
	"-atr":          ATR,
	"-cls":          CLS,
	"-slf":          SLF,
	"-end":          END,
	"-clr":          CLR,
	"-pfc":          PFC,
	"-deq":          DEQ,
	"-plg":          PLG,
	"-elg":          ELG,
	"-fwd":          FWD,
	"-awd":          AWD,
	"-wrp":          WRP,
	"-enc":          ENC,
	"-pkg":          PKG,
	"-rst":          RST,
	"-def":          DEF,
	"-reg":          REG,
	"-exp":          EXP,
	"-color":        COLOR,
	"-position":     POSITION,
	"-select":       SELECT,
	"-if":           IF,
	"-unless":       UNLESS,
	"-match":        MATCH,
	"-avoid":        AVOID,
	"-and":          AND,
	"-or":           OR,
	"-equals":       EQUALS,
	"-contains":     CONTAINS,
	"-includes":     INCLUDES,
	"-is-within":    ISWITHIN,
	"-starts-with":  STARTSWITH,
	"-ends-with":    ENDSWITH,
	"-is-not":       ISNOT,
	"-is-before":    ISBEFORE,
	"-is-after":     ISAFTER,
	"-matches":      MATCHES,
	"-resembles":    RESEMBLES,
	"-is-equal-to":  ISEQUALTO,
	"-differs-from": DIFFERSFROM,
	"-gt":           GT,
	"-ge":           GE,
	"-lt":           LT,
	"-le":           LE,
	"-eq":           EQ,
	"-ne":           NE,
	"-num":          NUM,
	"-len":          LEN,
	"-sum":          SUM,
	"-acc":          ACC,
	"-min":          MIN,
	"-max":          MAX,
	"-inc":          INC,
	"-dec":          DEC,
	"-sub":          SUB,
	"-avg":          AVG,
	"-dev":          DEV,
	"-med":          MED,
	"-mul":          MUL,
	"-div":          DIV,
	"-mod":          MOD,
	"-lg2":          LG2,
	"-lge":          LGE,
	"-log":          LOG,
	"-bin":          BIN,
	"-oct":          OCT,
	"-hex":          HEX,
	"-bit":          BIT,
	"-pad":          PAD,
	"-raw":          RAW,
	"-0-based":      ZEROBASED,
	"-zero-based":   ZEROBASED,
	"-1-based":      ONEBASED,
	"-one-based":    ONEBASED,
	"-ucsc":         UCSCBASED,
	"-ucsc-based":   UCSCBASED,
	"-ucsc-coords":  UCSCBASED,
	"-bed-based":    UCSCBASED,
	"-bed-coords":   UCSCBASED,
	"-revcomp":      REVCOMP,
	"-nucleic":      NUCLEIC,
	"-fasta":        FASTA,
	"-ncbi2na":      NCBI2NA,
	"-ncbi4na":      NCBI4NA,
	"-molwt":        MOLWT,
	"-hgvs":         HGVS,
	"-else":         ELSE,
}

var sequenceTypeIs = map[string]SequenceType{
	"INSDSeq:INSDInterval_from":       {1, ISSTART},
	"INSDSeq:INSDInterval_to":         {1, ISSTOP},
	"DocumentSummary:ChrStart":        {0, ISSTART},
	"DocumentSummary:ChrStop":         {0, ISSTOP},
	"DocumentSummary:Chr_start":       {1, ISSTART},
	"DocumentSummary:Chr_end":         {1, ISSTOP},
	"DocumentSummary:Chr_inner_start": {1, ISSTART},
	"DocumentSummary:Chr_inner_end":   {1, ISSTOP},
	"DocumentSummary:Chr_outer_start": {1, ISSTART},
	"DocumentSummary:Chr_outer_end":   {1, ISSTOP},
	"DocumentSummary:start":           {1, ISSTART},
	"DocumentSummary:stop":            {1, ISSTOP},
	"DocumentSummary:display_start":   {1, ISSTART},
	"DocumentSummary:display_stop":    {1, ISSTOP},
	"Entrezgene:Seq-interval_from":    {0, ISSTART},
	"Entrezgene:Seq-interval_to":      {0, ISSTOP},
	"GenomicInfoType:ChrStart":        {0, ISSTART},
	"GenomicInfoType:ChrStop":         {0, ISSTOP},
	"RS:position":                     {0, ISPOS},
	"RS:@asnFrom":                     {0, ISSTART},
	"RS:@asnTo":                       {0, ISSTOP},
	"RS:@end":                         {0, ISSTOP},
	"RS:@leftContigNeighborPos":       {0, ISSTART},
	"RS:@physMapInt":                  {0, ISPOS},
	"RS:@protLoc":                     {0, ISPOS},
	"RS:@rightContigNeighborPos":      {0, ISSTOP},
	"RS:@start":                       {0, ISSTART},
	"RS:@structLoc":                   {0, ISPOS},
}

var monthTable = map[string]int{
	"jan":       1,
	"january":   1,
	"feb":       2,
	"february":  2,
	"mar":       3,
	"march":     3,
	"apr":       4,
	"april":     4,
	"may":       5,
	"jun":       6,
	"june":      6,
	"jul":       7,
	"july":      7,
	"aug":       8,
	"august":    8,
	"sep":       9,
	"september": 9,
	"oct":       10,
	"october":   10,
	"nov":       11,
	"november":  11,
	"dec":       12,
	"december":  12,
}

var propertyTable = map[string]string{
	"AssociatedDataset":           "Associated Dataset",
	"AssociatedPublication":       "Associated Publication",
	"CommentIn":                   "Comment In",
	"CommentOn":                   "Comment On",
	"ErratumFor":                  "Erratum For",
	"ErratumIn":                   "Erratum In",
	"ExpressionOfConcernFor":      "Expression Of Concern For",
	"ExpressionOfConcernIn":       "Expression Of Concern In",
	"OriginalReportIn":            "Original Report In",
	"ReprintIn":                   "Reprint In",
	"ReprintOf":                   "Reprint Of",
	"RepublishedFrom":             "Republished From",
	"RepublishedIn":               "Republished In",
	"RetractedandRepublishedFrom": "Retracted And Republished From",
	"RetractedandRepublishedIn":   "Retracted And Republished In",
	"RetractionIn":                "Retraction In",
	"RetractionOf":                "Retraction Of",
	"SummaryForPatientsIn":        "Summary For Patients In",
	"UpdateIn":                    "Update In",
	"UpdateOf":                    "Update Of",
	"aheadofprint":                "Ahead Of Print",
	"epublish":                    "Electronically Published",
	"ppublish":                    "Published In Print",
}

// DATA OBJECTS

// Step contains parameters for executing a single command step
type Step struct {
	Type   OpType
	Value  string
	Parent string
	Match  string
	Attrib string
	TypL   RangeType
	StrL   string
	IntL   int
	TypR   RangeType
	StrR   string
	IntR   int
	Norm   bool
	Wild   bool
	Unesc  bool
}

// Operation breaks commands into sequential steps
type Operation struct {
	Type   OpType
	Value  string
	Stages []*Step
}

// Block contains nested instructions for executing commands
type Block struct {
	Visit      string
	Parent     string
	Match      string
	Path       []string
	Working    []string
	Parsed     []string
	Position   string
	Foreword   string
	Afterword  string
	Conditions []*Operation
	Commands   []*Operation
	Failure    []*Operation
	Subtasks   []*Block
}

// Limiter is used for collecting specific nodes (e.g., first and last)
type Limiter struct {
	Obj *XMLNode
	Idx int
	Lvl int
}

// DebugBlock examines structure of parsed arguments (undocumented)
/*
func DebugBlock(blk *Block, depth int) {

	doIndent := func(indt int) {
		for i := 1; i < indt; i++ {
			fmt.Fprintf(os.Stderr, "  ")
		}
	}

	doIndent(depth)

	if blk.Visit != "" {
		doIndent(depth + 1)
		fmt.Fprintf(os.Stderr, "<Visit> %s </Visit>\n", blk.Visit)
	}
	if len(blk.Parsed) > 0 {
		doIndent(depth + 1)
		fmt.Fprintf(os.Stderr, "<Parsed>")
		for _, str := range blk.Parsed {
			fmt.Fprintf(os.Stderr, " %s", str)
		}
		fmt.Fprintf(os.Stderr, " </Parsed>\n")
	}

	if len(blk.Subtasks) > 0 {
		for _, sub := range blk.Subtasks {
			DebugBlock(sub, depth+1)
		}
	}
}
*/

// PARSE COMMAND-LINE ARGUMENTS

// ParseArguments parses nested exploration instruction from command-line arguments
func ParseArguments(cmdargs []string, pttrn string) *Block {

	// different names of exploration control arguments allow multiple levels of nested "for" loops in a linear command line
	// (capitalized versions for backward-compatibility with original Perl implementation handling of recursive definitions)
	var (
		lcname = []string{
			"",
			"-unit",
			"-subset",
			"-section",
			"-block",
			"-branch",
			"-group",
			"-division",
			"-path",
			"-pattern",
		}

		ucname = []string{
			"",
			"-Unit",
			"-Subset",
			"-Section",
			"-Block",
			"-Branch",
			"-Group",
			"-Division",
			"-Path",
			"-Pattern",
		}
	)

	parseFlag := func(str string) (OpType, bool) {

		op, ok := opTypeIs[str]
		if ok {
			if argTypeIs[str] == EXTRACTION {
				return op, true
			}
			return op, false
		}

		if len(str) > 1 && str[0] == '-' && IsAllCapsOrDigits(str[1:]) {
			return VARIABLE, true
		}

		if len(str) > 2 && strings.HasPrefix(str, "--") && IsAllCapsOrDigits(str[2:]) {
			return ACCUMULATOR, true
		}

		if len(str) > 0 && str[0] == '-' {
			return UNRECOGNIZED, false
		}

		return UNSET, false
	}

	// parseCommands recursive definition
	var parseCommands func(parent *Block, startLevel LevelType)

	// parseCommands does initial parsing of exploration command structure
	parseCommands = func(parent *Block, startLevel LevelType) {

		// find next highest level exploration argument
		findNextLevel := func(args []string, level LevelType) (LevelType, string, string) {

			if len(args) > 1 {

				for {

					if level < UNIT {
						break
					}

					lctag := lcname[level]
					uctag := ucname[level]

					for _, txt := range args {
						if txt == lctag {
							return level, lctag, uctag
						}
						if txt == uctag {
							fmt.Fprintf(os.Stderr, "\nWARNING: Upper-case '%s' exploration command is deprecated, use lower-case '%s' instead\n", uctag, lctag)
							return level, lctag, uctag
						}
					}

					level--
				}
			}

			return 0, "", ""
		}

		arguments := parent.Working

		level, lctag, uctag := findNextLevel(arguments, startLevel)

		if level < UNIT {

			// break recursion
			return
		}

		// group arguments at a given exploration level
		subsetCommands := func(args []string) *Block {

			max := len(args)

			visit := ""

			// extract name of object to visit
			if max > 1 {
				visit = args[1]
				args = args[2:]
				max -= 2
			}

			partition := 0
			for cur, str := range args {

				// record point of next exploration command
				partition = cur + 1

				// skip if not a command
				if len(str) < 1 || str[0] != '-' {
					continue
				}

				if argTypeIs[str] == EXPLORATION {
					partition = cur
					break
				}
			}

			// convert slashes (e.g., parent/child construct) to periods (e.g., dotted exploration path)
			if strings.Contains(visit, "/") {
				if !strings.Contains(visit, ".") {
					visit = strings.Replace(visit, "/", ".", -1)
				}
			}

			// parse parent.child or dotted path construct
			// colon indicates a namespace prefix in any or all of the components
			prnt, rmdr := SplitInTwoRight(visit, ".")
			match, rest := SplitInTwoLeft(rmdr, ".")

			if rest != "" {

				// exploration match on first component, then search remainder one level at a time with subsequent components
				dirs := strings.Split(rmdr, ".")

				// signal with "path" position
				return &Block{Visit: visit, Parent: "", Match: prnt, Path: dirs, Position: "path", Parsed: args[0:partition], Working: args[partition:]}
			}

			// promote arguments parsed at this level
			return &Block{Visit: visit, Parent: prnt, Match: match, Parsed: args[0:partition], Working: args[partition:]}
		}

		cur := 0

		// search for positions of current exploration command

		for idx, txt := range arguments {
			if txt == lctag || txt == uctag {
				if idx == 0 {
					continue
				}

				blk := subsetCommands(arguments[cur:idx])
				parseCommands(blk, level-1)
				parent.Subtasks = append(parent.Subtasks, blk)

				cur = idx
			}
		}

		if cur < len(arguments) {
			blk := subsetCommands(arguments[cur:])
			parseCommands(blk, level-1)
			parent.Subtasks = append(parent.Subtasks, blk)
		}

		// clear execution arguments from parent after subsetting
		parent.Working = nil
	}

	// parse optional [min:max], [&VAR:&VAR], or [after|before] range specification
	parseRange := func(item, rnge string) (typL RangeType, strL string, intL int, typR RangeType, strR string, intR int) {

		typL = NORANGE
		typR = NORANGE
		strL = ""
		strR = ""
		intL = 0
		intR = 0

		if rnge == "" {
			// no range specification, return default values
			return
		}

		// check if last character is right square bracket
		if !strings.HasSuffix(rnge, "]") {
			fmt.Fprintf(os.Stderr, "\nERROR: Unrecognized range %s\n", rnge)
			os.Exit(1)
		}

		rnge = strings.TrimSuffix(rnge, "]")

		if rnge == "" {
			fmt.Fprintf(os.Stderr, "\nERROR: Empty range %s[]\n", item)
			os.Exit(1)
		}

		// check for [after|before] variant
		if strings.Contains(rnge, "|") {

			strL, strR = SplitInTwoLeft(rnge, "|")
			// spacing matters, so do not call TrimSpace

			if strL == "" && strR == "" {
				fmt.Fprintf(os.Stderr, "\nERROR: Empty range %s[|]\n", item)
				os.Exit(1)
			}

			typL = STRINGRANGE
			typR = STRINGRANGE

			// return statement returns named variables
			return
		}

		// otherwise must have colon within brackets
		if !strings.Contains(rnge, ":") {
			fmt.Fprintf(os.Stderr, "\nERROR: Colon missing in range %s[%s]\n", item, rnge)
			os.Exit(1)
		}

		// split at colon
		lft, rgt := SplitInTwoLeft(rnge, ":")

		lft = strings.TrimSpace(lft)
		rgt = strings.TrimSpace(rgt)

		if lft == "" && rgt == "" {
			fmt.Fprintf(os.Stderr, "\nERROR: Empty range %s[:]\n", item)
			os.Exit(1)
		}

		// for variable, parse optional +/- offset suffix
		parseOffset := func(str string) (string, int) {

			if str == "" || str[0] == ' ' {
				fmt.Fprintf(os.Stderr, "\nERROR: Unrecognized variable '&%s'\n", str)
				os.Exit(1)
			}

			pls := ""
			mns := ""

			ofs := 0

			// check for &VAR+1 or &VAR-1 integer adjustment
			str, pls = SplitInTwoLeft(str, "+")
			str, mns = SplitInTwoLeft(str, "-")

			if pls != "" {
				val, err := strconv.Atoi(pls)
				if err != nil {
					fmt.Fprintf(os.Stderr, "\nERROR: Unrecognized range adjustment &%s+%s\n", str, pls)
					os.Exit(1)
				}
				ofs = val
			} else if mns != "" {
				val, err := strconv.Atoi(mns)
				if err != nil {
					fmt.Fprintf(os.Stderr, "\nERROR: Unrecognized range adjustment &%s-%s\n", str, mns)
					os.Exit(1)
				}
				ofs = -val
			}

			return str, ofs
		}

		// parse integer position, 1-based coordinate must be greater than 0
		parseInteger := func(str string, mustBePositive bool) int {
			if str == "" {
				return 0
			}

			val, err := strconv.Atoi(str)
			if err != nil {
				fmt.Fprintf(os.Stderr, "\nERROR: Unrecognized range component %s[%s:]\n", item, str)
				os.Exit(1)
			}
			if mustBePositive {
				if val < 1 {
					fmt.Fprintf(os.Stderr, "\nERROR: Range component %s[%s:] must be positive\n", item, str)
					os.Exit(1)
				}
			} else {
				if val == 0 {
					fmt.Fprintf(os.Stderr, "\nERROR: Range component %s[%s:] must not be zero\n", item, str)
					os.Exit(1)
				}
			}

			return val
		}

		if lft != "" {
			if lft[0] == '&' {
				lft = lft[1:]
				strL, intL = parseOffset(lft)
				typL = VARIABLERANGE
			} else {
				intL = parseInteger(lft, true)
				typL = INTEGERRANGE
			}
		}

		if rgt != "" {
			if rgt[0] == '&' {
				rgt = rgt[1:]
				strR, intR = parseOffset(rgt)
				typR = VARIABLERANGE
			} else {
				intR = parseInteger(rgt, false)
				typR = INTEGERRANGE
			}
		}

		// return statement required to return named variables
		return
	}

	parseConditionals := func(cmds *Block, arguments []string) []*Operation {

		max := len(arguments)
		if max < 1 {
			return nil
		}

		// check for missing condition command
		txt := arguments[0]
		if txt != "-if" && txt != "-unless" && txt != "-select" && txt != "-match" && txt != "-avoid" && txt != "-position" {
			fmt.Fprintf(os.Stderr, "\nERROR: Missing -if command before '%s'\n", txt)
			os.Exit(1)
		}
		if txt == "-position" && max > 2 {
			fmt.Fprintf(os.Stderr, "\nERROR: Cannot combine -position with -if or -unless commands\n")
			os.Exit(1)
		}
		// check for missing argument after last condition
		txt = arguments[max-1]
		if len(txt) > 0 && txt[0] == '-' {
			fmt.Fprintf(os.Stderr, "\nERROR: Item missing after %s command\n", txt)
			os.Exit(1)
		}

		cond := make([]*Operation, 0, max)

		// parse conditional clause into execution step
		parseStep := func(op *Operation, elementColonValue bool) {

			if op == nil {
				return
			}

			str := op.Value

			status := ELEMENT

			// isolate and parse optional [min:max], [&VAR:&VAR], or [after|before] range specification
			str, rnge := SplitInTwoLeft(str, "[")

			str = strings.TrimSpace(str)
			rnge = strings.TrimSpace(rnge)

			if str == "" && rnge != "" {
				fmt.Fprintf(os.Stderr, "\nERROR: Variable missing in range specification [%s\n", rnge)
				os.Exit(1)
			}

			typL, strL, intL, typR, strR, intR := parseRange(str, rnge)

			// check for pound, percent, or caret character at beginning of name
			if len(str) > 1 {
				switch str[0] {
				case '&':
					if IsAllCapsOrDigits(str[1:]) {
						status = VARIABLE
						str = str[1:]
					} else if strings.Contains(str, ":") {
						fmt.Fprintf(os.Stderr, "\nERROR: Unsupported construct '%s', use -if &VARIABLE -equals VALUE instead\n", str)
						os.Exit(1)
					} else {
						fmt.Fprintf(os.Stderr, "\nERROR: Unrecognized variable '%s'\n", str)
						os.Exit(1)
					}
				case '#':
					status = COUNT
					str = str[1:]
				case '%':
					status = LENGTH
					str = str[1:]
				case '^':
					status = DEPTH
					str = str[1:]
				default:
				}
			} else if str == "+" {
				status = INDEX
			}

			// parse parent/element@attribute construct
			// colon indicates a namespace prefix in any or all of the components
			prnt, match := SplitInTwoRight(str, "/")
			match, attrib := SplitInTwoLeft(match, "@")
			val := ""

			// leading colon indicates namespace prefix wildcard
			wildcard := false
			if strings.HasPrefix(prnt, ":") || strings.HasPrefix(match, ":") || strings.HasPrefix(attrib, ":") {
				wildcard = true
			}

			if elementColonValue {

				// allow parent/element@attribute:value construct for deprecated -match and -avoid, and for subsequent -and and -or commands
				match, val = SplitInTwoLeft(str, ":")
				prnt, match = SplitInTwoRight(match, "/")
				match, attrib = SplitInTwoLeft(match, "@")
			}

			norm := true
			if rnge != "" {
				if typL != NORANGE || typR != NORANGE || strL != "" || strR != "" || intL != 0 || intR != 0 {
					norm = false
				}
			}

			tsk := &Step{Type: status, Value: str, Parent: prnt, Match: match, Attrib: attrib,
				TypL: typL, StrL: strL, IntL: intL, TypR: typR, StrR: strR, IntR: intR,
				Norm: norm, Wild: wildcard}

			op.Stages = append(op.Stages, tsk)

			// transform old -match "element:value" to -match element -equals value
			if val != "" {
				tsk := &Step{Type: EQUALS, Value: val}
				op.Stages = append(op.Stages, tsk)
			}
		}

		idx := 0

		// conditionals should alternate between command and object/value
		expectDash := true
		last := ""

		var op *Operation

		// flag to allow element-colon-value for deprecated -match and -avoid commands, otherwise colon is for namespace prefixes
		elementColonValue := false

		status := UNSET

		numIf := 0
		numUnless := 0
		lastCond := ""

		// parse command strings into operation structure
		for idx < max {
			str := arguments[idx]
			idx++

			// conditionals should alternate between command and object/value
			if expectDash {
				if len(str) < 1 || str[0] != '-' {
					fmt.Fprintf(os.Stderr, "\nERROR: Unexpected '%s' argument after '%s'\n", str, last)
					os.Exit(1)
				}
				expectDash = false
			} else {
				if len(str) > 0 && str[0] == '-' {
					fmt.Fprintf(os.Stderr, "\nERROR: Unexpected '%s' command after '%s'\n", str, last)
					os.Exit(1)
				}
				expectDash = true
			}
			last = str

			switch status {
			case UNSET:
				status, _ = parseFlag(str)
			case POSITION:
				if cmds.Position != "" {
					fmt.Fprintf(os.Stderr, "\nERROR: -position '%s' conflicts with existing '%s'\n", str, cmds.Position)
					os.Exit(1)
				}
				cmds.Position = str
				status = UNSET
			case MATCH, AVOID:
				elementColonValue = true
				fallthrough
			case IF:
				numIf++
				if numIf > 1 || numUnless > 1 || numIf > 0 && numUnless > 0 {
					fmt.Fprintf(os.Stderr, "\nERROR: Unexpected '-if %s' after '%s'\n", str, lastCond)
					os.Exit(1)
				}
				lastCond = "-if " + str
				op = &Operation{Type: status, Value: str}
				cond = append(cond, op)
				parseStep(op, elementColonValue)
				status = UNSET
			case UNLESS:
				numUnless++
				if numIf > 1 || numUnless > 1 || numIf > 0 && numUnless > 0 {
					fmt.Fprintf(os.Stderr, "\nERROR: Unexpected '-unless %s' after '%s'\n", str, lastCond)
					os.Exit(1)
				}
				lastCond = "-unless " + str
				op = &Operation{Type: status, Value: str}
				cond = append(cond, op)
				parseStep(op, elementColonValue)
				status = UNSET
			case SELECT, AND, OR:
				op = &Operation{Type: status, Value: str}
				cond = append(cond, op)
				parseStep(op, elementColonValue)
				status = UNSET
			case EQUALS, CONTAINS, INCLUDES, ISWITHIN, STARTSWITH, ENDSWITH, ISNOT, ISBEFORE, ISAFTER:
				if op != nil {
					if len(str) > 1 && str[0] == '\\' {
						// first character may be backslash protecting dash (undocumented)
						str = str[1:]
					}
					tsk := &Step{Type: status, Value: str}
					op.Stages = append(op.Stages, tsk)
					op = nil
				} else {
					fmt.Fprintf(os.Stderr, "\nERROR: Unexpected adjacent string match constraints\n")
					os.Exit(1)
				}
				status = UNSET
			case MATCHES:
				if op != nil {
					if len(str) > 1 && str[0] == '\\' {
						// first character may be backslash protecting dash (undocumented)
						str = str[1:]
					}
					str = RemoveCommaOrSemicolon(str)
					tsk := &Step{Type: status, Value: str}
					op.Stages = append(op.Stages, tsk)
					op = nil
				} else {
					fmt.Fprintf(os.Stderr, "\nERROR: Unexpected adjacent string match constraints\n")
					os.Exit(1)
				}
				status = UNSET
			case RESEMBLES:
				if op != nil {
					if len(str) > 1 && str[0] == '\\' {
						// first character may be backslash protecting dash (undocumented)
						str = str[1:]
					}
					str = SortStringByWords(str)
					tsk := &Step{Type: status, Value: str}
					op.Stages = append(op.Stages, tsk)
					op = nil
				} else {
					fmt.Fprintf(os.Stderr, "\nERROR: Unexpected adjacent string match constraints\n")
					os.Exit(1)
				}
				status = UNSET
			case ISEQUALTO, DIFFERSFROM:
				if op != nil {
					if len(str) < 1 {
						fmt.Fprintf(os.Stderr, "\nERROR: Empty conditional argument\n")
						os.Exit(1)
					}
					ch := str[0]
					// uses element as second argument
					orig := str
					if ch == '#' || ch == '%' || ch == '^' {
						// check for pound, percent, or caret character at beginning of element (undocumented)
						str = str[1:]
						if len(str) < 1 {
							fmt.Fprintf(os.Stderr, "\nERROR: Unexpected conditional constraints\n")
							os.Exit(1)
						}
						ch = str[0]
					}
					if (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') {
						prnt, match := SplitInTwoRight(str, "/")
						match, attrib := SplitInTwoLeft(match, "@")
						wildcard := false
						if strings.HasPrefix(prnt, ":") || strings.HasPrefix(match, ":") || strings.HasPrefix(attrib, ":") {
							wildcard = true
						}
						tsk := &Step{Type: status, Value: orig, Parent: prnt, Match: match, Attrib: attrib, Wild: wildcard}
						op.Stages = append(op.Stages, tsk)
					} else {
						fmt.Fprintf(os.Stderr, "\nERROR: Unexpected conditional constraints\n")
						os.Exit(1)
					}
					op = nil
				}
				status = UNSET
			case GT, GE, LT, LE, EQ, NE:
				if op != nil {
					if len(str) > 1 && str[0] == '\\' {
						// first character may be backslash protecting minus sign (undocumented)
						str = str[1:]
					}
					if len(str) < 1 {
						fmt.Fprintf(os.Stderr, "\nERROR: Empty numeric match constraints\n")
						os.Exit(1)
					}
					ch := str[0]
					if (ch >= '0' && ch <= '9') || ch == '-' || ch == '+' {
						// literal numeric constant
						tsk := &Step{Type: status, Value: str}
						op.Stages = append(op.Stages, tsk)
					} else {
						// numeric test allows element as second argument
						orig := str
						if ch == '#' || ch == '%' || ch == '^' {
							// check for pound, percent, or caret character at beginning of element (undocumented)
							str = str[1:]
							if len(str) < 1 {
								fmt.Fprintf(os.Stderr, "\nERROR: Unexpected numeric match constraints\n")
								os.Exit(1)
							}
							ch = str[0]
						}
						if (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || ch == '&' {
							prnt, match := SplitInTwoRight(str, "/")
							match, attrib := SplitInTwoLeft(match, "@")
							wildcard := false
							if strings.HasPrefix(prnt, ":") || strings.HasPrefix(match, ":") || strings.HasPrefix(attrib, ":") {
								wildcard = true
							}
							tsk := &Step{Type: status, Value: orig, Parent: prnt, Match: match, Attrib: attrib, Wild: wildcard}
							op.Stages = append(op.Stages, tsk)
						} else {
							fmt.Fprintf(os.Stderr, "\nERROR: Unexpected numeric match constraints\n")
							os.Exit(1)
						}
					}
					op = nil
				} else {
					fmt.Fprintf(os.Stderr, "\nERROR: Unexpected adjacent numeric match constraints\n")
					os.Exit(1)
				}
				status = UNSET
			case UNRECOGNIZED:
				fmt.Fprintf(os.Stderr, "\nERROR: Unrecognized argument '%s'\n", str)
				os.Exit(1)
			default:
				fmt.Fprintf(os.Stderr, "\nERROR: Unexpected argument '%s'\n", str)
				os.Exit(1)
			}
		}

		return cond
	}

	parseExtractions := func(cmds *Block, arguments []string) []*Operation {

		max := len(arguments)
		if max < 1 {
			return nil
		}

		// check for missing -element (or -first, etc.) command
		txt := arguments[0]
		if len(txt) < 1 || txt[0] != '-' {
			fmt.Fprintf(os.Stderr, "\nERROR: Missing -element command before '%s'\n", txt)
			os.Exit(1)
		}
		// check for missing argument after last -element (or -first, etc.) command
		txt = arguments[max-1]
		if len(txt) > 0 && txt[0] == '-' {
			if txt == "-rst" {
				fmt.Fprintf(os.Stderr, "\nERROR: Unexpected position for %s command\n", txt)
				os.Exit(1)
			} else if txt == "-clr" {
				// main loop runs out after trailing -clr, add another one so this one will be executed
				arguments = append(arguments, "-clr")
				max++
			} else if txt == "-cls" || txt == "-slf" {
				// okay at end
			} else if max < 2 || arguments[max-2] != "-lbl" {
				fmt.Fprintf(os.Stderr, "\nERROR: Item missing after %s command\n", txt)
				os.Exit(1)
			} else if max < 3 || (arguments[max-3] != "-att" && arguments[max-3] != "-atr") {
				fmt.Fprintf(os.Stderr, "\nERROR: Item missing after %s command\n", txt)
				os.Exit(1)
			}
		}

		comm := make([]*Operation, 0, max)

		// parse next argument
		nextStatus := func(str string) (OpType, bool) {

			status, isExtraction := parseFlag(str)

			// no-argument flags are supported here to prevent subsequent "No -element before" error
			switch status {
			case VARIABLE:
				op := &Operation{Type: status, Value: str[1:]}
				comm = append(comm, op)
				status = VALUE
			case ACCUMULATOR:
				op := &Operation{Type: status, Value: str[2:]}
				comm = append(comm, op)
				status = VALUE
			case CLR, RST:
				op := &Operation{Type: status, Value: ""}
				comm = append(comm, op)
				status = UNSET
			case ELEMENT:
			case TAB, RET, PFX, SFX, SEP, LBL, TAG, ATT, ATR, END, PFC, DEQ, PLG, ELG, WRP, ENC, DEF, REG, EXP, COLOR:
			case CLS:
				op := &Operation{Type: LBL, Value: ">"}
				comm = append(comm, op)
				status = UNSET
			case SLF:
				op := &Operation{Type: LBL, Value: " />"}
				comm = append(comm, op)
				status = UNSET
			case FWD, AWD, PKG:
			case UNSET:
				fmt.Fprintf(os.Stderr, "\nERROR: No -element before '%s'\n", str)
				os.Exit(1)
			case UNRECOGNIZED:
				fmt.Fprintf(os.Stderr, "\nERROR: Unrecognized argument '%s'\n", str)
				os.Exit(1)
			default:
				if !isExtraction {
					// not ELEMENT through HGVS
					fmt.Fprintf(os.Stderr, "\nERROR: Misplaced %s command\n", str)
					os.Exit(1)
				}
			}

			return status, isExtraction
		}

		// parse extraction clause into individual steps
		parseSteps := func(op *Operation, pttrn string) {

			if op == nil {
				return
			}

			stat := op.Type
			str := op.Value

			// element names combined with commas are treated as a prefix-separator-suffix group
			comma := strings.Split(str, ",")

			rnge := ""
			for _, item := range comma {
				status := stat

				// isolate and parse optional [min:max], [&VAR:&VAR], or [after|before] range specification
				item, rnge = SplitInTwoLeft(item, "[")

				item = strings.TrimSpace(item)
				rnge = strings.TrimSpace(rnge)

				if item == "" && rnge != "" {
					fmt.Fprintf(os.Stderr, "\nERROR: Variable missing in range specification [%s\n", rnge)
					os.Exit(1)
				}

				typL, strL, intL, typR, strR, intR := parseRange(item, rnge)

				// check for special character at beginning of name
				if len(item) > 1 {
					switch item[0] {
					case '&':
						if IsAllCapsOrDigits(item[1:]) {
							status = VARIABLE
							item = item[1:]
						} else {
							fmt.Fprintf(os.Stderr, "\nERROR: Unrecognized variable '%s'\n", item)
							os.Exit(1)
						}
					case '#':
						status = COUNT
						item = item[1:]
					case '%':
						status = LENGTH
						item = item[1:]
					case '^':
						status = DEPTH
						item = item[1:]
					case '*':
						for _, ch := range item {
							if ch != '*' {
								break
							}
						}
						status = STAR
					default:
					}
				} else {
					switch item {
					case "?":
						status = QUESTION
					case "~":
						status = TILDE
					case ".":
						status = DOT
					case "%":
						status = PRCNT
					case "*":
						status = STAR
					case "$":
						status = DOLLAR
					case "@":
						status = ATSIGN
					case "+":
						status = INDEX
					default:
					}
				}

				// parse parent/element@attribute construct
				// colon indicates a namespace prefix in any or all of the components
				prnt, match := SplitInTwoRight(item, "/")
				match, attrib := SplitInTwoLeft(match, "@")

				// leading colon indicates namespace prefix wildcard
				wildcard := false
				if strings.HasPrefix(prnt, ":") || strings.HasPrefix(match, ":") || strings.HasPrefix(attrib, ":") {
					wildcard = true
				}

				// sequence coordinate adjustments
				switch status {
				case ZEROBASED, ONEBASED, UCSCBASED:
					seq := pttrn + ":"
					if attrib != "" {
						seq += "@"
						seq += attrib
					} else if match != "" {
						seq += match
					}
					// confirm -0-based or -1-based arguments are known sequence position elements or attributes
					slock.RLock()
					seqtype, ok := sequenceTypeIs[seq]
					slock.RUnlock()
					if !ok {
						fmt.Fprintf(os.Stderr, "\nERROR: Element '%s' is not suitable for sequence coordinate conversion\n", item)
						os.Exit(1)
					}
					switch status {
					case ZEROBASED:
						status = ELEMENT
						// if 1-based coordinates, decrement to get 0-based value
						if seqtype.Based == 1 {
							status = DEC
						}
					case ONEBASED:
						status = ELEMENT
						// if 0-based coordinates, increment to get 1-based value
						if seqtype.Based == 0 {
							status = INC
						}
					case UCSCBASED:
						status = ELEMENT
						// half-open intervals, start is 0-based, stop is 1-based
						if seqtype.Based == 0 && seqtype.Which == ISSTOP {
							status = INC
						} else if seqtype.Based == 1 && seqtype.Which == ISSTART {
							status = DEC
						}
					default:
						status = ELEMENT
					}
				default:
				}

				norm := true
				if rnge != "" {
					if typL != NORANGE || typR != NORANGE || strL != "" || strR != "" || intL != 0 || intR != 0 {
						norm = false
					}
				}

				unescape := (status != INDICES && status != ARTICLE && status != ABSTRACT && status != PARAGRAPH && status != STEMMED && status != RAW)

				tsk := &Step{Type: status, Value: item, Parent: prnt, Match: match, Attrib: attrib,
					TypL: typL, StrL: strL, IntL: intL, TypR: typR, StrR: strR, IntR: intR,
					Norm: norm, Wild: wildcard, Unesc: unescape}

				op.Stages = append(op.Stages, tsk)
			}
		}

		idx := 0

		status := UNSET
		isExtraction := false

		// parse command strings into operation structure
		for idx < max {
			str := arguments[idx]
			idx++

			if argTypeIs[str] == CONDITIONAL {
				fmt.Fprintf(os.Stderr, "\nERROR: Misplaced %s command\n", str)
				os.Exit(1)
			}

			switch status {
			case UNSET:
				status, isExtraction = nextStatus(str)
			case TAB, RET, PFX, SFX, SEP, LBL, CLS, SLF, PFC, DEQ, PLG, ELG, WRP, ENC, DEF, REG, EXP, COLOR:
				op := &Operation{Type: status, Value: ConvertSlash(str)}
				comm = append(comm, op)
				status = UNSET
			case TAG:
				// when starting to construct XML tag and attributes from components, first clear -tab and -sep values
				op := &Operation{Type: TAB, Value: ""}
				comm = append(comm, op)
				op = &Operation{Type: SEP, Value: ""}
				comm = append(comm, op)
				// TAG variant of LBL sets wrp flag for automatic content reencoding
				op = &Operation{Type: TAG, Value: "<" + ConvertSlash(str)}
				comm = append(comm, op)
				status = UNSET
			case ATT:
				if idx < max {
					// -att takes key and literal string value
					val := arguments[idx]
					idx++
					if val != "" {
						op := &Operation{Type: LBL, Value: " " + ConvertSlash(str) + "=" + "\"" + ConvertSlash(val) + "\""}
						comm = append(comm, op)
					}
				}
				status = UNSET
			case ATR:
				if idx < max {
					// -atr takes key and object or &variable name
					val := arguments[idx]
					idx++
					if val != "" {
						op := &Operation{Type: LBL, Value: " " + ConvertSlash(str) + "=" + "\""}
						comm = append(comm, op)
						op = &Operation{Type: ELEMENT, Value: val}
						comm = append(comm, op)
						parseSteps(op, pttrn)
						op = &Operation{Type: LBL, Value: "\""}
						comm = append(comm, op)
					}
				}
				status = UNSET
			case END:
				op := &Operation{Type: LBL, Value: "</" + ConvertSlash(str) + ">"}
				comm = append(comm, op)
				status = UNSET
			case FWD:
				cmds.Foreword = ConvertSlash(str)
				status = UNSET
			case AWD:
				cmds.Afterword = ConvertSlash(str)
				status = UNSET
			case PKG:
				pkg := ConvertSlash(str)
				cmds.Foreword = ""
				cmds.Afterword = ""
				if pkg != "" && pkg != "-" {
					items := strings.Split(pkg, "/")
					for i := 0; i < len(items); i++ {
						cmds.Foreword += "<" + items[i] + ">"
					}
					for i := len(items) - 1; i >= 0; i-- {
						cmds.Afterword += "</" + items[i] + ">"
					}
				}
				status = UNSET
			case VARIABLE:
				op := &Operation{Type: status, Value: str[1:]}
				comm = append(comm, op)
				status = VALUE
			case ACCUMULATOR:
				op := &Operation{Type: status, Value: str[2:]}
				comm = append(comm, op)
				status = VALUE
			case VALUE:
				op := &Operation{Type: status, Value: str}
				comm = append(comm, op)
				parseSteps(op, pttrn)
				status = UNSET
			case UNRECOGNIZED:
				fmt.Fprintf(os.Stderr, "\nERROR: Unrecognized argument '%s'\n", str)
				os.Exit(1)
			default:
				if isExtraction {
					// ELEMENT through HGVS
					for !strings.HasPrefix(str, "-") {
						// create one operation per argument, even if under a single -element statement
						op := &Operation{Type: status, Value: str}
						comm = append(comm, op)
						parseSteps(op, pttrn)
						if idx >= max {
							break
						}
						str = arguments[idx]
						idx++
					}
					status = UNSET
					if idx < max {
						status, isExtraction = nextStatus(str)
					}
				}
			}
		}

		return comm
	}

	// parseOperations recursive definition
	var parseOperations func(parent *Block)

	// parseOperations converts parsed arguments to operations lists
	parseOperations = func(parent *Block) {

		args := parent.Parsed

		partition := 0
		for cur, str := range args {

			// record junction between conditional and extraction commands
			partition = cur + 1

			// skip if not a command
			if len(str) < 1 || str[0] != '-' {
				continue
			}

			if argTypeIs[str] != CONDITIONAL {
				partition = cur
				break
			}
		}

		// split arguments into conditional tests and extraction or customization commands
		conditionals := args[0:partition]
		args = args[partition:]

		partition = 0
		foundElse := false
		for cur, str := range args {

			// record junction at -else command
			partition = cur + 1

			// skip if not a command
			if len(str) < 1 || str[0] != '-' {
				continue
			}

			if str == "-else" {
				partition = cur
				foundElse = true
				break
			}
		}

		extractions := args[0:partition]
		alternative := args[partition:]

		if len(alternative) > 0 && alternative[0] == "-else" {
			alternative = alternative[1:]
		}

		// validate argument structure and convert to operations lists
		parent.Conditions = parseConditionals(parent, conditionals)
		parent.Commands = parseExtractions(parent, extractions)
		parent.Failure = parseExtractions(parent, alternative)

		// reality checks on placement of -else command
		if foundElse {
			if len(conditionals) < 1 {
				fmt.Fprintf(os.Stderr, "\nERROR: Misplaced -else command\n")
				os.Exit(1)
			}
			if len(alternative) < 1 {
				fmt.Fprintf(os.Stderr, "\nERROR: Misplaced -else command\n")
				os.Exit(1)
			}
			if len(parent.Subtasks) > 0 {
				fmt.Fprintf(os.Stderr, "\nERROR: Misplaced -else command\n")
				os.Exit(1)
			}
		}

		for _, sub := range parent.Subtasks {
			parseOperations(sub)
		}
	}

	// ParseArguments

	head := &Block{}

	for _, txt := range cmdargs {
		head.Working = append(head.Working, txt)
	}

	// initial parsing of exploration command structure
	parseCommands(head, PATTERN)

	if len(head.Subtasks) != 1 {
		return nil
	}

	// skip past empty placeholder
	head = head.Subtasks[0]

	// convert command strings to array of operations for faster processing
	parseOperations(head)

	// check for no -element or multiple -pattern commands
	noElement := true
	noClose := true
	numPatterns := 0
	for _, txt := range cmdargs {
		if argTypeIs[txt] == EXTRACTION {
			noElement = false
		}
		if txt == "-pattern" || txt == "-Pattern" {
			numPatterns++
		} else if txt == "-select" {
			noElement = false
			head.Position = "select"
		} else if txt == "-cls" || txt == "-slf" {
			noClose = false
		}
	}

	if numPatterns < 1 {
		fmt.Fprintf(os.Stderr, "\nERROR: No -pattern in command-line arguments\n")
		os.Exit(1)
	}

	if numPatterns > 1 {
		fmt.Fprintf(os.Stderr, "\nERROR: Only one -pattern command is permitted\n")
		os.Exit(1)
	}

	if noElement && noClose {
		fmt.Fprintf(os.Stderr, "\nERROR: No -element statement in argument list\n")
		os.Exit(1)
	}

	return head
}

// printXMLtree supports XML compression styles selected by -element "*" through "****"
func printXMLtree(node *XMLNode, style IndentType, printAttrs bool, proc func(string)) {

	if node == nil || proc == nil {
		return
	}

	// WRAPPED is SUBTREE plus each attribute on its own line
	wrapped := false
	if style == WRAPPED {
		style = SUBTREE
		wrapped = true
	}

	// INDENT is offset by two spaces to allow for parent tag, SUBTREE is not offset
	initial := 1
	if style == SUBTREE {
		style = INDENT
		initial = 0
	}

	// array to speed up indentation
	indentSpaces := []string{
		"",
		"  ",
		"    ",
		"      ",
		"        ",
		"          ",
		"            ",
		"              ",
		"                ",
		"                  ",
	}

	// indent a specified number of spaces
	doIndent := func(indt int) {
		i := indt
		for i > 9 {
			proc("                    ")
			i -= 10
		}
		if i < 0 {
			return
		}
		proc(indentSpaces[i])
	}

	// doSubtree recursive definition
	var doSubtree func(*XMLNode, int)

	doSubtree = func(curr *XMLNode, depth int) {

		// suppress if it would be an empty self-closing tag
		if !IsNotJustWhitespace(curr.Attributes) && curr.Contents == "" && curr.Children == nil {
			return
		}

		if style == INDENT {
			doIndent(depth)
		}

		if curr.Name != "" {
			proc("<")
			proc(curr.Name)

			if printAttrs {

				attr := strings.TrimSpace(curr.Attributes)
				attr = CompressRunsOfSpaces(attr)

				if attr != "" {

					if wrapped {

						start := 0
						idx := 0

						attlen := len(attr)

						for idx < attlen {
							ch := attr[idx]
							if ch == '=' {
								str := attr[start:idx]
								proc("\n")
								doIndent(depth)
								proc(" ")
								proc(str)
								// skip past equal sign and leading double quote
								idx += 2
								start = idx
							} else if ch == '"' || ch == '\'' {
								str := attr[start:idx]
								proc("=\"")
								proc(str)
								proc("\"")
								// skip past trailing double quote and (possible) space
								idx += 2
								start = idx
							} else {
								idx++
							}
						}

						proc("\n")
						doIndent(depth)

					} else {

						proc(" ")
						proc(attr)
					}
				}
			}

			// see if suitable for for self-closing tag
			if curr.Contents == "" && curr.Children == nil {
				proc("/>")
				if style != COMPACT {
					proc("\n")
				}
				return
			}

			proc(">")
		}

		if curr.Contents != "" {

			proc(curr.Contents[:])

		} else {

			if style != COMPACT {
				proc("\n")
			}

			for chld := curr.Children; chld != nil; chld = chld.Next {
				doSubtree(chld, depth+1)
			}

			if style == INDENT {
				i := depth
				for i > 9 {
					proc("                    ")
					i -= 10
				}
				proc(indentSpaces[i])
			}
		}

		if curr.Name != "" {
			proc("<")
			proc("/")
			proc(curr.Name)
			proc(">")
		}

		if style != COMPACT {
			proc("\n")
		}
	}

	doSubtree(node, initial)
}

// printASNtree prints ASN.1 selected by -element "."
func printASNtree(node *XMLNode, proc func(string)) {

	if node == nil || proc == nil {
		return
	}

	// array to speed up indentation
	indentSpaces := []string{
		"",
		"  ",
		"    ",
		"      ",
		"        ",
		"          ",
		"            ",
		"              ",
		"                ",
		"                  ",
	}

	// indent a specified number of spaces
	doIndent := func(indt int) {
		i := indt
		for i > 9 {
			proc("                    ")
			i -= 10
		}
		if i < 0 {
			return
		}
		proc(indentSpaces[i])
	}

	afix := strings.NewReplacer(
		"&lt;", "<",
		"&gt;", ">",
		"&amp;", "&",
		"&apos;", "'",
		"&#39;", "'",
		"&quot;", "'",
		"&#34;", "'",
		"\"", "'",
	)

	// doASNtree recursive definition
	var doASNtree func(*XMLNode, int, bool)

	doASNtree = func(curr *XMLNode, depth int, comma bool) {

		// suppress if it would be an empty self-closing tag
		if !IsNotJustWhitespace(curr.Attributes) && curr.Contents == "" && curr.Children == nil {
			return
		}

		name := curr.Name
		if name == "" {
			name = "_"
		}

		// just a hyphen   - unnamed braces (element of SEQUENCE OF or SET OF structured objects)
		// trailing hyphen - unquoted value
		// internal hyphen - convert to space
		show := true
		quot := true
		if name == "_" {
			show = false
		} else if strings.HasPrefix(name, "_") {
			// if leading hyphen, ignore remainder of name
			show = false
		} else if strings.HasSuffix(name, "_") {
			name = strings.TrimSuffix(name, "_")
			quot = false
		}
		name = strings.Replace(name, "_", " ", -1)
		name = strings.TrimSpace(name)

		if curr.Contents != "" {

			doIndent(depth)
			proc(name)
			proc(" ")

			if quot {
				proc("\"")
			}

			str := curr.Contents[:]
			if HasBadSpace(str) {
				str = CleanupBadSpaces(str)
			}
			if IsNotASCII(str) {
				str = TransformAccents(str, false, false)
			}
			if HasAdjacentSpaces(str) {
				str = CompressRunsOfSpaces(str)
			}
			str = afix.Replace(str)
			proc(str)

			if quot {
				proc("\"")
			}

		} else {

			doIndent(depth)
			if show {
				proc(name)
				proc(" ")
			}
			if depth == 0 {
				proc("::= ")
			}
			proc("{\n")

			for chld := curr.Children; chld != nil; chld = chld.Next {
				// do not print comma after last child object in chain
				doASNtree(chld, depth+1, (chld.Next != nil))
			}

			doIndent(depth)
			proc("}")
		}

		if comma {
			proc(",")
		}
		proc("\n")
	}

	doASNtree(node, 0, false)
}

// printJSONtree prints JSON selected by -element "%"
func printJSONtree(node *XMLNode, proc func(string)) {

	// COPIED FROM printASNtree, MODIFICATIONS NOT YET COMPLETE

	if node == nil || proc == nil {
		return
	}

	// array to speed up indentation
	indentSpaces := []string{
		"",
		"  ",
		"    ",
		"      ",
		"        ",
		"          ",
		"            ",
		"              ",
		"                ",
		"                  ",
	}

	// indent a specified number of spaces
	doIndent := func(indt int) {
		i := indt
		for i > 9 {
			proc("                    ")
			i -= 10
		}
		if i < 0 {
			return
		}
		proc(indentSpaces[i])
	}

	// doJSONtree recursive definition
	var doJSONtree func(*XMLNode, int, bool)

	doJSONtree = func(curr *XMLNode, depth int, comma bool) {

		// suppress if it would be an empty self-closing tag
		if !IsNotJustWhitespace(curr.Attributes) && curr.Contents == "" && curr.Children == nil {
			return
		}

		name := curr.Name
		if name == "" {
			name = "_"
		}

		// just a hyphen   - unnamed brackets
		// leading hyphen  - array instead of object
		// trailing hyphen - unquoted value
		show := true
		array := false
		quot := true
		if name == "_" {
			show = false
		} else if strings.HasPrefix(name, "_") {
			array = true
		} else if strings.HasSuffix(name, "_") {
			name = strings.TrimSuffix(name, "_")
			quot = false
		}
		name = strings.Replace(name, "_", " ", -1)
		name = strings.TrimSpace(name)

		if curr.Contents != "" {

			doIndent(depth)
			proc("\"")
			proc(name)
			proc("\": ")

			if quot {
				proc("\"")
			}

			str := curr.Contents[:]
			if HasBadSpace(str) {
				str = CleanupBadSpaces(str)
			}
			if IsNotASCII(str) {
				str = TransformAccents(str, false, false)
			}
			if HasAdjacentSpaces(str) {
				str = CompressRunsOfSpaces(str)
			}
			proc(str)

			if quot {
				proc("\"")
			}

		} else {

			doIndent(depth)
			if show && depth > 0 {
				proc("\"")
				proc(name)
				proc("\": ")
			}
			if array {
				proc("[")
			} else {
				proc("{")
			}
			proc("\n")

			for chld := curr.Children; chld != nil; chld = chld.Next {
				// do not print comma after last child object in chain
				doJSONtree(chld, depth+1, (chld.Next != nil))
			}

			doIndent(depth)
			if array {
				proc("]")
			} else {
				proc("}")
			}
		}

		if comma {
			proc(",")
		}
		proc("\n")
	}

	doJSONtree(node, 0, false)
}

var (
	rlock sync.Mutex
	replx map[string]*regexp.Regexp
)

// processClause handles comma-separated -element arguments
func processClause(
	curr *XMLNode,
	stages []*Step,
	mask string,
	prev string,
	pfx string,
	sfx string,
	plg string,
	sep string,
	def string,
	reg string,
	exp string,
	wrp bool,
	status OpType,
	index int,
	level int,
	variables map[string]string,
	transform map[string]string,
	srchr *FSMSearcher,
	histogram map[string]int,
) (string, bool) {

	if curr == nil || stages == nil {
		return "", false
	}

	if replx == nil {
		rlock.Lock()
		if replx == nil {
			replx = make(map[string]*regexp.Regexp)
		}
		rlock.Unlock()
	}

	// processElement handles individual -element constructs
	processElement := func(acc func(string)) {

		if acc == nil {
			return
		}

		// element names combined with commas are treated as a prefix-separator-suffix group
		for _, stage := range stages {

			stat := stage.Type
			item := stage.Value
			prnt := stage.Parent
			match := stage.Match
			attrib := stage.Attrib
			typL := stage.TypL
			strL := stage.StrL
			intL := stage.IntL
			typR := stage.TypR
			strR := stage.StrR
			intR := stage.IntR
			norm := stage.Norm
			wildcard := stage.Wild
			unescape := stage.Unesc

			// exploreElements is a wrapper for ExploreElements, obtaining most arguments as closures
			exploreElements := func(proc func(string, int)) {
				ExploreElements(curr, mask, prnt, match, attrib, wildcard, unescape, level, proc)
			}

			// sendSlice applies optional [min:max] range restriction and sends result to accumulator
			sendSlice := func(str string) {

				// handle usual situation with no range first
				if norm {
					if wrp && stat != REPLACE {
						str = html.EscapeString(str)
					}
					acc(str)
					return
				}

				// check for [after|before] variant
				if typL == STRINGRANGE || typR == STRINGRANGE {
					if strL != "" {
						// use case-insensitive test
						strL = strings.ToUpper(strL)
						idx := strings.Index(strings.ToUpper(str), strL)
						if idx < 0 {
							// specified substring must be present in original string
							return
						}
						ln := len(strL)
						// remove leading text
						str = str[idx+ln:]
					}
					if strR != "" {
						strR = strings.ToUpper(strR)
						idx := strings.Index(strings.ToUpper(str), strR)
						if idx < 0 {
							// specified substring must be present in remaining string
							return
						}
						// remove trailing text
						str = str[:idx]
					}
					if str != "" {
						if wrp && stat != REPLACE {
							str = html.EscapeString(str)
						}
						acc(str)
					}
					return
				}

				min := 0
				max := 0

				// slice arguments use variable value +- adjustment or integer constant
				if typL == VARIABLERANGE {
					if strL == "" {
						return
					}
					lft, ok := variables[strL]
					if !ok {
						return
					}
					val, err := strconv.Atoi(lft)
					if err != nil {
						return
					}
					// range argument values are inclusive and 1-based, decrement variable start +- offset to use in slice
					min = val + intL - 1
				} else if typL == INTEGERRANGE {
					// range argument values are inclusive and 1-based, decrement literal start to use in slice
					min = intL - 1
				}
				if typR == VARIABLERANGE {
					if strR == "" {
						return
					}
					rgt, ok := variables[strR]
					if !ok {
						return
					}
					val, err := strconv.Atoi(rgt)
					if err != nil {
						return
					}
					if val+intR < 0 {
						// negative value is 1-based inset from end of string (undocumented)
						max = len(str) + val + intR + 1
					} else {
						max = val + intR
					}
				} else if typR == INTEGERRANGE {
					if intR < 0 {
						// negative max is inset from end of string (undocumented)
						max = len(str) + intR + 1
					} else {
						max = intR
					}
				}

				doRevComp := false
				doUpCase := false
				if status == NUCLEIC {
					// -nucleic uses direction of range to decide between forward strand or reverse complement
					if min+1 > max {
						min, max = max-1, min+1
						doRevComp = true
					}
					doUpCase = true
				}

				// numeric range now calculated, apply slice to string
				if min == 0 && max == 0 {
					if doRevComp {
						str = ReverseComplement(str)
					}
					if doUpCase {
						str = strings.ToUpper(str)
					}
					if wrp && stat != REPLACE {
						str = html.EscapeString(str)
					}
					acc(str)
				} else if max == 0 {
					if min > 0 && min < len(str) {
						str = str[min:]
						if str != "" {
							if doRevComp {
								str = ReverseComplement(str)
							}
							if doUpCase {
								str = strings.ToUpper(str)
							}
							if wrp && stat != REPLACE {
								str = html.EscapeString(str)
							}
							acc(str)
						}
					}
				} else if min == 0 {
					if max > 0 && max <= len(str) {
						str = str[:max]
						if str != "" {
							if doRevComp {
								str = ReverseComplement(str)
							}
							if doUpCase {
								str = strings.ToUpper(str)
							}
							if wrp && stat != REPLACE {
								str = html.EscapeString(str)
							}
							acc(str)
						}
					}
				} else {
					if min < max && min > 0 && max <= len(str) {
						str = str[min:max]
						if str != "" {
							if doRevComp {
								str = ReverseComplement(str)
							}
							if doUpCase {
								str = strings.ToUpper(str)
							}
							if wrp && stat != REPLACE {
								str = html.EscapeString(str)
							}
							acc(str)
						}
					}
				}
			}

			switch stat {
			case ELEMENT:
				exploreElements(func(str string, lvl int) {
					if str != "" {
						sendSlice(str)
					}
				})
			case VARIABLE, ACCUMULATOR:
				// use value of stored variable
				val, ok := variables[match]
				if ok {
					sendSlice(val)
				}
			case NUM, COUNT:
				count := 0

				exploreElements(func(str string, lvl int) {
					count++
				})

				// number of element objects
				val := strconv.Itoa(count)
				acc(val)
			case LENGTH:
				length := 0

				exploreElements(func(str string, lvl int) {
					length += len(str)
				})

				// length of element strings
				val := strconv.Itoa(length)
				acc(val)
			case DEPTH:
				exploreElements(func(str string, lvl int) {
					// depth of each element in scope
					val := strconv.Itoa(lvl)
					acc(val)
				})
			case INDEX:
				// -element "+" prints index of current XML object
				val := strconv.Itoa(index)
				acc(val)
			case INC:
				// -inc, or component of -0-based, -1-based, or -ucsc-based
				exploreElements(func(str string, lvl int) {
					if str != "" {
						num, err := strconv.Atoi(str)
						if err == nil {
							// increment value
							num++
							val := strconv.Itoa(num)
							acc(val)
						}
					}
				})
			case DEC:
				// -dec, or component of -0-based, -1-based, or -ucsc-based
				exploreElements(func(str string, lvl int) {
					if str != "" {
						num, err := strconv.Atoi(str)
						if err == nil {
							// decrement value
							num--
							val := strconv.Itoa(num)
							acc(val)
						}
					}
				})
			case QUESTION:
				acc(curr.Name)
			case TILDE:
				acc(curr.Contents)
			case DOT:
				// -element "." prints current XML subtree as ASN.1
				var buffer strings.Builder

				printASNtree(curr,
					func(str string) {
						if str != "" {
							buffer.WriteString(str)
						}
					})

				txt := buffer.String()
				if txt != "" {
					if strings.HasSuffix(txt, "\n") {
						txt = strings.TrimSuffix(txt, "\n")
					}
					acc(txt)
				}
			case PRCNT:
				// -element "%" prints current XML subtree as JSON
				var buffer strings.Builder

				printJSONtree(curr,
					func(str string) {
						if str != "" {
							buffer.WriteString(str)
						}
					})

				txt := buffer.String()
				if txt != "" {
					if strings.HasSuffix(txt, "\n") {
						txt = strings.TrimSuffix(txt, "\n")
					}
					acc(txt)
				}
			case STAR:
				// -element "*" prints current XML subtree on a single line
				style := SINGULARITY
				printAttrs := true

				for _, ch := range item {
					if ch == '*' {
						style++
					} else if ch == '@' {
						printAttrs = false
					}
				}
				if style > WRAPPED {
					style = WRAPPED
				}
				if style < COMPACT {
					style = COMPACT
				}

				var buffer strings.Builder

				printXMLtree(curr, style, printAttrs,
					func(str string) {
						if str != "" {
							buffer.WriteString(str)
						}
					})

				txt := buffer.String()
				if txt != "" {
					acc(txt)
				}
			case DOLLAR:
				for chld := curr.Children; chld != nil; chld = chld.Next {
					acc(chld.Name)
				}
			case ATSIGN:
				if curr.Attributes != "" && curr.Attribs == nil {
					curr.Attribs = ParseAttributes(curr.Attributes)
				}
				for i := 0; i < len(curr.Attribs)-1; i += 2 {
					acc(curr.Attribs[i])
				}
			default:
				exploreElements(func(str string, lvl int) {
					if str != "" {
						sendSlice(str)
					}
				})
			}
		}
	}

	ok := false

	// format results in buffer
	var buffer strings.Builder

	buffer.WriteString(prev)
	buffer.WriteString(plg)
	buffer.WriteString(pfx)
	between := ""

	switch status {
	case ELEMENT:
		processElement(func(str string) {
			if str != "" {
				ok = true
				buffer.WriteString(between)
				buffer.WriteString(str)
				between = sep
			}
		})

	case FIRST:
		single := ""

		processElement(func(str string) {
			ok = true
			if single == "" {
				single = str
			}
		})

		if single != "" {
			buffer.WriteString(between)
			buffer.WriteString(single)
			between = sep
		}

	case LAST:
		single := ""

		processElement(func(str string) {
			ok = true
			single = str
		})

		if single != "" {
			buffer.WriteString(between)
			buffer.WriteString(single)
			between = sep
		}

	case BACKWARD:
		var arry []string

		processElement(func(str string) {
			if str != "" {
				ok = true
				arry = append(arry, str)
			}
		})

		if ok {
			for i := len(arry) - 1; i >= 0; i-- {
				buffer.WriteString(between)
				buffer.WriteString(arry[i])
				between = sep
			}
		}

	case ENCODE:
		processElement(func(str string) {
			if str != "" {
				ok = true
				if !wrp {
					str = html.EscapeString(str)
				}
				buffer.WriteString(between)
				buffer.WriteString(str)
				between = sep
			}
		})

	case DECODE:
		// superseded by transmute -decode64 (undocumented)
		processElement(func(str string) {
			if str != "" {
				txt, err := base64.StdEncoding.DecodeString(str)
				if err == nil {
					ok = true
					buffer.WriteString(between)
					buffer.WriteString(string(txt))
					between = sep
				}
			}
		})

	case UPPER:
		processElement(func(str string) {
			if str != "" {
				ok = true
				str = strings.ToUpper(str)
				buffer.WriteString(between)
				buffer.WriteString(str)
				between = sep
			}
		})

	case LOWER:
		processElement(func(str string) {
			if str != "" {
				ok = true
				str = strings.ToLower(str)
				buffer.WriteString(between)
				buffer.WriteString(str)
				between = sep
			}
		})

	case CHAIN:
		processElement(func(str string) {
			if str != "" {
				ok = true
				str = strings.Replace(str, " ", "_", -1)
				buffer.WriteString(between)
				buffer.WriteString(str)
				between = sep
			}
		})

	case TITLE:
		processElement(func(str string) {
			if str != "" {
				ok = true
				str = strings.ToLower(str)
				// str = strings.Title(str)
				csr := cases.Title(language.English)
				str = csr.String(str)
				buffer.WriteString(between)
				buffer.WriteString(str)
				between = sep
			}
		})

	case MIRROR:
		processElement(func(str string) {
			if str != "" {
				ok = true
				runes := []rune(str)
				for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
					runes[i], runes[j] = runes[j], runes[i]
				}
				str = string(runes)
				buffer.WriteString(between)
				buffer.WriteString(str)
				between = sep
			}
		})

	case ALNUM:
		processElement(func(str string) {
			if str != "" {
				words := strings.FieldsFunc(str, func(c rune) bool {
					// split at non-alphanumeric characters
					return (!unicode.IsLetter(c) && !unicode.IsDigit(c)) || c > 127
				})
				str = strings.Join(words, " ")
				str = strings.TrimSpace(str)
				str = CompressRunsOfSpaces(str)
				if str != "" {
					ok = true
					buffer.WriteString(between)
					buffer.WriteString(str)
					between = sep
				}
			}
		})

	case BASIC, PLAIN, SIMPLE, AUTHOR, PROSE:
		processElement(func(str string) {
			if str != "" {
				ok = true
				str = strings.Replace(str, "\n", " ", -1)
				if status == PLAIN {
					str = RemoveEmbeddedMarkup(str)
					str = TransformAccents(str, false, false)
				} else if status == SIMPLE {
					str = TransformAccents(str, true, false)
				} else if status == AUTHOR {
					str = FixMisusedLetters(str, false, true, false)
					str = TransformAccents(str, false, false)
					// convert numeric encoding to apostrophe
					str = strings.Replace(str, "&#39;", "'", -1)
					// remove space following apostrophe
					str = strings.Replace(str, "' ", "'", -1)
					// but leave apostrophe present in result
				} else if status == PROSE {
					if wrp {
						str = html.UnescapeString(str)
					}
					str = RemoveEmbeddedMarkup(str)
					str = TransformAccents(str, false, false)
					str = FixMisusedLetters(str, true, false, true)
					str = TransformAccents(str, false, false)
					if wrp {
						str = html.EscapeString(str)
					}
				}

				if HasUnicodeMarkup(str) {
					str = RepairUnicodeMarkup(str, SPACE)
				}
				if HasAngleBracket(str) {
					str = RepairTableMarkup(str, SPACE)
					str = RemoveHTMLDecorations(str)
					if wrp {
						str = encodeAngleBracketsAndAmpersand(str)
					}
				}
				if HasBadSpace(str) {
					str = CleanupBadSpaces(str)
				}
				if HasAdjacentSpaces(str) {
					str = CompressRunsOfSpaces(str)
				}
				if HasExtraSpaces(str) {
					str = RemoveExtraSpaces(str)
				}
				buffer.WriteString(between)
				buffer.WriteString(str)
				between = sep
			}
		})

	case ORDER:
		processElement(func(str string) {
			if str != "" {
				ok = true
				str = SortStringByWords(str)
				buffer.WriteString(between)
				buffer.WriteString(str)
				between = sep
			}
		})

	case YEAR:
		year := ""

		processElement(func(str string) {
			if str != "" && year == "" {
				words := strings.FieldsFunc(str, func(c rune) bool {
					return !unicode.IsDigit(c)
				})
				for _, item := range words {
					if len(item) == 4 && IsAllDigits(item) {
						year = item
						ok = true
						// only print integer for first year, e.g., PubDate/MedlineDate "2008 Dec-2009 Jan" is 2008
						break
					}
				}
			}
		})

		if year != "" {
			buffer.WriteString(between)
			buffer.WriteString(year)
			between = sep
		}

	case MONTH:
		month := ""

		processElement(func(str string) {
			if str != "" && month == "" {
				words := strings.FieldsFunc(str, func(c rune) bool {
					return !unicode.IsLetter(c)
				})
				for _, item := range words {
					item = strings.ToLower(item)
					val, found := monthTable[item]
					if found {
						month = strconv.Itoa(val)
						ok = true
						// only print integer for first month, e.g., PubDate/MedlineDate "2008 Dec-2009 Jan" is 12
						break
					}
				}
			}
		})

		if month != "" {
			buffer.WriteString(between)
			buffer.WriteString(month)
			between = sep
		}

	case DATE:
		// xtract -pattern PubmedArticle -unit "PubDate" -date "*"
		// xtract -pattern collection -unit date -date "*"
		year := ""
		month := ""
		day := ""

		extractBetweenTags := func(txt, tag string) string {

			if txt == "" || tag == "" {
				return ""
			}
			_, after, found := strings.Cut(txt, "<"+tag+">")
			if !found || after == "" {
				return ""
			}
			res, _, found := strings.Cut(after, "</"+tag+">")
			if !found || res == "" {
				return ""
			}
			return res
		}

		processElement(func(str string) {
			if str != "" {
				if strings.Contains(str, "MedlineDate") {

					words := strings.FieldsFunc(str, func(c rune) bool {
						return !unicode.IsDigit(c)
					})
					for _, item := range words {
						if len(item) == 4 && IsAllDigits(item) {
							year = item
							// only print integer for first year
							break
						}
					}
					if year != "" {
						words := strings.FieldsFunc(str, func(c rune) bool {
							return !unicode.IsLetter(c)
						})
						for _, item := range words {
							item = strings.ToLower(item)
							val, found := monthTable[item]
							if found {
								month = strconv.Itoa(val)
								// only print integer for first month
								break
							}
						}
					}
				} else if strings.Contains(str, "date") {

					str = html.UnescapeString(str)
					// <date>20201214</date>
					raw := extractBetweenTags(str, "date")
					if len(raw) == 8 {
						year = raw[0:4]
						month = raw[4:6]
						day = raw[6:8]
					} else if len(raw) == 6 {
						year = raw[0:4]
						month = raw[4:6]
					} else if len(raw) == 4 {
						year = raw[0:4]
					}

				} else if strings.Contains(str, "PubDate") {

					str = extractBetweenTags(str, "PubDate")
					items := strings.Split(str, " ")
					for _, itm := range items {
						if year == "" {
							year = itm
						} else if month == "" {
							month = itm
						} else if day == "" {
							day = itm
						}
					}
					if month != "" {
						if !IsAllDigits(month) {
							month = strings.ToLower(month)
							val, found := monthTable[month]
							if found {
								month = strconv.Itoa(val)
							}
						}
					}

				} else {

					year = extractBetweenTags(str, "Year")
					month = extractBetweenTags(str, "Month")
					if month != "" {
						if !IsAllDigits(month) {
							month = strings.ToLower(month)
							val, found := monthTable[month]
							if found {
								month = strconv.Itoa(val)
							}
						}
					}
					day = extractBetweenTags(str, "Day")
				}
			}
		})

		slash := "/"
		if reg == "/" && exp != "" {
			slash = exp
		}

		txt := ""
		if year != "" {
			buffer.WriteString(between)
			txt = year
			if month != "" {
				if len(month) == 1 {
					txt += slash + "0" + month
				} else {
					txt += slash + month
				}
				if day != "" {
					if len(day) == 1 {
						txt += slash + "0" + day
					} else {
						txt += slash + day
					}
				}
			}
			ok = true
		}

		if txt != "" {
			buffer.WriteString(between)
			buffer.WriteString(txt)
			between = sep
		}

	case PAGE:
		processElement(func(str string) {
			if str != "" {
				words := strings.FieldsFunc(str, func(c rune) bool {
					return !unicode.IsLetter(c) && !unicode.IsDigit(c)
				})
				if len(words) > 0 {
					firstPage := words[0]
					if firstPage != "" {
						ok = true
						buffer.WriteString(between)
						buffer.WriteString(firstPage)
						between = sep
					}
				}
			}
		})

	case AUTH:
		processElement(func(str string) {
			if str != "" {
				ok = true
				// convert GenBank author to searchable form
				str = GenBankToMedlineAuthors(str)
				buffer.WriteString(between)
				buffer.WriteString(str)
				between = sep
			}
		})

	case INITIALS:
		processElement(func(str string) {
			if str != "" {
				ok = true
				// convert given name to initials
				if len(str) != 2 || !unicode.IsUpper(rune(str[0])) || !unicode.IsUpper(rune(str[1])) {
					lft, rgt, found := strings.Cut(str, " ")
					if !found {
						lft, rgt, found = strings.Cut(str, "-")
					}
					if !found {
						lft, rgt, found = strings.Cut(str, ".")
					}
					if found && lft != "" && rgt != "" {
						str = lft[:1] + rgt[:1]
					} else {
						str = str[:1]
					}
				}
				str = strings.ToUpper(str)
				buffer.WriteString(between)
				buffer.WriteString(str)
				between = sep
			}
		})

	case JOUR:
		processElement(func(str string) {
			if str != "" {
				ok = true
				str = CleanJournal(str)
				buffer.WriteString(between)
				buffer.WriteString(str)
				between = sep
			}
		})

	case PROP:
		processElement(func(str string) {
			if str != "" {
				prop, fnd := propertyTable[str]
				if fnd {
					ok = true
					buffer.WriteString(between)
					buffer.WriteString(prop)
					between = sep
				} else {
					ok = true
					buffer.WriteString(between)
					buffer.WriteString("Other")
					between = sep
				}
			}
		})

	case TRIM:
		processElement(func(str string) {
			if str != "" {
				str = strings.TrimPrefix(str, " ")
				str = strings.TrimSuffix(str, " ")
				str = strings.TrimSpace(str)
				if strings.HasPrefix(str, "0") {
					// also trim leading zeros
					str = strings.TrimPrefix(str, "0")
					if str == "" {
						// but leave one if was only zeros
						str = "0"
					}
				}
				if str != "" {
					ok = true
					buffer.WriteString(between)
					buffer.WriteString(str)
					between = sep
				}
			}
		})

	case WCT:
		count := 0

		processElement(func(str string) {
			if str != "" {

				words := strings.FieldsFunc(str, func(c rune) bool {
					return !unicode.IsLetter(c) && !unicode.IsDigit(c)
				})
				for _, item := range words {
					item = strings.ToLower(item)
					if deStop {
						// exclude stop words from count
						if IsStopWord(item) {
							continue
						}
					}
					if doStem {
						item = porter2.Stem(item)
						item = strings.TrimSpace(item)
					}
					if item == "" {
						continue
					}
					count++
					ok = true
				}
			}
		})

		if ok {
			// total number of words
			val := strconv.Itoa(count)
			buffer.WriteString(between)
			buffer.WriteString(val)
			between = sep
		}

	case DOI:
		processElement(func(str string) {
			if str != "" {
				ok = true
				str = strings.TrimPrefix(str, "doi:")
				str = strings.TrimSpace(str)
				str = strings.TrimPrefix(str, "/")
				str = strings.TrimPrefix(str, "https://doi.org/")
				str = strings.TrimPrefix(str, "http://dx.doi.org/")
				str = url.QueryEscape(str)
				str = "https://doi.org/" + str
				buffer.WriteString(between)
				buffer.WriteString(str)
				between = sep
			}
		})

	case TRANSLATE:
		processElement(func(str string) {
			if str != "" {
				txt, found := transform[str]
				if found {
					// require successful mapping
					ok = true
					buffer.WriteString(between)
					buffer.WriteString(txt)
					between = sep
				}
			}
		})

	case REPLACE:
		processElement(func(str string) {
			if str != "" {
				rlock.Lock()
				re, found := replx[str]
				if !found {
					re, found = replx[str]
					if !found {
						nw, err := regexp.Compile(reg)
						if err == nil {
							replx[str] = nw
							re = nw
						}
					}
				}
				rlock.Unlock()
				if re != nil {
					txt := re.ReplaceAllString(str, exp)
					if txt != "" {
						ok = true
						buffer.WriteString(between)
						// wrp-directed EscapeString was delayed for REPLACE
						if wrp {
							txt = html.EscapeString(txt)
						}
						buffer.WriteString(txt)
						between = sep
					}
				}
			}
		})

	case VALUE:
		processElement(func(str string) {
			if str != "" {
				ok = true
				buffer.WriteString(between)
				buffer.WriteString(str)
				between = sep
			}
		})

	case NUM:
		processElement(func(str string) {
			if str != "" {
				ok = true
				buffer.WriteString(between)
				buffer.WriteString(str)
				between = sep
			}
		})

	case INC:
		processElement(func(str string) {
			if str != "" {
				ok = true
				buffer.WriteString(between)
				buffer.WriteString(str)
				between = sep
			}
		})

	case DEC:
		processElement(func(str string) {
			if str != "" {
				ok = true
				buffer.WriteString(between)
				buffer.WriteString(str)
				between = sep
			}
		})

	case ZEROBASED:
		processElement(func(str string) {
			if str != "" {
				ok = true
				buffer.WriteString(between)
				buffer.WriteString(str)
				between = sep
			}
		})

	case ONEBASED:
		processElement(func(str string) {
			if str != "" {
				ok = true
				buffer.WriteString(between)
				buffer.WriteString(str)
				between = sep
			}
		})

	case UCSCBASED:
		processElement(func(str string) {
			if str != "" {
				ok = true
				buffer.WriteString(between)
				buffer.WriteString(str)
				between = sep
			}
		})

	case NUCLEIC:
		processElement(func(str string) {
			if str != "" {
				ok = true
				buffer.WriteString(between)
				buffer.WriteString(str)
				between = sep
			}
		})

	case LEN:
		length := 0

		processElement(func(str string) {
			length += len(str)
			ok = true
		})

		if ok {
			// length of element strings
			val := strconv.Itoa(length)
			buffer.WriteString(between)
			buffer.WriteString(val)
			between = sep
		}

	case SUM:
		sum := 0

		processElement(func(str string) {
			value, err := strconv.Atoi(str)
			if err == nil {
				sum += value
				ok = true
			}
		})

		if ok {
			// sum of element values
			val := strconv.Itoa(sum)
			buffer.WriteString(between)
			buffer.WriteString(val)
			between = sep
		}

	case ACC:
		sum := 0

		processElement(func(str string) {
			value, err := strconv.Atoi(str)
			if err == nil {
				sum += value
				ok = true
				// running sum of element values
				val := strconv.Itoa(sum)
				buffer.WriteString(between)
				buffer.WriteString(val)
				between = sep
			}
		})

	case MIN:
		min := 0

		processElement(func(str string) {
			value, err := strconv.Atoi(str)
			if err == nil {
				if !ok || value < min {
					min = value
				}
				ok = true
			}
		})

		if ok {
			// minimum of element values
			val := strconv.Itoa(min)
			buffer.WriteString(between)
			buffer.WriteString(val)
			between = sep
		}

	case MAX:
		max := 0

		processElement(func(str string) {
			value, err := strconv.Atoi(str)
			if err == nil {
				if !ok || value > max {
					max = value
				}
				ok = true
			}
		})

		if ok {
			// maximum of element values
			val := strconv.Itoa(max)
			buffer.WriteString(between)
			buffer.WriteString(val)
			between = sep
		}

	case SUB:
		first := 0
		second := 0
		count := 0

		processElement(func(str string) {
			value, err := strconv.Atoi(str)
			if err == nil {
				count++
				if count == 1 {
					first = value
				} else if count == 2 {
					second = value
				}
			}
		})

		if count == 2 {
			// must have exactly 2 elements
			ok = true
			// difference of element values
			val := strconv.Itoa(first - second)
			buffer.WriteString(between)
			buffer.WriteString(val)
			between = sep
		}

	case AVG:
		sum := 0
		count := 0

		processElement(func(str string) {
			value, err := strconv.Atoi(str)
			if err == nil {
				sum += value
				count++
				ok = true
			}
		})

		if ok {
			// average of element values
			avg := int(float64(sum) / float64(count))
			val := strconv.Itoa(avg)
			buffer.WriteString(between)
			buffer.WriteString(val)
			between = sep
		}

	case DEV:
		count := 0
		mean := 0.0
		m2 := 0.0

		processElement(func(str string) {
			value, err := strconv.Atoi(str)
			if err == nil {
				// Welford algorithm for one-pass standard deviation
				count++
				x := float64(value)
				delta := x - mean
				mean += delta / float64(count)
				m2 += delta * (x - mean)
			}
		})

		if count > 1 {
			// must have at least 2 elements
			ok = true
			// standard deviation of element values
			vrc := m2 / float64(count-1)
			dev := int(math.Sqrt(vrc))
			val := strconv.Itoa(dev)
			buffer.WriteString(between)
			buffer.WriteString(val)
			between = sep
		}

	case MED:
		var arry []int
		count := 0

		processElement(func(str string) {
			value, err := strconv.Atoi(str)
			if err == nil {
				arry = append(arry, value)
				count++
				ok = true
			}
		})

		if ok {
			// median of element values
			sort.Slice(arry, func(i, j int) bool { return arry[i] < arry[j] })
			med := arry[count/2]
			val := strconv.Itoa(med)
			buffer.WriteString(between)
			buffer.WriteString(val)
			between = sep
		}

	case MUL:
		first := 0
		second := 0
		count := 0

		processElement(func(str string) {
			value, err := strconv.Atoi(str)
			if err == nil {
				count++
				if count == 1 {
					first = value
				} else if count == 2 {
					second = value
				}
			}
		})

		if count == 2 {
			// must have exactly 2 elements
			ok = true
			// product of element values
			val := strconv.Itoa(first * second)
			buffer.WriteString(between)
			buffer.WriteString(val)
			between = sep
		}

	case DIV:
		first := 0
		second := 0
		count := 0

		processElement(func(str string) {
			value, err := strconv.Atoi(str)
			if err == nil {
				count++
				if count == 1 {
					first = value
				} else if count == 2 {
					second = value
				}
			}
		})

		if count == 2 {
			// must have exactly 2 elements
			ok = true
			// quotient of element values
			val := strconv.Itoa(first / second)
			buffer.WriteString(between)
			buffer.WriteString(val)
			between = sep
		}

	case MOD:
		first := 0
		second := 0
		count := 0

		processElement(func(str string) {
			value, err := strconv.Atoi(str)
			if err == nil {
				count++
				if count == 1 {
					first = value
				} else if count == 2 {
					second = value
				}
			}
		})

		if count == 2 {
			// must have exactly 2 elements
			ok = true
			// modulus of element values
			val := strconv.Itoa(first % second)
			buffer.WriteString(between)
			buffer.WriteString(val)
			between = sep
		}

	case LG2, LGE, LOG:
		// return logarithm truncated to integer (undocumented)
		processElement(func(str string) {
			value, err := strconv.Atoi(str)
			if err == nil && value > 0 {
				lg := float64(0)
				if status == LG2 {
					lg = math.Log2(float64(value))
				} else if status == LGE {
					lg = math.Log(float64(value))
				} else if status == LOG {
					lg = math.Log10(float64(value))
				}
				dec, _ := math.Modf(lg)
				val := strconv.Itoa(int(dec))
				buffer.WriteString(between)
				buffer.WriteString(val)
				between = sep
				ok = true
			}
		})

	case BIN:
		processElement(func(str string) {
			num, err := strconv.Atoi(str)
			if err == nil {
				// convert to binary representation
				val := strconv.FormatInt(int64(num), 2)
				buffer.WriteString(between)
				buffer.WriteString(val)
				between = sep
				ok = true
			}
		})

	case OCT:
		processElement(func(str string) {
			num, err := strconv.Atoi(str)
			if err == nil {
				// convert to octal representation
				val := strconv.FormatInt(int64(num), 8)
				buffer.WriteString(between)
				buffer.WriteString(val)
				between = sep
				ok = true
			}
		})

	case HEX:
		processElement(func(str string) {
			num, err := strconv.Atoi(str)
			if err == nil {
				// convert to hexadecimal representation
				val := strconv.FormatInt(int64(num), 16)
				val = strings.ToUpper(val)
				// val := fmt.Sprintf("%X", num)
				buffer.WriteString(between)
				buffer.WriteString(val)
				between = sep
				ok = true
			}
		})

	case BIT:
		processElement(func(str string) {
			num, err := strconv.Atoi(str)
			if err == nil {
				// Kernighan algorithm for counting set bits
				count := 0
				for num != 0 {
					num &= num - 1
					count++
				}
				val := strconv.Itoa(count)
				buffer.WriteString(between)
				buffer.WriteString(val)
				between = sep
				ok = true
			}
		})

	case PAD:
		processElement(func(str string) {
			if str != "" {
				str = PadNumericID(str)
				buffer.WriteString(between)
				buffer.WriteString(str)
				between = sep
				ok = true
			}
		})

	case RAW:
		// for development and debugging of common XML cleanup functions (undocumented)
		processElement(func(str string) {
			if str != "" {
				buffer.WriteString(between)
				buffer.WriteString(str)
				between = sep
				ok = true
			}
		})

	case REVCOMP:
		processElement(func(str string) {
			if str != "" {
				ok = true
				buffer.WriteString(between)
				str = ReverseComplement(str)
				buffer.WriteString(str)
				between = sep
			}
		})

	case FASTA:
		processElement(func(str string) {
			for str != "" {
				mx := len(str)
				if mx > 70 {
					mx = 70
				}
				item := str[:mx]
				str = str[mx:]
				ok = true
				item = strings.ToUpper(item)
				buffer.WriteString(between)
				buffer.WriteString(item)
				between = sep
			}
		})

	case NCBI2NA:
		processElement(func(str string) {
			if str != "" {
				ok = true
				buffer.WriteString(between)
				str = Ncbi2naToIupac(str)
				buffer.WriteString(str)
				between = sep
			}
		})

	case NCBI4NA:
		processElement(func(str string) {
			if str != "" {
				ok = true
				buffer.WriteString(between)
				str = Ncbi4naToIupac(str)
				buffer.WriteString(str)
				between = sep
			}
		})

	case MOLWT:
		processElement(func(str string) {
			if str != "" {
				ok = true
				buffer.WriteString(between)
				str = ProteinWeight(str, true)
				buffer.WriteString(str)
				between = sep
			}
		})

	case HGVS:
		processElement(func(str string) {
			if str != "" {
				ok = true
				buffer.WriteString(between)
				str = ParseHGVS(str)
				buffer.WriteString(str)
				between = sep
			}
		})

	case INDICES, ARTICLE, ABSTRACT, PARAGRAPH, STEMMED:
		// build positional index with a choice of TITL, TIAB, ABST, TEXT, and STEM field names
		indices := make(map[string][]string)

		cumulative := 0

		var ilock sync.Mutex

		addItem := func(term string, position int) {

			// protect with mutex
			ilock.Lock()

			arry, found := indices[term]
			if !found {
				arry = make([]string, 0, 1)
			}
			arry = append(arry, strconv.Itoa(position))
			indices[term] = arry

			ilock.Unlock()
		}

		processElement(func(str string) {

			if str == "" {
				return
			}

			if str == "[Not Available]." {
				return
			}

			// remove parentheses to keep bracketed subscripts
			/*
				var (
					buffer []rune
					prev   rune
					inside bool
				)
				for _, ch := range str {
					if ch == '(' && prev != ' ' {
						inside = true
					} else if ch == ')' && inside {
						inside = false
					} else {
						buffer = append(buffer, ch)
					}
					prev = ch
				}
				str = string(buffer)
			*/

			if IsNotASCII(str) {
				str = FixMisusedLetters(str, true, false, true)
				str = TransformAccents(str, true, true)
				if HasUnicodeMarkup(str) {
					str = RepairUnicodeMarkup(str, SPACE)
				}
			}

			str = strings.ToLower(str)

			if HasBadSpace(str) {
				str = CleanupBadSpaces(str)
			}
			if HasAngleBracket(str) {
				str = RepairEncodedMarkup(str)
				str = RepairTableMarkup(str, SPACE)
				str = RepairScriptMarkup(str, SPACE)
				str = RepairMathMLMarkup(str, SPACE)
				// RemoveEmbeddedMarkup must be called before UnescapeString, which was suppressed in ExploreElements
				str = RemoveEmbeddedMarkup(str)
			}

			if HasAmpOrNotASCII(str) {
				str = html.UnescapeString(str)
				str = strings.ToLower(str)
			}

			if HasAdjacentSpaces(str) {
				str = CompressRunsOfSpaces(str)
			}

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
				return (!unicode.IsLetter(c) && !unicode.IsDigit(c)) && c != ' ' && c != '_' || c > 127
			})

			// space replaces plus sign to separate runs of unpunctuated words
			phrases := strings.Join(clauses, " ")

			// break phrases into individual words
			words := strings.Fields(phrases)

			for _, item := range words {

				cumulative++

				// skip at site of punctuation break
				if item == "+" {
					continue
				}

				// skip terms that are all digits
				if IsAllDigitsOrPeriod(item) {
					continue
				}

				// optional stop word removal
				if deStop && IsStopWord(item) {
					continue
				}

				if status == STEMMED {
					// optionally apply stemming algorithm
					item = porter2.Stem(item)
					item = strings.TrimSpace(item)
				}

				// index single normalized term with positions
				addItem(item, cumulative)
				ok = true
			}

			// pad to avoid false positive proximity match of words in adjacent paragraphs
			rounded := ((cumulative + 99) / 100) * 100
			if rounded-cumulative < 20 {
				rounded += 100
			}
			cumulative = rounded
		})

		prepareIndices := func(label string) {

			if len(indices) < 1 {
				return
			}

			var arry []string

			for item := range indices {
				arry = append(arry, item)
			}

			sort.Slice(arry, func(i, j int) bool { return arry[i] < arry[j] })

			last := ""
			for _, item := range arry {
				item = strings.TrimSpace(item)
				if item == "" {
					continue
				}
				if item == last {
					// skip duplicate entry
					continue
				}
				buffer.WriteString("<")
				buffer.WriteString(label)
				if len(indices[item]) > 0 {
					buffer.WriteString(" pos=\"")
					attr := strings.Join(indices[item], ",")
					buffer.WriteString(attr)
					buffer.WriteString("\"")
				}
				buffer.WriteString(">")
				buffer.WriteString(item)
				buffer.WriteString("</")
				buffer.WriteString(label)
				buffer.WriteString(">")
				last = item
			}
		}

		if ok {
			label := "TEXT"

			switch status {
			case INDICES:
				label = "TIAB"
			case ARTICLE:
				label = "TITL"
			case ABSTRACT:
				label = "ABST"
			case PARAGRAPH:
				label = "TEXT"
			case STEMMED:
				label = "STEM"
			default:
				label = "TITL"
			}

			prepareIndices(label)
		}

	case TERMS:
		processElement(func(str string) {
			if str != "" {

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
					ok = true
					buffer.WriteString(between)
					buffer.WriteString(item)
					between = sep
				}
			}
		})

	case WORDS:
		processElement(func(str string) {
			if str != "" {

				words := strings.FieldsFunc(str, func(c rune) bool {
					return !unicode.IsLetter(c) && !unicode.IsDigit(c)
				})
				for _, item := range words {
					item = strings.ToLower(item)
					if deStop {
						if IsStopWord(item) {
							continue
						}
					}
					if doStem {
						item = porter2.Stem(item)
						item = strings.TrimSpace(item)
					}
					if item == "" {
						continue
					}
					ok = true
					buffer.WriteString(between)
					buffer.WriteString(item)
					between = sep
				}
			}
		})

	case PAIRS, PAIRX:
		processElement(func(str string) {
			if str != "" {

				doSingle := (status == PAIRX)

				if doSingle {
					str = PrepareForIndexing(str, true, false, true, true, true)
				}

				// break clauses at punctuation other than space, and at non-ASCII characters
				clauses := strings.FieldsFunc(str, func(c rune) bool {
					return (!unicode.IsLetter(c) && !unicode.IsDigit(c)) && c != ' ' || c > 127
				})

				// plus sign separates runs of unpunctuated words
				phrases := strings.Join(clauses, " + ")

				// break phrases into individual words
				words := strings.FieldsFunc(phrases, func(c rune) bool {
					return !unicode.IsLetter(c) && !unicode.IsDigit(c)
				})

				// word pairs (or isolated singletons) separated by stop words
				if len(words) > 1 {
					past := ""
					run := 0
					for _, item := range words {
						if item == "+" {
							if doSingle && run == 1 && past != "" {
								ok = true
								buffer.WriteString(between)
								buffer.WriteString(past)
								between = sep
							}
							past = ""
							run = 0
							continue
						}
						item = strings.ToLower(item)
						if deStop {
							if IsStopWord(item) {
								if doSingle && run == 1 && past != "" {
									ok = true
									buffer.WriteString(between)
									buffer.WriteString(past)
									between = sep
								}
								past = ""
								run = 0
								continue
							}
						}
						if doStem {
							item = porter2.Stem(item)
							item = strings.TrimSpace(item)
						}
						if item == "" {
							past = ""
							continue
						}
						if past != "" {
							ok = true
							buffer.WriteString(between)
							buffer.WriteString(past + " " + item)
							between = sep
						}
						past = item
						run++
					}
					if doSingle && run == 1 && past != "" {
						ok = true
						buffer.WriteString(between)
						buffer.WriteString(past)
						between = sep
					}
				}
			}
		})

	case REVERSE:
		processElement(func(str string) {
			if str != "" {

				words := strings.FieldsFunc(str, func(c rune) bool {
					return !unicode.IsLetter(c) && !unicode.IsDigit(c)
				})
				for lf, rt := 0, len(words)-1; lf < rt; lf, rt = lf+1, rt-1 {
					words[lf], words[rt] = words[rt], words[lf]
				}
				for _, item := range words {
					item = strings.ToLower(item)
					if deStop {
						if IsStopWord(item) {
							continue
						}
					}
					if doStem {
						item = porter2.Stem(item)
						item = strings.TrimSpace(item)
					}
					if item == "" {
						continue
					}
					ok = true
					buffer.WriteString(between)
					buffer.WriteString(item)
					between = sep
				}
			}
		})

	case LETTERS:
		processElement(func(str string) {
			if str != "" {
				for _, ch := range str {
					ok = true
					buffer.WriteString(between)
					buffer.WriteRune(ch)
					between = sep
				}
			}
		})

	case CLAUSES:
		processElement(func(str string) {
			if str != "" {

				clauses := strings.FieldsFunc(str, func(c rune) bool {
					return c == '.' || c == ',' || c == ';' || c == ':'
				})
				for _, item := range clauses {
					item = strings.ToLower(item)
					item = strings.TrimSpace(item)
					if item == "" {
						continue
					}
					ok = true
					buffer.WriteString(between)
					buffer.WriteString(item)
					between = sep
				}
			}
		})

	case MESHCODE:
		var code []string
		var tree []string

		processElement(func(str string) {
			if str != "" {
				txt, found := transform[str]
				str = strings.ToLower(str)
				code = append(code, str)
				ok = true

				if !found {
					return
				}
				txt = strings.ToLower(txt)
				txt = strings.Replace(txt, ".", "_", -1)
				codes := strings.FieldsFunc(txt, func(c rune) bool {
					return c == ','
				})
				for _, item := range codes {
					ch := item[0]
					if item == "" {
						continue
					}
					switch ch {
					case 'a', 'c', 'd', 'e', 'f', 'g', 'z':
						tree = append(tree, item)
					default:
					}
				}
			}
		})

		if len(code) > 1 {
			sort.Slice(code, func(i, j int) bool { return code[i] < code[j] })
		}
		if len(tree) > 1 {
			sort.Slice(tree, func(i, j int) bool { return tree[i] < tree[j] })
		}

		last := ""
		for _, item := range code {
			if item == last {
				// skip duplicate entry
				continue
			}
			buffer.WriteString("<CODE>")
			buffer.WriteString(item)
			buffer.WriteString("</CODE>")
			last = item
		}

		last = ""
		for _, item := range tree {
			if item == last {
				// skip duplicate entry
				continue
			}
			buffer.WriteString("<TREE>")
			buffer.WriteString(item)
			buffer.WriteString("</TREE>")
			last = item
		}

	case MATRIX:
		var arry []string

		processElement(func(str string) {
			if str != "" {
				txt, found := transform[str]
				if found {
					str = txt
				}
				arry = append(arry, str)
				ok = true
			}
		})

		if len(arry) > 1 {
			sort.Slice(arry, func(i, j int) bool { return arry[i] < arry[j] })

			for i, frst := range arry {
				for j, scnd := range arry {
					if i == j {
						continue
					}
					buffer.WriteString(between)
					buffer.WriteString(frst)
					buffer.WriteString("\t")
					buffer.WriteString(scnd)
					between = "\n"
				}
			}
		}

	case CLASSIFY:
		processElement(func(str string) {
			if str != "" {
				kywds := make(map[string]bool)

				// search for whole word or whole phrase substrings
				srchr.Search(str[:],
					func(str, pat string, pos int) bool {
						mtch := strings.TrimSpace(pat)
						rslt := transform[mtch]
						if rslt != "" {
							items := strings.Split(rslt, ",")
							for _, itm := range items {
								tag, val := SplitInTwoRight(itm, ":")
								txt := val
								if tag != "" {
									txt = "<" + tag + ">" + val + "</" + tag + ">"
								}
								kywds[txt] = true
							}
						}
						return true
					})

				var keys []string
				for ky := range kywds {
					keys = append(keys, ky)
				}
				sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

				// record single copy of each match, in alphabetical order
				for _, key := range keys {
					ok = true
					buffer.WriteString(between)
					buffer.WriteString(key)
					between = sep
				}
			}
		})

	case HISTOGRAM:
		processElement(func(str string) {
			if str != "" {
				ok = true

				hlock.Lock()

				val := histogram[str]
				val++
				histogram[str] = val
				/*
					if strings.Contains(str, "&lt;sub&gt;") || strings.Contains(str, "<sub>") {
						val := histogram["sub"]
						val++
						histogram["sub"] = val
					}
					if strings.Contains(str, "&lt;sup&gt;") || strings.Contains(str, "<sup>") {
						val := histogram["sup"]
						val++
						histogram["sup"] = val
					}
					for _, ch := range str {
						if IsUnicodeSubsc(ch) {
							val := histogram["usb"]
							val++
							histogram["usb"] = val
							break
						}
					}
					for _, ch := range str {
						if IsUnicodeSuper(ch) {
							val := histogram["usp"]
							val++
							histogram["usp"] = val
							break
						}
					}
				*/
				/*
					for _, ch := range str {
						num := strconv.Itoa(int(ch))
						val := histogram[num]
						val++
						histogram[num] = val
					}
				*/

				hlock.Unlock()
			}
		})

	case ACCENTED:
		processElement(func(str string) {
			if str != "" {
				found := false
				for _, ch := range str {
					if ch > 127 {
						found = true
						break
					}
				}
				if found {
					ok = true
					buffer.WriteString(between)
					buffer.WriteString(str)
					between = sep
				}
			}
		})
		/*
			processElement(func(str string) {
				if str != "" {
					found := false
					for _, ch := range str {
						if ch > 127 {
							found = true
							break
						}
					}
					if found {
						ok = true
						buffer.WriteString(between)
						buffer.WriteString(str)
						between = sep
					}
				}
			})
		*/
		/*
			processElement(func(str string) {
				if str != "" {
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
						found := false
						for _, ch := range item {
							if ch > 127 {
								found = true
								break
							}
						}
						if found {
							ok = true
							after := TransformAccents(item, true, false)
							for _, c := range item {
								if c > 127 {
									tg := fmt.Sprintf("%d\t%U\t%s\t%s\t%s\n", c, c, string(c), item, after)
									buffer.WriteString(tg)
								}
							}
						}
					}
				}
			})
		*/

	case TEST:
		suffix := ""
		if reg == "" && exp != "" {
			suffix = " in " + exp
		}

		processElement(func(str string) {
			if str != "" {
				if HasCombiningAccent(str[:]) {
					ok = true
					buffer.WriteString(between)
					buffer.WriteString("Combining Accent" + suffix)
					between = sep
				}
				if HasInvisibleUnicode(str[:]) {
					ok = true
					buffer.WriteString(between)
					buffer.WriteString("Invisible Unicode" + suffix)
					between = sep
				}
			}
		})

	case SCAN:
		// for identification of records with current data issues of interest (undocumented)
		processElement(func(str string) {
			if str != "" {
				for _, ch := range str {
					if ch < 128 {
						continue
					}
					if ch == 223 {
						// sharp s 0x00DF
						ok = true
						buffer.WriteString(between)
						buffer.WriteString("0x00DF")
						between = sep
						break
					}
					if ch == 946 {
						// beta 0x03B2
						ok = true
						buffer.WriteString(between)
						buffer.WriteString("0x03B2")
						between = sep
						break
					}
				}
				/*
					for _, ch := range str {
						if ch < 128 {
							continue
						}
						if ch == 181 {
							// micro 0x00B5
							ok = true
							buffer.WriteString(between)
							buffer.WriteString("0x00B5")
							between = sep
							break
						}
						if ch == 956 {
							// mu 0x03BC
							ok = true
							buffer.WriteString(between)
							buffer.WriteString("0x03BC")
							between = sep
							break
						}
					}
				*/
				/*
					terms := strings.Fields(str)
					for _, item := range terms {
						hasmicro := false
						hasmu := false
						for _, ch := range item {
							if ch < 128 {
								continue
							}
							if ch == 181 {
								// micro 0x00B5
								hasmicro = true
							}
							if ch == 956 {
								// mu 0x03BC
								hasmu = true
							}
						}
						if hasmicro || hasmu {
							ok = true
							buffer.WriteString(between)
							if hasmicro {
								buffer.WriteString("r")
							}
							if hasmu {
								buffer.WriteString("u")
							}
							buffer.WriteString("\t")
							buffer.WriteString(item)
							between = sep
						}
					}
				*/
			}
		})

	default:
	}

	// use default value if nothing written
	if !ok && def != "" {
		ok = true
		buffer.WriteString(def)
	}

	buffer.WriteString(sfx)

	if !ok {
		return "", false
	}

	txt := buffer.String()

	return txt, true
}

// processInstructions performs extraction commands on a subset of XML
func processInstructions(
	commands []*Operation,
	curr *XMLNode,
	mask string,
	tab string,
	ret string,
	index int,
	level int,
	variables map[string]string,
	transform map[string]string,
	srchr *FSMSearcher,
	histogram map[string]int,
	accum func(string),
) (string, string) {

	if accum == nil {
		return tab, ret
	}

	sep := "\t"
	pfx := ""
	sfx := ""
	plg := ""
	elg := ""
	lst := ""

	def := ""

	reg := ""
	exp := ""

	col := "\t"
	lin := "\n"

	varname := ""
	isAccum := false

	wrp := false

	plain := true
	var currColor *color.Color

	// handles color, e.g., -color "red,bold", reset to plain by -color "-" (undocumented)
	printInColor := func(str string) {
		if plain || currColor == nil {
			accum(str)
		} else {
			tx := currColor.SprintFunc()
			tmp := fmt.Sprintf("%s", tx(str))
			accum(tmp)
		}
	}

	// process commands
	for _, op := range commands {

		str := op.Value

		switch op.Type {
		case ELEMENT:
			txt, ok := processClause(curr, op.Stages, mask, tab, pfx, sfx, plg, sep, def, reg, exp, wrp, op.Type, index, level, variables, transform, srchr, histogram)
			if ok {
				plg = ""
				lst = elg
				tab = col
				ret = lin
				if plain {
					accum(txt)
				} else {
					printInColor(txt)
				}
			}
		case HISTOGRAM:
			txt, ok := processClause(curr, op.Stages, mask, "", "", "", "", "", "", "", "", wrp, op.Type, index, level, variables, transform, srchr, histogram)
			if ok {
				accum(txt)
			}
		case TAB:
			col = str
		case RET:
			lin = str
		case PFX:
			pfx = str
		case SFX:
			sfx = str
		case SEP:
			sep = str
		case TAG:
			wrp = true
			fallthrough
		case LBL:
			lbl := str
			accum(tab)
			accum(plg)
			accum(pfx)
			if plain {
				accum(lbl)
			} else {
				printInColor(lbl)
			}
			accum(sfx)
			plg = ""
			lst = elg
			tab = col
			ret = lin
		case PFC:
			// preface clears previous tab and sets prefix in one command
			pfx = str
			fallthrough
		case CLR:
			// clear previous tab after the fact
			tab = ""
		case DEQ:
			// set queued tab after the fact
			tab = str
		case PLG:
			plg = str
		case ELG:
			elg = str
		case WRP:
			// shortcut to wrap elements in XML tags
			if str == "" || str == "-" {
				sep = "\t"
				pfx = ""
				sfx = ""
				plg = ""
				elg = ""
				wrp = false
				break
			}
			if strings.Index(str, ",") >= 0 {
				// -wrp with comma-separated arguments is deprecated, but supported for backward compatibility
				lft, rgt := SplitInTwoRight(str, ",")
				if lft != "" {
					plg = "<" + lft + ">"
					elg = "</" + lft + ">"
				}
				if rgt != "" && rgt != "-" {
					pfx = "<" + rgt + ">"
					sfx = "</" + rgt + ">"
					sep = sfx + pfx
				}
				wrp = true
				break
			}
			if strings.Index(str, "/") >= 0 {
				// supports slash-separated components
				pfx = ""
				sfx = ""
				sep = ""
				items := strings.Split(str, "/")
				for i := 0; i < len(items); i++ {
					pfx += "<" + items[i] + ">"
				}
				for i := len(items) - 1; i >= 0; i-- {
					sfx += "</" + items[i] + ">"
				}
				sep = sfx + pfx
				wrp = true
				break
			}
			// shortcut for strings.HasPrefix(str, "&") and strings.TrimPrefix(str, "&")
			if len(str) > 1 && str[0] == '&' {
				str = str[1:]
				// expand variable to get actual tag
				str = variables[str]
			}
			// single object name, no comma or slash
			pfx = "<" + str + ">"
			sfx = "</" + str + ">"
			sep = sfx + pfx
			wrp = true
		case ENC:
			// shortcut to mark unexpanded instances with XML tags
			plg = ""
			elg = ""
			// shortcut for strings.HasPrefix(str, "&") and strings.TrimPrefix(str, "&")
			if len(str) > 1 && str[0] == '&' {
				str = str[1:]
				// expand variable to get actual tag
				str = variables[str]
			}
			if str != "" && str != "-" {
				items := strings.Split(str, "/")
				for i := 0; i < len(items); i++ {
					plg += "<" + items[i] + ">"
				}
				for i := len(items) - 1; i >= 0; i-- {
					elg += "</" + items[i] + ">"
				}
			}
		case RST:
			pfx = ""
			sfx = ""
			plg = ""
			elg = ""
			sep = "\t"
			def = ""
			wrp = false
		case DEF:
			def = str
		case REG:
			reg = str
		case EXP:
			exp = str
		case COLOR:
			currColor = color.New()
			if str == "-" || str == "reset" || str == "clear" {
				plain = true
				break
			}
			plain = false
			items := strings.Split(str, ",")
			for _, itm := range items {
				switch itm {
				case "red":
					currColor.Add(color.FgRed)
				case "grn", "green":
					currColor.Add(color.FgGreen)
				case "blu", "blue":
					currColor.Add(color.FgBlue)
				case "blk", "black":
					currColor.Add(color.FgBlack)
				case "bld", "bold":
					currColor.Add(color.Bold)
				case "ital", "italic", "italics":
					currColor.Add(color.Italic)
				case "blink", "flash":
					currColor.Add(color.BlinkSlow)
				default:
					fmt.Fprintf(os.Stderr, "\nERROR: Unrecognized color argument '%s'\n", itm)
					os.Exit(1)
				}
			}
		case ACCUMULATOR:
			isAccum = true
			varname = str
		case VARIABLE:
			isAccum = false
			varname = str
		case VALUE:
			length := len(str)
			if length > 1 && str[0] == '(' && str[length-1] == ')' {
				// set variable from literal text inside parentheses, e.g., -COM "(, )"
				variables[varname] = str[1 : length-1]
				// -if "&VARIABLE" will succeed if set to blank with empty parentheses "()"
			} else if str == "" {
				// -if "&VARIABLE" will fail if initialized with empty string ""
				delete(variables, varname)
			} else {
				txt, ok := processClause(curr, op.Stages, mask, "", pfx, sfx, plg, sep, def, reg, exp, wrp, op.Type, index, level, variables, transform, srchr, histogram)
				if ok {
					plg = ""
					lst = elg
					if isAccum {
						if variables[varname] == "" {
							variables[varname] = txt
						} else {
							variables[varname] += sep + txt
						}
					} else {
						variables[varname] = txt
					}
				}
			}
			varname = ""
			isAccum = false
		default:
			txt, ok := processClause(curr, op.Stages, mask, tab, pfx, sfx, plg, sep, def, reg, exp, wrp, op.Type, index, level, variables, transform, srchr, histogram)
			if ok {
				plg = ""
				lst = elg
				tab = col
				ret = lin
				if plain {
					accum(txt)
				} else {
					printInColor(txt)
				}
			}
		}
	}

	if plain {
		accum(lst)
	} else {
		printInColor(lst)
	}

	return tab, ret
}

// CONDITIONAL EXECUTION USES -if AND -unless STATEMENT, WITH SUPPORT FOR DEPRECATED -match AND -avoid STATEMENTS

// conditionsAreSatisfied tests a set of conditions to determine if extraction should proceed
func conditionsAreSatisfied(conditions []*Operation, curr *XMLNode, mask string, index, level int, variables map[string]string) bool {

	if curr == nil {
		return false
	}

	required := 0
	observed := 0
	forbidden := 0
	isMatch := false
	isAvoid := false

	// matchFound tests individual conditions
	matchFound := func(stages []*Step) bool {

		if stages == nil || len(stages) < 1 {
			return false
		}

		stage := stages[0]

		var constraint *Step

		if len(stages) > 1 {
			constraint = stages[1]
		}

		status := stage.Type
		prnt := stage.Parent
		match := stage.Match
		attrib := stage.Attrib
		typL := stage.TypL
		strL := stage.StrL
		intL := stage.IntL
		typR := stage.TypR
		strR := stage.StrR
		intR := stage.IntR
		norm := stage.Norm
		wildcard := stage.Wild
		unescape := true

		found := false
		number := ""

		// exploreElements is a wrapper for ExploreElements, obtaining most arguments as closures
		exploreElements := func(proc func(string, int)) {
			ExploreElements(curr, mask, prnt, match, attrib, wildcard, unescape, level, proc)
		}

		// test string or numeric constraints
		testConstraint := func(str string) bool {

			if str == "" || constraint == nil {
				return false
			}

			val := constraint.Value
			stat := constraint.Type

			switch stat {
			case EQUALS, CONTAINS, INCLUDES, ISWITHIN, STARTSWITH, ENDSWITH, ISNOT, ISBEFORE, ISAFTER, MATCHES, RESEMBLES:
				// substring test on element values
				str = strings.ToUpper(str)
				val = strings.ToUpper(val)

				switch stat {
				case EQUALS:
					if str == val {
						return true
					}
				case CONTAINS:
					if strings.Contains(str, val) {
						return true
					}
				case INCLUDES:
					str = strings.TrimSpace(str)
					val = strings.TrimSpace(val)
					if strings.Contains(" "+str+" ", " "+val+" ") {
						return true
					}
				case ISWITHIN:
					if strings.Contains(val, str) {
						return true
					}
				case STARTSWITH:
					if strings.HasPrefix(str, val) {
						return true
					}
				case ENDSWITH:
					if strings.HasSuffix(str, val) {
						return true
					}
				case ISNOT:
					if str != val {
						return true
					}
				case ISBEFORE:
					if str < val {
						return true
					}
				case ISAFTER:
					if str > val {
						return true
					}
				case MATCHES:
					if RemoveCommaOrSemicolon(str) == strings.ToLower(val) {
						return true
					}
				case RESEMBLES:
					if SortStringByWords(str) == strings.ToLower(val) {
						return true
					}
				default:
				}
			case ISEQUALTO, DIFFERSFROM:
				// conditional argument is element specifier
				if constraint.Parent != "" || constraint.Match != "" || constraint.Attrib != "" {
					ch := val[0]
					// pound, percent, and caret prefixes supported (undocumented)
					switch ch {
					case '#':
						count := 0
						ExploreElements(curr, mask, constraint.Parent, constraint.Match, constraint.Attrib, constraint.Wild, true, level, func(stn string, lvl int) {
							count++
						})
						val = strconv.Itoa(count)
					case '%':
						length := 0
						ExploreElements(curr, mask, constraint.Parent, constraint.Match, constraint.Attrib, constraint.Wild, true, level, func(stn string, lvl int) {
							if stn != "" {
								length += len(stn)
							}
						})
						val = strconv.Itoa(length)
					case '^':
						depth := 0
						ExploreElements(curr, mask, constraint.Parent, constraint.Match, constraint.Attrib, constraint.Wild, true, level, func(stn string, lvl int) {
							depth = lvl
						})
						val = strconv.Itoa(depth)
					default:
						ExploreElements(curr, mask, constraint.Parent, constraint.Match, constraint.Attrib, constraint.Wild, true, level, func(stn string, lvl int) {
							if stn != "" {
								val = stn
							}
						})
					}
				}
				str = strings.ToUpper(str)
				val = strings.ToUpper(val)

				switch stat {
				case ISEQUALTO:
					if str == val {
						return true
					}
				case DIFFERSFROM:
					if str != val {
						return true
					}
				default:
				}
			case GT, GE, LT, LE, EQ, NE:
				// second argument of numeric test can be element specifier
				if constraint.Parent != "" || constraint.Match != "" || constraint.Attrib != "" {
					ch := val[0]
					// pound, percent, and caret prefixes supported as potentially useful for data QA (undocumented)
					switch ch {
					case '#':
						count := 0
						ExploreElements(curr, mask, constraint.Parent, constraint.Match, constraint.Attrib, constraint.Wild, true, level, func(stn string, lvl int) {
							count++
						})
						val = strconv.Itoa(count)
					case '%':
						length := 0
						ExploreElements(curr, mask, constraint.Parent, constraint.Match, constraint.Attrib, constraint.Wild, true, level, func(stn string, lvl int) {
							if stn != "" {
								length += len(stn)
							}
						})
						val = strconv.Itoa(length)
					case '^':
						depth := 0
						ExploreElements(curr, mask, constraint.Parent, constraint.Match, constraint.Attrib, constraint.Wild, true, level, func(stn string, lvl int) {
							depth = lvl
						})
						val = strconv.Itoa(depth)
					case '&':
						if len(val) > 1 {
							val = val[1:]
							// expand variable to get actual tag
							val = variables[val]
						}
					default:
						ExploreElements(curr, mask, constraint.Parent, constraint.Match, constraint.Attrib, constraint.Wild, true, level, func(stn string, lvl int) {
							if stn != "" {
								_, errz := strconv.Atoi(stn)
								if errz == nil {
									val = stn
								}
							}
						})
					}
				}

				// numeric tests on element values
				x, errx := strconv.Atoi(str)
				y, erry := strconv.Atoi(val)

				// both arguments must resolve to integers
				if errx != nil || erry != nil {
					return false
				}

				switch stat {
				case GT:
					if x > y {
						return true
					}
				case GE:
					if x >= y {
						return true
					}
				case LT:
					if x < y {
						return true
					}
				case LE:
					if x <= y {
						return true
					}
				case EQ:
					if x == y {
						return true
					}
				case NE:
					if x != y {
						return true
					}
				default:
				}
			default:
			}

			return false
		}

		// checkConstraint applies optional [min:max] range restriction and sends result to testConstraint
		checkConstraint := func(str string) bool {

			// handle usual situation with no range first
			if norm {
				return testConstraint(str)
			}

			// check for [after|before] variant
			if typL == STRINGRANGE || typR == STRINGRANGE {
				if strL != "" {
					// use case-insensitive test
					strL = strings.ToUpper(strL)
					idx := strings.Index(strings.ToUpper(str), strL)
					if idx < 0 {
						// specified substring must be present in original string
						return false
					}
					ln := len(strL)
					// remove leading text
					str = str[idx+ln:]
				}
				if strR != "" {
					strR = strings.ToUpper(strR)
					idx := strings.Index(strings.ToUpper(str), strR)
					if idx < 0 {
						// specified substring must be present in remaining string
						return false
					}
					// remove trailing text
					str = str[:idx]
				}
				if str != "" {
					return testConstraint(str)
				}
				return false
			}

			min := 0
			max := 0

			// slice arguments use variable value +- adjustment or integer constant
			if typL == VARIABLERANGE {
				if strL == "" {
					return false
				}
				lft, ok := variables[strL]
				if !ok {
					return false
				}
				val, err := strconv.Atoi(lft)
				if err != nil {
					return false
				}
				// range argument values are inclusive and 1-based, decrement variable start +- offset to use in slice
				min = val + intL - 1
			} else if typL == INTEGERRANGE {
				// range argument values are inclusive and 1-based, decrement literal start to use in slice
				min = intL - 1
			}
			if typR == VARIABLERANGE {
				if strR == "" {
					return false
				}
				rgt, ok := variables[strR]
				if !ok {
					return false
				}
				val, err := strconv.Atoi(rgt)
				if err != nil {
					return false
				}
				if val+intR < 0 {
					// negative value is 1-based inset from end of string (undocumented)
					max = len(str) + val + intR + 1
				} else {
					max = val + intR
				}
			} else if typR == INTEGERRANGE {
				if intR < 0 {
					// negative max is inset from end of string (undocumented)
					max = len(str) + intR + 1
				} else {
					max = intR
				}
			}

			// numeric range now calculated, apply slice to string
			if min == 0 && max == 0 {
				return testConstraint(str)
			} else if max == 0 {
				if min > 0 && min < len(str) {
					str = str[min:]
					if str != "" {
						return testConstraint(str)
					}
				}
			} else if min == 0 {
				if max > 0 && max <= len(str) {
					str = str[:max]
					if str != "" {
						return testConstraint(str)
					}
				}
			} else {
				if min < max && min > 0 && max <= len(str) {
					str = str[min:max]
					if str != "" {
						return testConstraint(str)
					}
				}
			}

			return false
		}

		switch status {
		case ELEMENT:
			exploreElements(func(str string, lvl int) {
				// match to XML container object sends empty string, so do not check for str != "" here
				// test every selected element individually if value is specified
				if constraint == nil || checkConstraint(str) {
					found = true
				}
			})
		case VARIABLE:
			// use value of stored variable
			str, ok := variables[match]
			if ok {
				//  -if &VARIABLE -equals VALUE is the supported construct
				if constraint == nil || checkConstraint(str) {
					found = true
				}
			}
		case COUNT:
			count := 0

			exploreElements(func(str string, lvl int) {
				count++
				found = true
			})

			// number of element objects
			number = strconv.Itoa(count)
		case LENGTH:
			length := 0

			exploreElements(func(str string, lvl int) {
				length += len(str)
				found = true
			})

			// length of element strings
			number = strconv.Itoa(length)
		case DEPTH:
			depth := 0

			exploreElements(func(str string, lvl int) {
				depth = lvl
				found = true
			})

			// depth of last element in scope
			number = strconv.Itoa(depth)
		case INDEX:
			// index of explored parent object
			number = strconv.Itoa(index)
			found = true
		default:
		}

		if number == "" {
			return found
		}

		if constraint == nil || checkConstraint(number) {
			return true
		}

		return false
	}

	// test conditional arguments
	for _, op := range conditions {

		switch op.Type {
		// -if tests for presence of element (deprecated -match can test element:value)
		case SELECT, IF, MATCH:
			// checking for failure here allows for multiple -if [ -and / -or ] clauses
			if isMatch && observed < required {
				return false
			}
			if isAvoid && forbidden > 0 {
				return false
			}
			required = 0
			observed = 0
			forbidden = 0
			isMatch = true
			isAvoid = false
			// continue on to next two cases
			fallthrough
		case AND:
			required++
			// continue on to next case
			fallthrough
		case OR:
			if matchFound(op.Stages) {
				observed++
				// record presence of forbidden element if in -unless clause
				forbidden++
			}
		// -unless tests for absence of element, or presence but with failure of subsequent value test (deprecated -avoid can test element:value)
		case UNLESS, AVOID:
			if isMatch && observed < required {
				return false
			}
			if isAvoid && forbidden > 0 {
				return false
			}
			required = 0
			observed = 0
			forbidden = 0
			isMatch = false
			isAvoid = true
			if matchFound(op.Stages) {
				forbidden++
			}
		default:
		}
	}

	if isMatch && observed < required {
		return false
	}
	if isAvoid && forbidden > 0 {
		return false
	}

	return true
}

// RECURSIVELY PROCESS EXPLORATION COMMANDS AND XML DATA STRUCTURE

// processCommands visits XML nodes, performs conditional tests, and executes data extraction instructions
func processCommands(
	cmds *Block,
	curr *XMLNode,
	tab string,
	ret string,
	index int,
	level int,
	variables map[string]string,
	transform map[string]string,
	srchr *FSMSearcher,
	histogram map[string]int,
	accum func(string),
) (string, string) {

	if accum == nil {
		return tab, ret
	}

	prnt := cmds.Parent
	match := cmds.Match

	// closure passes local variables to callback, which can modify caller tab and ret values
	processNode := func(node *XMLNode, idx, lvl int) {

		// apply -if or -unless tests
		if conditionsAreSatisfied(cmds.Conditions, node, match, idx, lvl, variables) {

			// execute data extraction commands
			if len(cmds.Commands) > 0 {
				tab, ret = processInstructions(cmds.Commands, node, match, tab, ret, idx, lvl, variables, transform, srchr, histogram, accum)
			}

			// process sub commands on child node
			for _, sub := range cmds.Subtasks {
				tab, ret = processCommands(sub, node, tab, ret, 1, lvl, variables, transform, srchr, histogram, accum)
			}

		} else {

			// execute commands after -else statement
			if len(cmds.Failure) > 0 {
				tab, ret = processInstructions(cmds.Failure, node, match, tab, ret, idx, lvl, variables, transform, srchr, histogram, accum)
			}
		}
	}

	// explorePath recursive definition
	var explorePath func(*XMLNode, []string, int, int, func(*XMLNode, int, int)) int

	// explorePath visits child nodes and matches against next entry in path
	explorePath = func(curr *XMLNode, path []string, indx, levl int, proc func(*XMLNode, int, int)) int {

		if curr == nil || proc == nil {
			return indx
		}

		if len(path) < 1 {
			proc(curr, indx, levl)
			indx++
			return indx
		}

		name := path[0]
		rest := path[1:]

		// explore next level of child nodes
		for chld := curr.Children; chld != nil; chld = chld.Next {
			if chld.Name == name {
				// recurse only if child matches next component in path
				indx = explorePath(chld, rest, indx, levl+1, proc)
			}
		}

		return indx
	}

	if cmds.Foreword != "" {
		accum(cmds.Foreword)
	}

	// apply -position test

	if cmds.Position == "" || cmds.Position == "all" {

		ExploreNodes(curr, prnt, match, index, level, processNode)

	} else if cmds.Position == "path" {

		ExploreNodes(curr, prnt, match, index, level,
			func(node *XMLNode, idx, lvl int) {
				// exploreNodes callback has matched first path component, now explore remainder one level and component at a time
				explorePath(node, cmds.Path, idx, lvl, processNode)
			})

	} else {

		var single *XMLNode
		lev := 0
		ind := 0

		if cmds.Position == "first" {

			ExploreNodes(curr, prnt, match, index, level,
				func(node *XMLNode, idx, lvl int) {
					if single == nil {
						single = node
						ind = idx
						lev = lvl
					}
				})

		} else if cmds.Position == "last" {

			ExploreNodes(curr, prnt, match, index, level,
				func(node *XMLNode, idx, lvl int) {
					single = node
					ind = idx
					lev = lvl
				})

		} else if cmds.Position == "outer" {

			// print only first and last nodes
			var beg *Limiter
			var end *Limiter

			ExploreNodes(curr, prnt, match, index, level,
				func(node *XMLNode, idx, lvl int) {
					if beg == nil {
						beg = &Limiter{node, idx, lvl}
					} else {
						end = &Limiter{node, idx, lvl}
					}
				})

			if beg != nil {
				processNode(beg.Obj, beg.Idx, beg.Lvl)
			}
			if end != nil {
				processNode(end.Obj, end.Idx, end.Lvl)
			}

		} else if cmds.Position == "inner" {

			// print all but first and last nodes
			var prev *Limiter
			var next *Limiter
			first := true

			ExploreNodes(curr, prnt, match, index, level,
				func(node *XMLNode, idx, lvl int) {
					if first {
						first = false
						return
					}

					prev = next
					next = &Limiter{node, idx, lvl}

					if prev != nil {
						processNode(prev.Obj, prev.Idx, prev.Lvl)
					}
				})

		} else if cmds.Position == "even" {

			okay := false

			ExploreNodes(curr, prnt, match, index, level,
				func(node *XMLNode, idx, lvl int) {
					if okay {
						processNode(node, idx, lvl)
					}
					okay = !okay
				})

		} else if cmds.Position == "odd" {

			okay := true

			ExploreNodes(curr, prnt, match, index, level,
				func(node *XMLNode, idx, lvl int) {
					if okay {
						processNode(node, idx, lvl)
					}
					okay = !okay
				})

		} else {

			// use numeric position
			number, err := strconv.Atoi(cmds.Position)
			if err == nil {

				pos := 0

				ExploreNodes(curr, prnt, match, index, level,
					func(node *XMLNode, idx, lvl int) {
						pos++
						if pos == number {
							single = node
							ind = idx
							lev = lvl
						}
					})

			} else {

				fmt.Fprintf(os.Stderr, "\nERROR: Unrecognized position '%s'\n", cmds.Position)
				os.Exit(1)
			}
		}

		if single != nil {
			processNode(single, ind, lev)
		}
	}

	if cmds.Afterword != "" {
		accum(cmds.Afterword)
	}

	return tab, ret
}

// PROCESS ONE XML COMPONENT RECORD

// ProcessExtract perform data extraction driven by command-line arguments
func ProcessExtract(text, parent string, index int, hd, tl string, transform map[string]string, srchr *FSMSearcher, histogram map[string]int, cmds *Block) string {

	if text == "" || cmds == nil {
		return ""
	}

	// exit from function will collect garbage of node structure for current XML object
	pat := ParseRecord(text, parent)

	if pat == nil {
		return ""
	}

	// exit from function will also free map of recorded variables for current -pattern
	variables := make(map[string]string)

	var buffer strings.Builder

	ok := false

	if hd != "" {
		buffer.WriteString(hd)
	}

	ret := ""

	if cmds.Position == "select" {

		if conditionsAreSatisfied(cmds.Conditions, pat, cmds.Match, index, 1, variables) {
			ok = true
			buffer.WriteString(text)
			ret = "\n"
		}

	} else {

		// start processing at top of command tree and top of XML subregion selected by -pattern
		_, ret = processCommands(cmds, pat, "", "", index, 1, variables, transform, srchr, histogram,
			func(str string) {
				if str != "" {
					ok = true
					buffer.WriteString(str)
				}
			})
	}

	if tl != "" {
		buffer.WriteString(tl)
	}

	if ret != "" {
		ok = true
		buffer.WriteString(ret)
	}

	txt := buffer.String()

	// remove leading newline (-insd -pfx artifact)
	if txt != "" && txt[0] == '\n' {
		txt = txt[1:]
	}

	if !ok {
		return ""
	}

	// return consolidated result string
	return txt
}

// INSDSEQ EXTRACTION COMMAND GENERATOR

// e.g., xtract -insd complete mat_peptide "%peptide" product peptide

// ProcessINSD generates extraction commands for GenBank/RefSeq records in INSDSet format
func ProcessINSD(args []string, isPipe, addDash, doIndex bool) []string {

	// legal GenBank / GenPept / RefSeq features

	features := []string{
		"-10_signal",
		"-35_signal",
		"3'clip",
		"3'UTR",
		"5'clip",
		"5'UTR",
		"allele",
		"assembly_gap",
		"attenuator",
		"Bond",
		"C_region",
		"CAAT_signal",
		"CDS",
		"centromere",
		"conflict",
		"D_segment",
		"D-loop",
		"enhancer",
		"exon",
		"gap",
		"GC_signal",
		"gene",
		"iDNA",
		"intron",
		"J_segment",
		"LTR",
		"mat_peptide",
		"misc_binding",
		"misc_difference",
		"misc_feature",
		"misc_recomb",
		"misc_RNA",
		"misc_signal",
		"misc_structure",
		"mobile_element",
		"modified_base",
		"mRNA",
		"mutation",
		"N_region",
		"ncRNA",
		"old_sequence",
		"operon",
		"oriT",
		"polyA_signal",
		"polyA_site",
		"precursor_RNA",
		"prim_transcript",
		"primer_bind",
		"promoter",
		"propeptide",
		"protein_bind",
		"Protein",
		"RBS",
		"Region",
		"regulatory",
		"rep_origin",
		"repeat_region",
		"repeat_unit",
		"rRNA",
		"S_region",
		"satellite",
		"scRNA",
		"sig_peptide",
		"Site",
		"snoRNA",
		"snRNA",
		"source",
		"stem_loop",
		"STS",
		"TATA_signal",
		"telomere",
		"terminator",
		"tmRNA",
		"transit_peptide",
		"tRNA",
		"unsure",
		"V_region",
		"V_segment",
		"variation",
	}

	// legal GenBank / GenPept / RefSeq qualifiers

	qualifiers := []string{
		"allele",
		"altitude",
		"anticodon",
		"artificial_location",
		"bio_material",
		"bond_type",
		"bound_moiety",
		"breed",
		"calculated_mol_wt",
		"cell_line",
		"cell_type",
		"chloroplast",
		"chromoplast",
		"chromosome",
		"circular_RNA",
		"citation",
		"clone_lib",
		"clone",
		"coded_by",
		"codon_start",
		"codon",
		"collected_by",
		"collection_date",
		"compare",
		"cons_splice",
		"country",
		"cultivar",
		"culture_collection",
		"cyanelle",
		"db_xref",
		"derived_from",
		"dev_stage",
		"direction",
		"EC_number",
		"ecotype",
		"encodes",
		"endogenous_virus",
		"environmental_sample",
		"estimated_length",
		"evidence",
		"exception",
		"experiment",
		"focus",
		"frequency",
		"function",
		"gap_type",
		"gdb_xref",
		"gene_synonym",
		"gene",
		"germline",
		"GO_component",
		"GO_function",
		"GO_process",
		"haplogroup",
		"haplotype",
		"host",
		"identified_by",
		"inference",
		"insertion_seq",
		"isolate",
		"isolation_source",
		"kinetoplast",
		"lab_host",
		"label",
		"lat_lon",
		"linkage_evidence",
		"locus_tag",
		"macronuclear",
		"map",
		"mating_type",
		"metagenome_source",
		"metagenomic",
		"mitochondrion",
		"mobile_element_type",
		"mobile_element",
		"mod_base",
		"mol_type",
		"name",
		"nat_host",
		"ncRNA_class",
		"non_functional",
		"note",
		"number",
		"old_locus_tag",
		"operon",
		"organelle",
		"organism",
		"partial",
		"PCR_conditions",
		"PCR_primers",
		"peptide",
		"phenotype",
		"plasmid",
		"pop_variant",
		"product",
		"protein_id",
		"proviral",
		"pseudo",
		"pseudogene",
		"rearranged",
		"recombination_class",
		"region_name",
		"regulatory_class",
		"replace",
		"ribosomal_slippage",
		"rpt_family",
		"rpt_type",
		"rpt_unit_range",
		"rpt_unit_seq",
		"rpt_unit",
		"satellite",
		"segment",
		"sequenced_mol",
		"serotype",
		"serovar",
		"sex",
		"site_type",
		"specific_host",
		"specimen_voucher",
		"standard_name",
		"strain",
		"structural_class",
		"sub_clone",
		"sub_species",
		"sub_strain",
		"submitter_seqid",
		"tag_peptide",
		"tissue_lib",
		"tissue_type",
		"trans_splicing",
		"transcript_id",
		"transcription",
		"transgenic",
		"transl_except",
		"transl_table",
		"translation",
		"transposon",
		"type_material",
		"UniProtKB_evidence",
		"usedin",
		"variety",
		"virion",
	}

	// legal INSDSeq XML fields

	insdtags := []string{
		"INSDAltSeqData_items",
		"INSDAltSeqData",
		"INSDAltSeqItem_first-accn",
		"INSDAltSeqItem_gap-comment",
		"INSDAltSeqItem_gap-length",
		"INSDAltSeqItem_gap-linkage",
		"INSDAltSeqItem_gap-type",
		"INSDAltSeqItem_interval",
		"INSDAltSeqItem_isgap",
		"INSDAltSeqItem_isgap@value",
		"INSDAltSeqItem_last-accn",
		"INSDAltSeqItem_value",
		"INSDAltSeqItem",
		"INSDAuthor",
		"INSDComment_paragraphs",
		"INSDComment_type",
		"INSDComment",
		"INSDCommentParagraph",
		"INSDFeature_intervals",
		"INSDFeature_key",
		"INSDFeature_location",
		"INSDFeature_operator",
		"INSDFeature_partial3",
		"INSDFeature_partial3@value",
		"INSDFeature_partial5",
		"INSDFeature_partial5@value",
		"INSDFeature_quals",
		"INSDFeature_xrefs",
		"INSDFeature",
		"INSDFeatureSet_annot-source",
		"INSDFeatureSet_features",
		"INSDFeatureSet",
		"INSDInterval_accession",
		"INSDInterval_from",
		"INSDInterval_interbp",
		"INSDInterval_interbp@value",
		"INSDInterval_iscomp",
		"INSDInterval_iscomp@value",
		"INSDInterval_point",
		"INSDInterval_to",
		"INSDInterval",
		"INSDKeyword",
		"INSDQualifier_name",
		"INSDQualifier_value",
		"INSDQualifier",
		"INSDReference_authors",
		"INSDReference_consortium",
		"INSDReference_journal",
		"INSDReference_position",
		"INSDReference_pubmed",
		"INSDReference_reference",
		"INSDReference_remark",
		"INSDReference_title",
		"INSDReference_xref",
		"INSDReference",
		"INSDSecondary-accn",
		"INSDSeq_accession-version",
		"INSDSeq_alt-seq",
		"INSDSeq_comment-set",
		"INSDSeq_comment",
		"INSDSeq_contig",
		"INSDSeq_create-date",
		"INSDSeq_create-release",
		"INSDSeq_database-reference",
		"INSDSeq_definition",
		"INSDSeq_division",
		"INSDSeq_entry-version",
		"INSDSeq_feature-set",
		"INSDSeq_feature-table",
		"INSDSeq_keywords",
		"INSDSeq_length",
		"INSDSeq_locus",
		"INSDSeq_moltype",
		"INSDSeq_organism",
		"INSDSeq_other-seqids",
		"INSDSeq_primary-accession",
		"INSDSeq_primary",
		"INSDSeq_project",
		"INSDSeq_references",
		"INSDSeq_secondary-accessions",
		"INSDSeq_segment",
		"INSDSeq_sequence",
		"INSDSeq_source-db",
		"INSDSeq_source",
		"INSDSeq_strandedness",
		"INSDSeq_struc-comments",
		"INSDSeq_taxonomy",
		"INSDSeq_topology",
		"INSDSeq_update-date",
		"INSDSeq_update-release",
		"INSDSeq_xrefs",
		"INSDSeq",
		"INSDSeqid",
		"INSDSet",
		"INSDStrucComment_items",
		"INSDStrucComment_name",
		"INSDStrucComment",
		"INSDStrucCommentItem_tag",
		"INSDStrucCommentItem_url",
		"INSDStrucCommentItem_value",
		"INSDStrucCommentItem",
		"INSDXref_dbname",
		"INSDXref_id",
		"INSDXref",
	}

	checkAgainstVocabulary := func(str, objtype string, arry []string) {

		if str == "" || arry == nil {
			return
		}

		// skip past pound, percent, or caret character at beginning of string
		if len(str) > 1 {
			switch str[0] {
			case '#', '%', '^':
				str = str[1:]
			default:
			}
		}

		for _, txt := range arry {
			if str == txt {
				return
			}
			if strings.ToUpper(str) == strings.ToUpper(txt) {
				fmt.Fprintf(os.Stderr, "\nERROR: Incorrect capitalization of '%s' %s, change to '%s'\n", str, objtype, txt)
				os.Exit(1)
			}
		}

		fmt.Fprintf(os.Stderr, "\nERROR: Item '%s' is not a legal -insd %s\n", str, objtype)
		os.Exit(1)
	}

	var acc []string

	max := len(args)
	if max < 1 {
		fmt.Fprintf(os.Stderr, "\nERROR: Insufficient command-line arguments supplied to xtract -insd\n")
		os.Exit(1)
	}

	// record accession and sequence

	if doIndex {
		if isPipe {
			acc = append(acc, "-head", "<IdxDocumentSet>", "-tail", "</IdxDocumentSet>")
			acc = append(acc, "-hd", "  <IdxDocument>\n", "-tl", "  </IdxDocument>")
			acc = append(acc, "-pattern", "INSDSeq", "-pfx", "    <IdxUid>", "-sfx", "</IdxUid>\n")
			acc = append(acc, "-element", "INSDSeq_accession-version", "-clr", "-rst", "-tab", "\n")
		} else {
			acc = append(acc, "-head", "\"<IdxDocumentSet>\"", "-tail", "\"</IdxDocumentSet>\"")
			acc = append(acc, "-hd", "\"  <IdxDocument>\\n\"", "-tl", "\"  </IdxDocument>\"")
			acc = append(acc, "-pattern", "INSDSeq", "-pfx", "\"    <IdxUid>\"", "-sfx", "\"</IdxUid>\\n\"")
			acc = append(acc, "-element", "INSDSeq_accession-version", "-clr", "-rst", "-tab", "\\n")
		}
	} else {
		acc = append(acc, "-pattern", "INSDSeq", "-ACCN", "INSDSeq_accession-version")
		acc = append(acc, "-LCUS", "INSDSeq_locus", "-SEQ", "INSDSeq_sequence")
	}

	if doIndex {
		if isPipe {
			acc = append(acc, "-group", "INSDSeq", "-lbl", "    <IdxSearchFields>\n")
		} else {
			acc = append(acc, "-group", "INSDSeq", "-lbl", "\"    <IdxSearchFields>\\n\"")
		}
	}

	printAccn := true

	// collect descriptors

	if strings.HasPrefix(args[0], "INSD") {

		if doIndex {
			acc = append(acc, "-clr", "-indices")
		} else {
			if isPipe {
				acc = append(acc, "-clr", "-pfx", "\\n", "-element", "&ACCN")
				acc = append(acc, "-group", "INSDSeq", "-sep", "|", "-element")
			} else {
				acc = append(acc, "-clr", "-pfx", "\"\\n\"", "-element", "\"&ACCN\"")
				acc = append(acc, "-group", "INSDSeq", "-sep", "\"|\"", "-element")
			}
			printAccn = false
		}

		for {
			if len(args) < 1 {
				return acc
			}
			str := args[0]
			if !strings.HasPrefix(args[0], "INSD") {
				break
			}
			checkAgainstVocabulary(str, "element", insdtags)
			acc = append(acc, str)
			args = args[1:]
		}

	} else if strings.HasPrefix(strings.ToUpper(args[0]), "INSD") {

		// report capitalization or vocabulary failure
		checkAgainstVocabulary(args[0], "element", insdtags)

		// program should not get to this point, but warn and exit anyway
		fmt.Fprintf(os.Stderr, "\nERROR: Item '%s' is not a legal -insd %s\n", args[0], "element")
		os.Exit(1)
	}

	processOneFeature := func(ftargs []string) {

		// skip past -insd feature clause separator

		if ftargs[0] == "-insd" {
			ftargs = ftargs[1:]
		}

		// collect qualifiers

		partial := false
		complete := false

		if ftargs[0] == "+" || ftargs[0] == "complete" {
			complete = true
			ftargs = ftargs[1:]
			max--
		} else if ftargs[0] == "-" || ftargs[0] == "partial" {
			partial = true
			ftargs = ftargs[1:]
			max--
		}

		if max < 1 {
			fmt.Fprintf(os.Stderr, "\nERROR: No feature key supplied to xtract -insd\n")
			os.Exit(1)
		}

		acc = append(acc, "-group", "INSDFeature")

		// limit to designated features

		feature := ftargs[0]

		fcmd := "-if"

		// can specify multiple features separated by plus sign (e.g., CDS+mRNA) or comma (e.g., CDS,mRNA)
		plus := strings.Split(feature, "+")
		for _, pls := range plus {
			comma := strings.Split(pls, ",")
			for _, cma := range comma {

				checkAgainstVocabulary(cma, "feature", features)
				acc = append(acc, fcmd, "INSDFeature_key", "-equals", cma)

				fcmd = "-or"
			}
		}

		if max < 2 {
			// still need at least one qualifier even on legal feature
			fmt.Fprintf(os.Stderr, "\nERROR: Feature '%s' must be followed by at least one qualifier\n", feature)
			os.Exit(1)
		}

		ftargs = ftargs[1:]

		if complete {
			acc = append(acc, "-branch", "INSDFeature", "-unless", "INSDFeature_partial5", "-or", "INSDFeature_partial3")
		} else if partial {
			acc = append(acc, "-branch", "INSDFeature", "-if", "INSDFeature_partial5", "-or", "INSDFeature_partial3")
		}

		if printAccn {
			if doIndex {
			} else {
				if isPipe {
					acc = append(acc, "-clr", "-pfx", "\\n", "-first", "&ACCN,&LCUS")
				} else {
					acc = append(acc, "-clr", "-pfx", "\"\\n\"", "-first", "\"&ACCN,&LCUS\"")
				}
				printAccn = false
			}
		}

		for _, str := range ftargs {

			if str == "mol_wt" {
				str = "calculated_mol_wt"
			}

			if strings.HasPrefix(str, "INSD") {

				checkAgainstVocabulary(str, "element", insdtags)
				if doIndex {
					acc = append(acc, "-block", "INSDFeature", "-clr", "-indices")
				} else {
					if isPipe {
						acc = append(acc, "-block", "INSDFeature", "-sep", "|", "-element")
					} else {
						acc = append(acc, "-block", "INSDFeature", "-sep", "\"|\"", "-element")
					}
				}
				acc = append(acc, str)
				if addDash {
					acc = append(acc, "-block", "INSDFeature", "-unless", str)
					if strings.HasSuffix(str, "@value") {
						if isPipe {
							acc = append(acc, "-lbl", "false")
						} else {
							acc = append(acc, "-lbl", "\"false\"")
						}
					} else {
						if isPipe {
							acc = append(acc, "-lbl", "\\-")
						} else {
							acc = append(acc, "-lbl", "\"\\-\"")
						}
					}
				}

			} else if strings.HasPrefix(str, "#INSD") {

				checkAgainstVocabulary(str, "element", insdtags)
				if doIndex {
					acc = append(acc, "-block", "INSDFeature", "-clr", "-indices")
				} else {
					if isPipe {
						acc = append(acc, "-block", "INSDFeature", "-sep", "|", "-element")
						acc = append(acc, str)
					} else {
						acc = append(acc, "-block", "INSDFeature", "-sep", "\"|\"", "-element")
						ql := fmt.Sprintf("\"%s\"", str)
						acc = append(acc, ql)
					}
				}

			} else if strings.HasPrefix(strings.ToUpper(str), "#INSD") {

				// report capitalization or vocabulary failure
				checkAgainstVocabulary(str, "element", insdtags)

			} else if str == "sub_sequence" {

				// special sub_sequence qualifier shows sequence under feature intervals
				acc = append(acc, "-block", "INSDFeature_intervals")

				acc = append(acc, "-subset", "INSDInterval", "-FR", "INSDInterval_from", "-TO", "INSDInterval_to")
				if isPipe {
					acc = append(acc, "-pfx", "", "-tab", "", "-nucleic", "&SEQ[&FR:&TO]")
				} else {
					acc = append(acc, "-pfx", "\"\"", "-tab", "\"\"", "-nucleic", "\"&SEQ[&FR:&TO]\"")
				}

				acc = append(acc, "-subset", "INSDFeature_intervals")
				if isPipe {
					acc = append(acc, "-deq", "\\t")
				} else {
					acc = append(acc, "-deq", "\"\\t\"")
				}

			} else if str == "feat_location" {

				// special feat_location qualifier shows feature intervals, in 1-based GenBank convention
				acc = append(acc, "-block", "INSDFeature_intervals")

				acc = append(acc, "-subset", "INSDInterval", "-FR", "INSDInterval_from", "-TO", "INSDInterval_to")
				if isPipe {
					acc = append(acc, "-pfx", "", "-tab", "..", "-element", "&FR")
					acc = append(acc, "-pfx", "", "-tab", ",", "-element", "&TO")
				} else {
					acc = append(acc, "-pfx", "\"\"", "-tab", "\"..\"", "-element", "\"&FR\"")
					acc = append(acc, "-pfx", "\"\"", "-tab", "\",\"", "-element", "\"&TO\"")
				}

				acc = append(acc, "-subset", "INSDFeature_intervals")
				if isPipe {
					acc = append(acc, "-deq", "\\t")
				} else {
					acc = append(acc, "-deq", "\"\\t\"")
				}

			} else if str == "feat_intervals" {

				// special feat_intervals qualifier shows feature intervals, decremented to 0-based
				acc = append(acc, "-block", "INSDFeature_intervals")

				acc = append(acc, "-subset", "INSDInterval")
				if isPipe {
					acc = append(acc, "-pfx", "", "-tab", "..", "-dec", "INSDInterval_from")
					acc = append(acc, "-pfx", "", "-tab", ",", "-dec", "INSDInterval_to")
				} else {
					acc = append(acc, "-pfx", "\"\"", "-tab", "\"..\"", "-dec", "\"INSDInterval_from\"")
					acc = append(acc, "-pfx", "\"\"", "-tab", "\",\"", "-dec", "\"INSDInterval_to\"")
				}

				acc = append(acc, "-subset", "INSDFeature_intervals")
				if isPipe {
					acc = append(acc, "-deq", "\\t")
				} else {
					acc = append(acc, "-deq", "\"\\t\"")
				}

			} else if str == "chloroplast" ||
				str == "chromoplast" ||
				str == "cyanelle" ||
				str == "environmental_sample" ||
				str == "focus" ||
				str == "germline" ||
				str == "kinetoplast" ||
				str == "macronuclear" ||
				str == "metagenomic" ||
				str == "mitochondrion" ||
				str == "partial" ||
				str == "proviral" ||
				str == "pseudo" ||
				str == "rearranged" ||
				str == "ribosomal_slippage" ||
				str == "trans_splicing" ||
				str == "transgenic" ||
				str == "virion" {

				acc = append(acc, "-block", "INSDQualifier")

				checkAgainstVocabulary(str, "qualifier", qualifiers)
				if doIndex {
					acc = append(acc, "-if", "INSDQualifier_name", "-equals", str)
					acc = append(acc, "-clr", "-indices", "INSDQualifier_name")
				} else {
					acc = append(acc, "-if", "INSDQualifier_name", "-equals", str)
					acc = append(acc, "-lbl", str)
				}
				if addDash {
					acc = append(acc, "-block", "INSDFeature", "-unless", "INSDQualifier_name", "-equals", str)
					if isPipe {
						acc = append(acc, "-lbl", "\\-")
					} else {
						acc = append(acc, "-lbl", "\"\\-\"")
					}
				}

			} else {

				acc = append(acc, "-block", "INSDQualifier")

				isTaxID := false
				if feature == "source" && (str == "taxon" || str == "taxid") {
					// special taxid qualifier extracts number from taxon db_xref
					isTaxID = true
					str = "db_xref"
				} else {
					checkAgainstVocabulary(str, "qualifier", qualifiers)
				}

				if len(str) > 2 && str[0] == '%' {
					acc = append(acc, "-if", "INSDQualifier_name", "-equals", str[1:])
					if doIndex {
						if isPipe {
							acc = append(acc, "-clr", "-indices", "%INSDQualifier_value")
						} else {
							acc = append(acc, "-clr", "-indices", "\"%INSDQualifier_value\"")
						}
					} else {
						if isPipe {
							acc = append(acc, "-element", "%INSDQualifier_value")
						} else {
							acc = append(acc, "-element", "\"%INSDQualifier_value\"")
						}
					}
					if addDash {
						acc = append(acc, "-block", "INSDFeature", "-unless", "INSDQualifier_name", "-equals", str[1:])
						if isPipe {
							acc = append(acc, "-lbl", "\\-")
						} else {
							acc = append(acc, "-lbl", "\"\\-\"")
						}
					}
				} else {
					if doIndex {
						acc = append(acc, "-if", "INSDQualifier_name", "-equals", str)
						acc = append(acc, "-clr", "-indices", "INSDQualifier_value")
					} else if isTaxID {
						acc = append(acc, "-if", "INSDQualifier_name", "-equals", str)
						acc = append(acc, "-and", "INSDQualifier_value", "-starts-with", "taxon:")
						acc = append(acc, "-element", "INSDQualifier_value[taxon:|]")
					} else {
						acc = append(acc, "-if", "INSDQualifier_name", "-equals", str)
						acc = append(acc, "-element", "INSDQualifier_value")
					}
					if addDash {
						if isTaxID {
							acc = append(acc, "-block", "INSDFeature", "-unless", "INSDQualifier_value", "-starts-with", "taxon:")
						} else {
							acc = append(acc, "-block", "INSDFeature", "-unless", "INSDQualifier_name", "-equals", str)
						}
						if isPipe {
							acc = append(acc, "-lbl", "\\-")
						} else {
							acc = append(acc, "-lbl", "\"\\-\"")
						}
					}
				}
			}
		}
	}

	// multiple feature clauses are separated by additional -insd arguments

	last := 0
	curr := 0
	nxt := ""

	for curr, nxt = range args {
		if nxt == "-insd" {
			if last < curr {
				processOneFeature(args[last:curr])
				last = curr
			}
		}
	}

	if last < curr {
		processOneFeature(args[last:])
	}

	if doIndex {
		if isPipe {
			acc = append(acc, "-group", "INSDSeq", "-clr", "-lbl", "    </IdxSearchFields>\n")
		} else {
			acc = append(acc, "-group", "INSDSeq", "-clr", "-lbl", "\"    </IdxSearchFields>\\n\"")
		}
	}

	return acc
}

// BIOTHINGS EXTRACTION COMMAND GENERATOR

// ProcessBiopath generates extraction commands for BioThings resources (undocumented)
func ProcessBiopath(args []string, isPipe bool) []string {

	// nquire -get "http://myvariant.info/v1/variant/chr6:g.26093141G>A" \
	//   -fields clinvar.rcv.conditions.identifiers \
	//   -always_list clinvar.rcv.conditions.identifiers |
	// transmute -j2x |
	// xtract -biopath opt clinvar.rcv.conditions.identifiers.omim

	var acc []string

	max := len(args)
	if max < 2 {
		fmt.Fprintf(os.Stderr, "\nERROR: Insufficient command-line arguments supplied to xtract -biopath\n")
		os.Exit(1)
	}

	obj := args[0]
	args = args[1:]

	acc = append(acc, "-pattern", obj)

	paths := args[0]

	items := strings.Split(paths, ",")

	for _, path := range items {

		dirs := strings.Split(path, ".")
		max = len(dirs)
		if max < 1 {
			fmt.Fprintf(os.Stderr, "\nERROR: Insufficient path arguments supplied to xtract -biopath\n")
			os.Exit(1)
		}
		if max > 7 {
			fmt.Fprintf(os.Stderr, "\nERROR: Too many nodes in argument supplied to xtract -biopath\n")
			os.Exit(1)
		}

		str := dirs[max-1]

		acc = append(acc, "-path")
		if isPipe {
			acc = append(acc, path)
			acc = append(acc, "-tab", "\\n")
			acc = append(acc, "-element", str)
		} else {
			acc = append(acc, "\""+path+"\"")
			acc = append(acc, "-tab", "\"\\n\"")
			acc = append(acc, "-element", "\""+str+"\"")
		}
	}

	return acc
}
