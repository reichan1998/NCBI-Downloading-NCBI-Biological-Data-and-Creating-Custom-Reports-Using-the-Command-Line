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
// File Name:  table.go
//
// Author:  Jonathan Kans
//
// ==========================================================================

package eutils

import (
	"bufio"
	"bytes"
	"fmt"
	"html"
	"io"
	"os"
	"runtime"
	"strings"
)

// TableConverter parses tab-delimited or comma-separated values files into XML object stream
func TableConverter(inp io.Reader, delim, set, rec string, skip int, header, lower, upper, indent bool, fields []string) <-chan string {

	if inp == nil {
		return nil
	}

	head := ""
	tail := ""

	hd := ""
	tl := ""

	if set != "" && set != "-" {
		head = "<" + set + ">"
		tail = "</" + set + ">"
	}

	if rec != "" && rec != "-" {
		hd = "<" + rec + ">"
		tl = "</" + rec + ">"
	}

	numFlds := len(fields)

	if numFlds < 1 && !header {
		fmt.Fprintf(os.Stderr, "\nERROR: Insufficient arguments for table converter\n")
		os.Exit(1)
	}

	out := make(chan string, chanDepth)
	if out == nil {
		fmt.Fprintf(os.Stderr, "\nERROR: Unable to create table converter channel\n")
		os.Exit(1)
	}

	convertTable := func(inp io.Reader, out chan<- string) {

		// close channel when all records have been sent
		defer close(out)

		okay := false
		row := 0

		var buffer strings.Builder

		scanr := bufio.NewScanner(inp)

		if head != "" {
			buffer.WriteString(head)
			buffer.WriteString("\n")
		}

		if header {

			// uses fields from first row for column names
			for scanr.Scan() {

				line := scanr.Text()

				row++

				if skip > 0 {
					skip--
					continue
				}

				cols := strings.Split(line, delim)

				for _, str := range cols {
					fields = append(fields, str)
					numFlds++
				}
				break
			}

			if numFlds < 1 {
				fmt.Fprintf(os.Stderr, "\nERROR: Line with column names not found\n")
				os.Exit(1)
			}
		}

		for scanr.Scan() {

			line := scanr.Text()

			row++

			if skip > 0 {
				skip--
				continue
			}

			cols := strings.Split(line, delim)

			if len(cols) != numFlds {
				fmt.Fprintf(os.Stderr, "Mismatched columns in row %d - '%s'\n", row, line)
				continue
			}

			if hd != "" {
				if indent {
					buffer.WriteString("  ")
				}
				buffer.WriteString(hd)
				buffer.WriteString("\n")
			}

			for i, fld := range fields {
				val := cols[i]
				if lower {
					val = strings.ToLower(val)
				}
				if upper {
					val = strings.ToUpper(val)
				}
				if fld[0] == '*' {
					fld = fld[1:]
				} else {
					val = html.EscapeString(val)
				}
				val = strings.TrimSpace(val)
				if indent {
					buffer.WriteString("    ")
				}
				buffer.WriteString("<")
				buffer.WriteString(fld)
				buffer.WriteString(">")
				buffer.WriteString(val)
				buffer.WriteString("</")
				buffer.WriteString(fld)
				buffer.WriteString(">")
				buffer.WriteString("\n")
			}

			if tl != "" {
				if indent {
					buffer.WriteString("  ")
				}
				buffer.WriteString(tl)
				buffer.WriteString("\n")
			}

			okay = true
		}

		if tail != "" {
			buffer.WriteString(tail)
			buffer.WriteString("\n")
		}

		if okay {
			txt := buffer.String()
			if txt != "" {
				// send remaining result through output channel
				out <- txt
			}
		}

		buffer.Reset()

		runtime.Gosched()
	}

	go convertTable(inp, out)

	return out
}

// TableToMap reads a two-column tab-delimited file and populates the data into an existing map
func TableToMap(tf string, mp map[string]string) {

	if mp == nil {
		// allow program to continue
		return
	}

	inFile, err := os.Open(tf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to open table file %s - %s\n", tf, err.Error())
		// warn, but allow program to continue
		return
	}

	scant := bufio.NewScanner(inFile)

	// populate transformation map
	for scant.Scan() {

		line := scant.Text()
		cols := strings.Split(line, "\t")
		if len(cols) != 2 {
			continue
		}
		frst := cols[0]
		scnd := cols[1]

		// set new value
		mp[frst] = scnd
	}

	inFile.Close()
}

// TextBlock is a (multi-line) string that is trimmed back to end with the last newline.
// The excluded characters are saved and prepended to the next buffer. Providing complete
// lines simplifies subsequent parsing.
type TextBlock string

// CreateTextStreamer reads input blocks of line-oriented text that is trimmed back to end
// at the last newline. The excluded characters are saved and prepended to the next buffer.
func CreateTextStreamer(in io.Reader) <-chan TextBlock {

	if in == nil {
		return nil
	}

	out := make(chan TextBlock, chanDepth)
	if out == nil {
		fmt.Fprintf(os.Stderr, "\nERROR: Unable to create line block reader channel\n")
		os.Exit(1)
	}

	// lineReader sends trimmed line blocks through the output channel.
	lineReader := func(in io.Reader, out chan<- TextBlock) {

		// close channel when all blocks have been processed
		defer close(out)

		// 65536 appears to be the maximum number of characters presented to io.Reader
		// when input is piped from stdin. Increasing the buffer size when input is from
		// a file does not improve program performance. An additional 16384 bytes are
		// reserved for copying the previous remainder to the beginning of the buffer
		// before the next read.
		const BUFSIZE = 65536 + 16384

		buffer := make([]byte, BUFSIZE)
		remainder := ""
		position := int64(0)
		delta := 0
		isClosed := false

		// nextBuffer reads one buffer, trims back to the right-most newline character,
		// and retains the remainder for prepending in the next call. It also signals if
		// there was no newline character, resulting in subsequent calls to nextBuffer to
		// continue reading a large content string.
		nextBuffer := func() ([]byte, bool, bool) {

			if isClosed {
				return nil, false, true
			}

			// prepend previous remainder to beginning of buffer
			m := copy(buffer, remainder)
			remainder = ""
			if m > 16384 {
				// previous remainder is larger than reserved section,
				// write and signal the need to continue reading.
				return buffer[:m], true, false
			}

			// read next block, append behind copied remainder from previous read
			n, err := in.Read(buffer[m:])
			// with data piped through stdin, read function may not always return the
			// same number of bytes each time
			if err != nil {
				if err != io.EOF {
					// real error.
					fmt.Fprintf(os.Stderr, "\n%sERROR: %s%s\n", RED, err.Error(), INIT)
					// ignore bytes - non-conforming implementations of io.Reader may
					// return mangled data on non-EOF errors
					isClosed = true
					return nil, false, true
				}
				// end of file.
				isClosed = true
				if n == 0 {
					// if EOF and no more data, do not send final remainder (not terminated
					// by right angle bracket that is used as a sentinel)
					return nil, false, true
				}
			}
			if n < 0 {
				// reality check - non-conforming implementations of io.Reader may return -1
				fmt.Fprintf(os.Stderr, "\n%sERROR: io.Reader returned negative count %d%s\n", RED, n, INIT)
				// treat as n == 0 in order to update file offset and avoid losing previous remainder
				n = 0
			}

			// keep track of file offset
			position += int64(delta)
			delta = n

			// slice of actual characters read
			bufr := buffer[:n+m]

			// Look for last newline character. It is safe to back up on UTF-8 rune array when looking
			// for a 7-bit ASCII character.
			pos := -1
			for pos = len(bufr) - 1; pos >= 0; pos-- {
				if bufr[pos] == '\n' {
					// found end of line, break
					break
				}
			}

			// trim back to last newline character, save remainder for next buffer
			if pos > -1 {
				pos++
				remainder = string(bufr[pos:])
				return bufr[:pos], false, false
			}

			// no > found, signal need to continue reading long content
			return bufr[:], true, false
		}

		// nextBlock reads buffer, concatenates if necessary to place long element content
		// into a single string. All result strings end in a newline character that is used
		// sentinel in subsequent code.
		nextBlock := func() string {

			// read next buffer
			line, cont, closed := nextBuffer()

			if closed {
				// no sentinel in remainder at end of file
				return ""
			}

			if cont {
				// current line does not end with newline character
				var buff bytes.Buffer

				// keep reading long content blocks
				for {
					if len(line) > 0 {
						buff.Write(line)
					}
					if !cont {
						// last buffer ended with sentinel
						break
					}
					line, cont, closed = nextBuffer()
					if closed {
						// no sentinel in multi-block buffer at end of file
						return ""
					}
				}

				// concatenate blocks
				return buff.String()
			}

			return string(line)
		}

		// read lines and send blocks through channel
		for {
			str := nextBlock()

			// trimming spaces here would throw off line tracking

			out <- TextBlock(str)

			// bail after sending empty string sentinel
			if str == "" {
				return
			}
		}
	}

	// launch single block reader goroutine
	go lineReader(in, out)

	return out
}

// CreateTextProducer partitions a text line set and sends records down a channel
// for asynchronous processing by multiple concurrent go routines.
func CreateTextProducer(pat string, inp <-chan TextBlock) <-chan string {

	if inp == nil || pat == "" {
		return nil
	}

	out := make(chan string, chanDepth)
	if out == nil {
		fmt.Fprintf(os.Stderr, "\nERROR: Unable to create text producer channel\n")
		os.Exit(1)
	}

	// textProducer sends partitioned strings through channel
	textProducer := func(pat string, rdr <-chan TextBlock, out chan<- string) {

		// close channel when all records have been processed
		defer close(out)

		// partition all input by pattern and send text substring to available consumer through channel
		PartitionText(pat, rdr,
			func(str string) {
				out <- str
			})
	}

	// launch single producer goroutine
	go textProducer(pat, inp, out)

	return out
}

// PartitionText splits a set of text lines by a pattern and sends individual records
// to a callback. Requiring the input to be a TextBlock channel of trimmed strings,
// generated by CreateTextStreamer, simplifies the code by eliminating the need to
// check for an incomplete pattern at the end.
func PartitionText(pat string, inp <-chan TextBlock, proc func(string)) {

	if pat == "" || inp == nil || proc == nil {
		return
	}

	blk := make(chan string, chanDepth)
	out := make(chan string, chanDepth)
	if blk == nil || out == nil {
		fmt.Fprintf(os.Stderr, "\nERROR: Unable to create text producer channel\n")
		os.Exit(1)
	}

	// single string search uses Boyer-Moore-Horspool algorithm
	srchr := StringSearcher(pat, true, false, false, false, false)

	blockReader := func(inp <-chan TextBlock, blk chan<- string) {

		// close internal channel when all records have been processed
		defer close(blk)

		prevHit := 0

		for text := range inp {

			srchr.Search(string(text[:]), 0,
				func(str, ptn string, pos int) bool {
					if prevHit != pos {
						txt := text[prevHit:pos]
						if txt != "" {
							blk <- string(txt)
						}
						prevHit = pos
					}
					return true
				})

			if prevHit < len(text) {
				txt := text[prevHit:]
				if txt != "" {
					blk <- string(txt)
				}
			}

			prevHit = 0
		}
	}

	blockMerger := func(blk <-chan string, out chan<- string) {

		// close channel when all records have been processed
		defer close(out)

		prev := ""

		for str := range blk {

			if str == "" {
				continue
			}

			// check for block starting with pattern
			if strings.HasPrefix(str, pat) {
				if prev != "" {
					// send previous buffer
					out <- prev
					// clear buffer
					prev = ""
				}
			}

			// add current block to buffer
			prev += str
		}

		if prev != "" {
			// send last buffer
			out <- prev
		}
	}

	// launch single block reader goroutine
	go blockReader(inp, blk)

	// launch single block merger goroutine
	go blockMerger(blk, out)

	// drain channel and send results to callback
	for str := range out {
		proc(str[:])
	}
}
