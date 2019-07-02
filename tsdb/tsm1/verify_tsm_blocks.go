package tsm1

import (
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/influxdata/influxdb/tsdb/cursors"
)

type VerifyTSMBlocks struct {
	Stderr io.Writer
	Stdout io.Writer
	Path   string

	total       int
	totalErrors int
}

func (v *VerifyTSMBlocks) Run() error {
	file, err := os.OpenFile(v.Path, os.O_RDONLY, 0600)
	if err != nil {
		fmt.Fprintf(v.Stderr, "error: %s: %v. Exiting.\n", v.Path, err)
		return err
	}

	reader, err := NewTSMReader(file)
	if err != nil {
		fmt.Fprintf(v.Stderr, "error: %s: %v. Exiting.\n", v.Path, err)
		return err
	}

	defer reader.Close()

	var ts cursors.TimestampArray
	count := 0
	iter := reader.BlockIterator()
	for iter.Next() {
		v.total++
		key, min, max, _, checksum, buf, err := iter.Read()
		if err != nil {
			fmt.Fprintf(v.Stderr, "could not read block %d due to error: %q\n", count, err)
			count++
			continue
		}

		if expected := crc32.ChecksumIEEE(buf); checksum != expected {
			v.totalErrors++
			fmt.Fprintf(v.Stderr, "unexpected checksum %d, expected %d for key %v, block %d\n", checksum, expected, key, count)
		}

		if err = DecodeTimestampArrayBlock(buf, &ts); err != nil {
			v.totalErrors++
			fmt.Fprintf(v.Stderr, "unable to decode timestamps for block %d: %q\n", count, err)
		}

		if expected := ts.MinTime(); min != expected {
			v.totalErrors++
			fmt.Fprintf(v.Stderr, "unexpected min time %d, expected %d for block %d: %q\n", min, expected, count, err)
		}
		if expected := ts.MaxTime(); max != expected {
			v.totalErrors++
			fmt.Fprintf(v.Stderr, "unexpected max time %d, expected %d for block %d: %q\n", max, expected, count, err)
		}

		count++
	}

	return nil
}
