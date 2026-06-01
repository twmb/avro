package ocf_test

import (
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/twmb/avro/ocf"
)

// TestDifferentialOCFCorpus decodes the real, Java-produced OCF files shipped
// in Apache Avro's share/test/data and checks the decoded records against the
// known contents of weather.json. This proves twmb reads actual reference
// output across every codec it supports (null/deflate/snappy/zstd) and that
// the decoded VALUES are correct — an external oracle, not the author's
// belief. See CORRECTNESS_PLAN.md §T1a'.
//
// The corpus is vendored at ocf/testdata/avro-share (see its PROVENANCE.md),
// so this runs by default with no external dependency. Point
// AVRO_SHARE_DATA at a live <apache-avro>/share/test/data clone to run
// against upstream instead.
type weatherRec struct {
	Station string `avro:"station"`
	Time    int64  `avro:"time"`
	Temp    int32  `avro:"temp"`
}

func sortWeather(r []weatherRec) {
	sort.Slice(r, func(i, j int) bool {
		if r[i].Station != r[j].Station {
			return r[i].Station < r[j].Station
		}
		return r[i].Time < r[j].Time
	})
}

func TestDifferentialOCFCorpus(t *testing.T) {
	dir := os.Getenv("AVRO_SHARE_DATA")
	if dir == "" {
		dir = filepath.Join("testdata", "avro-share") // vendored corpus
	}
	if _, err := os.Stat(dir); err != nil {
		t.Skipf("OCF corpus dir %q not present: %v", dir, err)
	}

	// Ground truth from weather.json (the records the OCF files encode).
	want := []weatherRec{
		{"011990-99999", -619524000000, 0},
		{"011990-99999", -619506000000, 22},
		{"011990-99999", -619484400000, -11},
		{"012650-99999", -655531200000, 111},
		{"012650-99999", -655509600000, 78},
	}

	readWeather := func(t *testing.T, name string) []weatherRec {
		f, err := os.Open(filepath.Join(dir, name))
		if err != nil {
			t.Skipf("corpus file %s not present: %v", name, err)
		}
		defer f.Close()
		r, err := ocf.NewReader(f)
		if err != nil {
			t.Fatalf("%s: NewReader: %v", name, err)
		}
		var got []weatherRec
		for {
			var rec weatherRec
			if err := r.Decode(&rec); err != nil {
				if err == io.EOF {
					break
				}
				t.Fatalf("%s: Decode: %v", name, err)
			}
			got = append(got, rec)
		}
		return got
	}

	// Codec variants: records decode in writer order, exactly matching the
	// Java-produced ground truth.
	for _, name := range []string{"weather.avro", "weather-deflate.avro", "weather-snappy.avro", "weather-zstd.avro"} {
		t.Run(name, func(t *testing.T) {
			got := readWeather(t, name)
			if !reflect.DeepEqual(got, want) {
				t.Errorf("%s decoded\n got  %+v\n want %+v", name, got, want)
			}
		})
	}

	// Sorted variant: same records, writer reordered them — compare as a set.
	t.Run("weather-sorted.avro", func(t *testing.T) {
		got := readWeather(t, "weather-sorted.avro")
		sortWeather(got)
		w := append([]weatherRec(nil), want...)
		sortWeather(w)
		if !reflect.DeepEqual(got, w) {
			t.Errorf("weather-sorted.avro decoded (sorted)\n got  %+v\n want %+v", got, w)
		}
	})

	// syncInMeta.avro carries the sync marker in the file metadata (a
	// different schema); just confirm twmb reads every record without error.
	t.Run("syncInMeta.avro", func(t *testing.T) {
		f, err := os.Open(filepath.Join(dir, "syncInMeta.avro"))
		if err != nil {
			t.Skipf("syncInMeta.avro not present: %v", err)
		}
		defer f.Close()
		r, err := ocf.NewReader(f)
		if err != nil {
			t.Fatalf("syncInMeta: NewReader: %v", err)
		}
		n := 0
		for {
			var v any
			if err := r.Decode(&v); err != nil {
				if err == io.EOF {
					break
				}
				t.Fatalf("syncInMeta: Decode: %v", err)
			}
			n++
		}
		t.Logf("syncInMeta.avro: decoded %d records", n)
	})
}
