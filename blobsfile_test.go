package blobsfile

import (
	"bytes"
	"crypto/rand"
	mrand "math/rand"
	"os"
	"reflect"
	"sort"
	"sync"
	"testing"

	_ "a4.io/blobstash/pkg/backend"
	pblob "a4.io/blobstash/pkg/blob"
	"a4.io/blobstash/pkg/hashutil"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func BenchmarkBlobsFilePut512B(b *testing.B) {
	back, err := New("./tmp_blobsfile_test", 0, false, sync.WaitGroup{})
	check(err)
	defer back.Close()
	defer os.RemoveAll("./tmp_blobsfile_test")
	benchmarkBlobsFilePut(back, 512, b)
}

func BenchmarkBlobsFilePut512KB(b *testing.B) {
	back, err := New("./tmp_blobsfile_test", 0, false, sync.WaitGroup{})
	check(err)
	defer back.Close()
	defer os.RemoveAll("./tmp_blobsfile_test")
	benchmarkBlobsFilePut(back, 512000, b)
}

func BenchmarkBlobsFilePut2MB(b *testing.B) {
	back, err := New("./tmp_blobsfile_test", 0, false, sync.WaitGroup{})
	check(err)
	defer back.Close()
	defer os.RemoveAll("./tmp_blobsfile_test")
	benchmarkBlobsFilePut(back, 2000000, b)
}

func BenchmarkBlobsFilePut512BCompressed(b *testing.B) {
	back, err := New("./tmp_blobsfile_test", 0, true, sync.WaitGroup{})
	check(err)
	defer back.Close()
	defer os.RemoveAll("./tmp_blobsfile_test")
	benchmarkBlobsFilePut(back, 512, b)
}

func BenchmarkBlobsFilePut512KBCompressed(b *testing.B) {
	back, err := New("./tmp_blobsfile_test", 0, true, sync.WaitGroup{})
	check(err)
	defer back.Close()
	defer os.RemoveAll("./tmp_blobsfile_test")
	benchmarkBlobsFilePut(back, 512000, b)
}

func BenchmarkBlobsFilePut2MBCompressed(b *testing.B) {
	back, err := New("./tmp_blobsfile_test", 0, true, sync.WaitGroup{})
	check(err)
	defer back.Close()
	defer os.RemoveAll("./tmp_blobsfile_test")
	benchmarkBlobsFilePut(back, 2000000, b)
}

func benchmarkBlobsFilePut(back *BlobsFileBackend, blobSize int, b *testing.B) {
	b.ResetTimer()
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		h, blob := randBlob(blobSize)
		b.StartTimer()
		if err := back.Put(h, blob); err != nil {
			panic(err)
		}
		b.StopTimer()
	}
	b.SetBytes(int64(blobSize))
}

func TestBlobsFileReedSolomon(t *testing.T) {
	b, err := New("./tmp_blobsfile_test", 16000000, false, sync.WaitGroup{})
	check(err)
	defer os.RemoveAll("./tmp_blobsfile_test")
	testParity(t, b, nil)
	fname := b.filename(0)
	b.Close()
	// Corrupt the file
	f, err := os.OpenFile(fname, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		panic(err)
	}
	// FIXME(tsileo): test this
	// if _, err := f.Seek(defaultMaxBlobsFileSize/10*3, os.SEEK_SET); err != nil {
	// if _, err := f.Seek(defaultMaxBlobsFileSize/10, os.SEEK_SET); err != nil {
	if _, err := f.Seek(1000, os.SEEK_SET); err != nil {
		panic(err)
	}
	f.Write([]byte("blobsfilelol"))
	f.Sync()
	f.Close()
	// Reopen the db
	b, err = New("./tmp_blobsfile_test", 16000000, false, sync.WaitGroup{})
	check(err)
	defer b.Close()
	// Ensure we can recover from this corruption
	cb := func(err error) error {
		return b.checkBlobsFile(0)
	}
	testParity(t, b, cb)
}

func TestBlobsFileReedSolomonWithCompression(t *testing.T) {
	b, err := New("./tmp_blobsfile_test", 16000000, true, sync.WaitGroup{})
	check(err)
	defer b.Close()
	defer os.RemoveAll("./tmp_blobsfile_test")
	testParity(t, b, nil)
}

func testParity(t *testing.T, b *BlobsFileBackend, cb func(error) error) {
	for i := 0; i < 31+10; i++ {
		h, blob := randBlob(512000)
		if err := b.Put(h, blob); err != nil {
			panic(err)
		}

	}
	if err := b.checkParityBlobs(0); err != nil {
		if cb == nil {
			panic(err)
		}
		if err := cb(err); err != nil {
			panic(err)
		}
	}
}

func randBlob(size int) (string, []byte) {
	blob := make([]byte, size)
	if _, err := rand.Read(blob); err != nil {
		panic(err)
	}
	return hashutil.Compute(blob), blob
}

func TestBlobsFileBlobPutGetEnumerate(t *testing.T) {
	b, err := New("./tmp_blobsfile_test", 0, false, sync.WaitGroup{})
	check(err)
	defer os.RemoveAll("./tmp_blobsfile_test")
	hashes, blobs := testBackendPutGetEnumerate(t, b, 50)
	b.Close()
	// Test we can still read everything when closing/reopening the blobsfile
	b, err = New("./tmp_blobsfile_test", 0, false, sync.WaitGroup{})
	check(err)
	testBackendEnumerate(t, b, hashes)
	testBackendGet(t, b, hashes, blobs)
	b.Close()
	b.RemoveIndex()
	// Try with the index and removed and test re-indexing
	b, err = New("./tmp_blobsfile_test", 0, false, sync.WaitGroup{})
	check(err)
	testBackendEnumerate(t, b, hashes)
	testBackendGet(t, b, hashes, blobs)
}

func backendPut(t *testing.T, b *BlobsFileBackend, blobsCount int) ([]string, [][]byte) {
	blobs := [][]byte{}
	hashes := []string{}
	// TODO(tsileo): 50 blobs if in short mode
	for i := 0; i < blobsCount; i++ {
		h, blob := randBlob(mrand.Intn(4000000-32) + 32)
		hashes = append(hashes, h)
		blobs = append(blobs, blob)
		if err := b.Put(h, blob); err != nil {
			panic(err)
		}
	}
	return hashes, blobs
}

func testBackendPutGetEnumerate(t *testing.T, b *BlobsFileBackend, blobsCount int) ([]string, [][]byte) {
	hashes, blobs := backendPut(t, b, blobsCount)
	testBackendGet(t, b, hashes, blobs)
	testBackendEnumerate(t, b, hashes)
	return hashes, blobs
}

func testBackendGet(t *testing.T, b *BlobsFileBackend, hashes []string, blobs [][]byte) {
	blobsIndex := map[string]bool{}
	for _, blob := range blobs {
		blobsIndex[hashutil.Compute(blob)] = true
	}
	for _, h := range hashes {
		if _, err := b.Get(h); err != nil {
			panic(err)
		}
		_, ok := blobsIndex[h]
		if !ok {
			t.Errorf("blob %s should be index", h)
		}
		delete(blobsIndex, h)
	}
	if len(blobsIndex) > 0 {
		t.Errorf("index should have been emptied, got len %d", len(blobsIndex))
	}
}

func testBackendEnumerate(t *testing.T, b *BlobsFileBackend, hashes []string) []string {
	sort.Strings(hashes)
	bchan := make(chan *pblob.SizedBlobRef)
	errc := make(chan error, 1)
	go func() {
		errc <- b.Enumerate(bchan, "", "\xff", 0)
	}()
	enumHashes := []string{}
	for ref := range bchan {
		enumHashes = append(enumHashes, ref.Hash)
	}
	if err := <-errc; err != nil {
		panic(err)
	}
	if !sort.StringsAreSorted(enumHashes) {
		t.Errorf("enum hashes should already be sorted")
	}
	if !reflect.DeepEqual(hashes, enumHashes) {
		t.Errorf("bad enumerate results")
	}
	return enumHashes
}

func TestBlobsFileBlobEncoding(t *testing.T) {
	b, err := New("./tmp_blobsfile_test", 0, false, sync.WaitGroup{})
	check(err)
	defer b.Close()
	defer os.RemoveAll("./tmp_blobsfile_test")
	_, blob := randBlob(512)
	_, data := b.encodeBlob(blob, Blob)
	size, blob2, f := b.decodeBlob(data)
	if f != Blob {
		t.Errorf("bad flag, got %v, expected %v", f, Blob)
	}
	if size != 512 || !bytes.Equal(blob, blob2) {
		t.Errorf("Error blob encoding, got size:%v, expected:512, got blob:%v, expected:%v", size, blob2[:10], blob[:10])
	}
}
