/*

Package blobsfile implement the BlobsFile backend for storing blobs.

It stores multiple blobs (optionally compressed with Snappy) inside "BlobsFile"/fat file/packed file
(256MB by default).
Blobs are indexed by a kv file.

New blobs are appended to the current file, and when the file exceed the limit, a new fie is created.

Blobs are stored with its hash and its size (for a total overhead of 32 bytes) followed by the blob itself, thus allowing re-indexing.

	Blob hash (32 bytes) + Flag (1 byte) + Blob size (4 byte, uint32 binary encoded) + Blob data

Blobs are indexed by a BlobPos entry (value stored as string):

	Blob Hash => n (BlobFile index) + (space) + offset + (space) + Blob size

FIXME(tsileo): switch to canonical imports a4.io/blobsfile

*/
package blobsfile

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"expvar"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/dchest/blake2b"
	"github.com/golang/snappy"
	"github.com/klauspost/reedsolomon"
	log2 "gopkg.in/inconshreveable/log15.v2"

	"a4.io/blobstash/pkg/blob"
	"a4.io/blobstash/pkg/logger"
)

// TODO(tsileo): for blobs file not yet filled, propose a manual way to correct errors (like repl or s3 in blobstash?)
// TODO(tsileo): embed the version in the header
// TODO(tsileo): travis CI + README + badge godoc/travis

const (
	headerMagic = "\x00Blobs"
	headerSize  = len(headerMagic) + 58 // magic + 58 reserved bytes

	// 38 bytes of meta-data are stored for each blob: 32 byte hash + 2 byte flag + 4 byte blob len
	blobOverhead = 38
	hashSize     = 32

	// Reed-Solomon config
	parityShards = 2  // 2 parity shards
	dataShards   = 10 // 10 data shards

	defaultMaxBlobsFileSize = 256 << 20 // 256MB
)

// Blob flags
const (
	flagBlob byte = 1 << iota
	flagCompressed
	flagParityBlob
	flagEOF
)

// Compression algorithms flag
const (
	flagSnappy byte = 1 << iota
)

var (
	openFdsVar      = expvar.NewMap("blobsfile-open-fds")
	bytesUploaded   = expvar.NewMap("blobsfile-bytes-uploaded")
	bytesDownloaded = expvar.NewMap("blobsfile-bytes-downloaded")
	blobsUploaded   = expvar.NewMap("blobsfile-blobs-uploaded")
	blobsDownloaded = expvar.NewMap("blobsfile-blobs-downloaded")
)

var (
	// ErrBlobNotFound reports that the blob could not be found
	ErrBlobNotFound = errors.New("blob not found")

	// ErrBlobsfileCorrupted reports that one of the BlobsFile is corrupted and could not be repaired
	ErrBlobsfileCorrupted = errors.New("blobsfile is corrupted")

	errParityBlobCorrupted = errors.New("a parity blob is corrupted")
)

// corruptedError give more about the corruption of a BlobsFile
type corruptedError struct {
	blobs  []*blobPos
	offset int64
	err    error
}

func (ce *corruptedError) Error() string {
	if len(ce.blobs) > 0 {
		return fmt.Sprintf("%d blobs are corrupt", len(ce.blobs))
	}
	return fmt.Sprintf("corrupted at offset %d: %v", ce.offset, ce.err)
}

func (ce *corruptedError) firstBadOffset() int64 {
	if len(ce.blobs) > 0 {
		off := int64(ce.blobs[0].offset)
		if ce.offset == -1 || off < ce.offset {
			return off
		}
	}
	return ce.offset
}

func firstCorruptedShard(offset int64, shardSize int) int {
	i := 0
	ioffset := int(offset)
	for j := 0; j < dataShards; j++ {
		if shardSize+(shardSize*i) > ioffset {
			return i
		}
		i++
	}
	return 0
}

// Stats represents some stats about the DB state
type Stats struct {
	// The total number of blobs stored
	BlobsCount int

	// The size of all the blobs stored
	BlobsSize int64

	// The number of BlobsFile
	BlobsFilesCount int

	// The size of all the BlobsFile
	BlobsFilesSize int64
}

// Opts represents the DB options
type Opts struct {
	// The max size of a BlobsFile, will be 256MB by default if not set
	BlobsFileSize int64

	// Where the data and indexes will be stored
	Directory string

	// Compression is enabled by default
	DisableCompression bool
}

func (o *Opts) init() {
	if o.BlobsFileSize == 0 {
		o.BlobsFileSize = defaultMaxBlobsFileSize
	}
}

// BlobsFiles represent the DB
type BlobsFiles struct {
	log log2.Logger
	// Directory which holds the blobsfile
	directory string

	// Maximum size for a blobsfile (256MB by default)
	maxBlobsFileSize int64

	// Backend state
	reindexMode bool

	// Compression is disabled by default
	snappyCompression bool

	// The kv index that maintains blob positions
	index *blobsIndex

	// Current blobs file opened for write
	n       int
	current *os.File
	// Size of the current blobs file
	size int64
	// All blobs files opened for read
	files map[int]*os.File

	lastErr      error
	lastErrMutex sync.Mutex // mutex for guarding the lastErr

	rse    reedsolomon.Encoder
	wg     sync.WaitGroup
	putErr chan error
	sync.Mutex
}

// New intializes a new BlobsFileBackend.
func New(opts *Opts) (*BlobsFiles, error) {
	opts.init()
	dir := opts.Directory
	// Try to create the directory
	os.MkdirAll(dir, 0700)
	var reindex bool
	// Check if an index file is already present
	if _, err := os.Stat(filepath.Join(dir, "blobs-index")); os.IsNotExist(err) {
		// No index found
		reindex = true
	}
	index, err := newIndex(dir)
	if err != nil {
		return nil, err
	}

	// Initialize the Reed-Solomon encoder
	enc, err := reedsolomon.New(dataShards, parityShards)
	if err != nil {
		return nil, err
	}

	backend := &BlobsFiles{
		directory:         dir,
		snappyCompression: !opts.DisableCompression,
		index:             index,
		files:             make(map[int]*os.File),
		maxBlobsFileSize:  opts.BlobsFileSize,
		rse:               enc,
		reindexMode:       reindex,
		putErr:            make(chan error, 2),
	}
	backend.log = logger.Log.New("backend", backend.String())
	backend.log.Debug("Started")
	if err := backend.load(); err != nil {
		panic(fmt.Errorf("Error loading %T: %v", backend, err))
	}
	// TODO(tsileo): fix the backend and prepare for multiple compressions algs
	if backend.snappyCompression {
		backend.log.Debug("snappy compression enabled")
	}
	return backend, nil
}

func (backend *BlobsFiles) iterOpenFiles() (files []*os.File) {
	for _, f := range backend.files {
		files = append(files, f)
	}
	return files
}

func (backend *BlobsFiles) closeOpenFiles() {
	for _, f := range backend.files {
		f.Close()
	}
}

// Stats returns some stats about the DB.
func (backend *BlobsFiles) Stats() (*Stats, error) {
	// Iterate the index to gather the stats
	bchan := make(chan *blob.SizedBlobRef)
	errc := make(chan error, 1)
	go func() {
		errc <- backend.Enumerate(bchan, "", "\xff", 0)
	}()
	blobsCount := 0
	var blobsSize int64
	for ref := range bchan {
		blobsCount++
		blobsSize += int64(ref.Size)
	}
	if err := <-errc; err != nil {
		panic(err)
	}

	backend.Lock()
	defer backend.Unlock()
	var bfs int64
	for _, f := range backend.iterOpenFiles() {
		finfo, err := f.Stat()
		if err != nil {
			return nil, err
		}
		bfs += finfo.Size()
	}
	n, err := backend.getN()
	if err != nil {
		return nil, err
	}

	return &Stats{
		BlobsFilesCount: n + 1,
		BlobsFilesSize:  bfs,
		BlobsCount:      blobsCount,
		BlobsSize:       blobsSize,
	}, nil
}

func (backend *BlobsFiles) setLastError(err error) {
	backend.lastErrMutex.Lock()
	defer backend.lastErrMutex.Unlock()
	backend.lastErr = err
}

// lastError returns the last error that may have happened in asynchronous way (like the parity blobs writing process).
func (backend *BlobsFiles) lastError() error {
	backend.lastErrMutex.Lock()
	defer backend.lastErrMutex.Unlock()
	if backend.lastErr == nil {
		return nil
	}
	err := backend.lastErr
	backend.lastErr = nil
	return err
}

// Close closes all the indexes and data files.
func (backend *BlobsFiles) Close() (err error) {
	backend.wg.Wait()
	if err := backend.lastError(); err != nil {
		err = err
	}
	backend.log.Debug("closing index...")
	if err := backend.index.Close(); err != nil {
		err = err
	}
	return
}

// RemoveIndex removes the index files (which will be rebuilt next time the DB is open).
func (backend *BlobsFiles) RemoveIndex() error {
	return backend.index.remove()
}

// getN returns the total numbers of BlobsFile.
func (backend *BlobsFiles) getN() (int, error) {
	return backend.index.getN()
}

func (backend *BlobsFiles) saveN() error {
	return backend.index.setN(backend.n)
}

func (backend *BlobsFiles) restoreN() error {
	n, err := backend.index.getN()
	if err != nil {
		return err
	}
	backend.n = n
	return nil
}

// String implements the Stringer interface.
func (backend *BlobsFiles) String() string {
	return fmt.Sprintf("blobsfile-%v", backend.directory)
}

func (backend *BlobsFiles) scanBlobsFile(n int, iterFunc func(*blobPos, byte, string, []byte) error) error {
	corrupted := []*blobPos{}

	err := backend.ropen(n)
	if err != nil {
		return err
	}

	offset := int64(headerSize)
	blobsfile := backend.files[n]
	if _, err := blobsfile.Seek(int64(headerSize), os.SEEK_SET); err != nil {
		return err
	}

	blobsIndexed := 0

	blobHash := make([]byte, hashSize)
	blobSizeEncoded := make([]byte, 4)
	flags := make([]byte, 2)

	for {
		// fmt.Printf("blobsIndexed=%d\noffset=%d\n", blobsIndexed, offset)
		// SCAN
		if _, err := blobsfile.Read(blobHash); err != nil {
			if err == io.EOF {
				break
			}
			return &corruptedError{nil, offset, fmt.Errorf("failed to read hash: %v", err)}
		}
		if _, err := blobsfile.Read(flags); err != nil {
			return &corruptedError{nil, offset, fmt.Errorf("failed to read flag: %v", err)}
		}
		// If we reached the EOF blob, break
		if flags[0] == flagEOF {
			break
		}
		if _, err := blobsfile.Read(blobSizeEncoded); err != nil {
			return &corruptedError{nil, offset, fmt.Errorf("failed to read blob size: %v", err)}
		}
		blobSize := int64(binary.LittleEndian.Uint32(blobSizeEncoded))
		rawBlob := make([]byte, int(blobSize))
		read, err := blobsfile.Read(rawBlob)
		if err != nil || read != int(blobSize) {
			return &corruptedError{nil, offset, fmt.Errorf("error while reading raw blob: %v", err)}
		}
		// XXX(tsileo): optional flag to `BlobPos`?
		blobPos := &blobPos{n: n, offset: offset, size: int(blobSize)}
		offset += blobOverhead + blobSize
		var blob []byte
		if backend.snappyCompression {
			blobDecoded, err := snappy.Decode(nil, rawBlob)
			if err != nil {
				return &corruptedError{nil, offset, fmt.Errorf("failed to decode blob: %v %v %v", err, blobSize, flags)}
			}
			blob = blobDecoded
		} else {
			blob = rawBlob
		}
		// Store the real blob size (i.e. the decompressed size if the data is compressed)
		blobPos.blobSize = len(blob)

		hash := fmt.Sprintf("%x", blake2b.Sum256(blob))
		if fmt.Sprintf("%x", blobHash) == hash {
			if iterFunc != nil {
				if err := iterFunc(blobPos, flags[0], hash, blob); err != nil {
					return err
				}
			}
			blobsIndexed++
			// FIXME(tsileo): continue an try to repair it?
		} else {
			// better out an error and provides a CLI for repairing
			fmt.Printf("corrupted\n")
			backend.log.Error(fmt.Sprintf("hash doesn't match %v/%v", fmt.Sprintf("%x", blobHash), hash))
			corrupted = append(corrupted, blobPos)
		}
	}

	if len(corrupted) > 0 {
		return &corruptedError{corrupted, -1, nil}
	}

	return nil
}

func copyShards(i [][]byte) (o [][]byte) {
	for _, a := range i {
		o = append(o, a)
	}
	return o
}

func (backend *BlobsFiles) checkBlobsFile(n int) error {
	pShards, err := backend.parityShards(n)
	if err != nil {
		// TODO(tsileo): log the error
		fmt.Printf("parity shards err=%v\n", err)
	}
	parityCnt := len(pShards)
	err = backend.scanBlobsFile(n, nil)
	fmt.Printf("scan result=%v\n", err)
	if err == nil && (pShards == nil || len(pShards) != parityShards) {
		// We can rebuild the parity blobs if needed
		// FIXME(tsileo): do it
	}
	if err != nil && (pShards == nil || len(pShards) == 0) {
		return fmt.Errorf("no parity shards available, can't recover")
	}
	if err == nil {
		fmt.Printf("noting to repair")
		return nil
	}
	if pShards == nil || len(pShards) != parityShards {
		var l int
		if pShards != nil {
			l = len(pShards)
		} else {
			pShards = [][]byte{}
		}

		for i := 0; i < parityShards-l; i++ {
			pShards = append(pShards, nil)
		}
	}
	dataShardIndex := 0
	if err != nil {
		if cerr, ok := err.(*corruptedError); ok {
			badOffset := cerr.firstBadOffset()
			fmt.Printf("badOffset: %v\n", badOffset)
			dataShardIndex = firstCorruptedShard(badOffset, int(backend.maxBlobsFileSize)/dataShards)
			fmt.Printf("dataShardIndex=%d\n", dataShardIndex)
		}
	}
	missing := []int{}
	for i := dataShardIndex; i < 10; i++ {
		missing = append(missing, i)
	}
	fmt.Printf("missing=%+v\n", missing)
	dShards, err := backend.dataShards(n)
	if err != nil {
		return err
	}
	fmt.Printf("try #1\n")
	if len(missing) <= parityCnt {
		shards := copyShards(append(dShards, pShards...))
		for _, idx := range missing {
			shards[idx] = nil
		}
		if err := backend.rse.Reconstruct(shards); err != nil {
			return err
		}
		ok, err := backend.rse.Verify(shards)
		if err != nil {
			return err
		}
		if ok {
			// FIXME(tsileo): update/fix the data
			fmt.Printf("reconstruct successful\n")
			return nil
		}
		return fmt.Errorf("unrecoverable corruption")
	}

	fmt.Printf("try #2\n")
	// Try one missing shards
	for i := dataShardIndex; i < 10; i++ {
		shards := copyShards(append(dShards, pShards...))
		shards[i] = nil
		if err := backend.rse.Reconstruct(shards); err != nil {
			return err
		}
		ok, err := backend.rse.Verify(shards)
		if err != nil {
			return err
		}
		if ok {
			// FIXME(tsileo): update/fix the data
			fmt.Printf("reconstruct successful\n")
			return nil
		}
	}

	// TODO(tsileo): only do this check if the two parity blobs are here
	fmt.Printf("try #3\n")
	if len(pShards) >= 2 {
		for i := dataShardIndex; i < 10; i++ {
			for j := dataShardIndex; j < 10; j++ {
				if j == i {
					continue
				}
				shards := copyShards(append(dShards, pShards...))
				// fmt.Printf("setting i=%d,j=%d to nil\n", i, j)
				shards[i] = nil
				shards[j] = nil
				if err := backend.rse.Reconstruct(shards); err != nil {
					return err
				}
				ok, err := backend.rse.Verify(shards)
				if err != nil {
					return err
				}
				if ok {
					// FIXME(tsileo): update/fix the data
					fmt.Printf("reconstruct successful\n")
					return nil
				}
			}
		}
	}

	// XXX(tsileo): support for 4 failed parity shards
	return fmt.Errorf("failed to recover")
}

func (backend *BlobsFiles) dataShards(n int) ([][]byte, error) {
	// Read the whole blobsfile data (except the parity blobs)
	data := make([]byte, backend.maxBlobsFileSize)
	if _, err := backend.files[n].ReadAt(data, 0); err != nil {
		return nil, err
	}

	// Rebuild the data shards using the data part of the blobsfile
	shards, err := backend.rse.Split(data)
	if err != nil {
		return nil, err
	}
	return shards[:10], nil
}

func (backend *BlobsFiles) parityShards(n int) ([][]byte, error) {
	// FIXME(tsileo): try to read them backward if it fails (as we know the size will be maxBlobsFileSize/10), don't forget to reorder them
	// Read the 2 parity shards at the ends of the file
	if _, err := backend.files[n].Seek(backend.maxBlobsFileSize, os.SEEK_SET); err != nil {
		return nil, fmt.Errorf("failed to seek to parity shards: %v", err)
	}
	blobsfile := backend.files[n]
	parityBlobs := [][]byte{}

	blobHash := make([]byte, hashSize)
	blobSizeEncoded := make([]byte, 4)
	flags := make([]byte, 2)

	for i := 0; i < parityShards; i++ {
		if _, err := blobsfile.Read(blobHash); err == io.EOF {
			return parityBlobs, fmt.Errorf("missing parity blob, only found %d", len(parityBlobs))
		}
		if _, err := blobsfile.Read(flags); err != nil {
			return parityBlobs, fmt.Errorf("failed to read flag: %v", err)
		}
		if _, err := blobsfile.Read(blobSizeEncoded); err != nil {
			return parityBlobs, err
		}
		blobSize := binary.LittleEndian.Uint32(blobSizeEncoded)
		blob := make([]byte, int(blobSize))
		read, err := blobsfile.Read(blob)
		if err != nil || read != int(blobSize) {
			return parityBlobs, fmt.Errorf("error while reading raw blob: %v", err)
		}
		hash := fmt.Sprintf("%x", blake2b.Sum256(blob))
		if fmt.Sprintf("%x", blobHash) != hash {
			return parityBlobs, errParityBlobCorrupted
		}
		parityBlobs = append(parityBlobs, blob)
	}
	return parityBlobs, nil
}

func (backend *BlobsFiles) checkParityBlobs(n int) error {
	dataShards, err := backend.dataShards(n)
	if err != nil {
		return fmt.Errorf("failed to build data shards: %v", err)
	}
	parityShards, err := backend.parityShards(n)
	if err != nil {
		return fmt.Errorf("failed to build parity shards: %v", err)
	}
	shards := append(dataShards, parityShards...)

	// Verify the integrity of the data
	ok, err := backend.rse.Verify(shards)
	if err != nil {
		return fmt.Errorf("failed to verify shards: %v", err)
	}
	if !ok {
		return ErrBlobsfileCorrupted
	}
	return nil
}

func (backend *BlobsFiles) scan(iterFunc func(*blobPos, byte, string, []byte) error) error {
	n := 0
	for {
		err := backend.scanBlobsFile(n, iterFunc)
		if os.IsNotExist(err) {
			break
		}
		if err != nil {
			return err
		}
		n++
	}
	if n == 0 {
		backend.log.Debug("no BlobsFiles found for re-indexing")
		return nil
	}
	return nil
}

// reindex scans all BlobsFile and reconstruct the index from scratch.
func (backend *BlobsFiles) reindex() error {
	backend.wg.Add(1)
	defer backend.wg.Done()
	backend.log.Info("re-indexing BlobsFiles...")
	n := 0
	blobsIndexed := 0

	iterFunc := func(blobPos *blobPos, flag byte, hash string, _ []byte) error {
		// Skip parity blobs
		if flag == flagParityBlob {
			return nil
		}
		if err := backend.index.setPos(hash, blobPos); err != nil {
			return err
		}
		n = blobPos.n
		blobsIndexed++
		return nil
	}

	if err := backend.scan(iterFunc); err != nil {
		return err
	}

	// FIXME(tsileo): check for CorruptedError and initialize a repair

	if n == 0 {
		backend.log.Debug("no BlobsFiles found for re-indexing")
		return nil
	}
	if err := backend.saveN(); err != nil {
		return err
	}
	return nil
}

// Open all the blobs-XXXXX (read-only) and open the last for write
func (backend *BlobsFiles) load() error {
	backend.wg.Add(1)
	defer backend.wg.Done()
	backend.log.Debug("BlobsFileBackend: scanning BlobsFiles...")
	n := 0
	for {
		err := backend.ropen(n)
		if os.IsNotExist(err) {
			// No more blobsfile
			break
		}
		if err != nil {
			return err
		}
		backend.log.Debug("BlobsFile loaded", "name", backend.filename(n))
		n++
	}
	if n == 0 {
		// The dir is empty, create a new blobs-XXXXX file,
		// and open it for read
		if err := backend.wopen(n); err != nil {
			return err
		}
		if err := backend.ropen(n); err != nil {
			return err
		}
		if err := backend.saveN(); err != nil {
			return err
		}
		return nil
	}
	// Open the last file for write
	if err := backend.wopen(n - 1); err != nil {
		return err
	}
	if err := backend.saveN(); err != nil {
		return err
	}
	if backend.reindexMode {
		if err := backend.reindex(); err != nil {
			return err
		}
	}
	return nil
}

// Open a file for writing, will close the previously open file if any.
func (backend *BlobsFiles) wopen(n int) error {
	backend.log.Info("opening blobsfile for writing", "name", backend.filename(n))
	// Close the already opened file if any
	if backend.current != nil {
		if err := backend.current.Close(); err != nil {
			openFdsVar.Add(backend.directory, -1)
			return err
		}
	}
	// Track if we created the file
	created := false
	if _, err := os.Stat(backend.filename(n)); os.IsNotExist(err) {
		created = true
	}

	// Open the file in rw mode
	f, err := os.OpenFile(backend.filename(n), os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	backend.current = f
	backend.n = n
	if created {
		// Write the header/magic number
		if _, err := backend.current.Write([]byte(headerMagic)); err != nil {
			return err
		}
		// Write the reserved bytes
		reserved := make([]byte, 58)
		if _, err := backend.current.Write(reserved); err != nil {
			return err
		}

		// Fsync
		if err = backend.current.Sync(); err != nil {
			panic(err)
		}
	}
	backend.size, err = f.Seek(0, os.SEEK_END)
	if err != nil {
		return err
	}
	openFdsVar.Add(backend.directory, 1)
	return nil
}

// Open a file for read
func (backend *BlobsFiles) ropen(n int) error {
	_, alreadyOpen := backend.files[n]
	if alreadyOpen {
		log.Printf("BlobsFileBackend: blobsfile %v already open", backend.filename(n))
		return nil
	}
	if n > len(backend.files) {
		return fmt.Errorf("Trying to open file %v whereas only %v files currently open", n, len(backend.files))
	}

	filename := backend.filename(n)
	f, err := os.Open(filename)
	if err != nil {
		return err
	}

	// Ensure the header's magic is present
	fmagic := make([]byte, len(headerMagic))
	_, err = f.Read(fmagic)
	if err != nil || headerMagic != string(fmagic) {
		return fmt.Errorf("magic not found in BlobsFile: %v or header not matching", err)
	}

	if _, err := f.Seek(int64(headerSize), os.SEEK_SET); err != nil {
		return err
	}

	backend.files[n] = f
	openFdsVar.Add(backend.directory, 1)

	return nil
}

func (backend *BlobsFiles) filename(n int) string {
	return filepath.Join(backend.directory, fmt.Sprintf("blobs-%05d", n))
}

// writeParityBlobs compute and write the 4 parity shards using Reed-Solomon 10,4 and write them at
// end the blobsfile, and write the "data size" (blobsfile size before writing the parity shards).
func (backend *BlobsFiles) writeParityBlobs(f *os.File, size int) error {
	start := time.Now()

	backend.wg.Add(1)
	defer backend.wg.Done()

	// First we write the padding blob
	paddingLen := backend.maxBlobsFileSize - (int64(size) + blobOverhead)
	headerEOF := makeHeaderEOF(paddingLen)
	n, err := f.Write(headerEOF)
	if err != nil {
		return fmt.Errorf("failed to write EOF header: %v", err)
	}
	size += n

	padding := make([]byte, paddingLen)
	n, err = f.Write(padding)
	if err != nil {
		return fmt.Errorf("failed to write padding 0: %v", err)
	}
	size += n

	// We write the data size at the end of the file
	if _, err := f.Seek(0, os.SEEK_END); err != nil {
		return err
	}

	// Read the whole blobsfile
	fdata := make([]byte, size)
	if _, err := f.ReadAt(fdata, 0); err != nil {
		return err
	}

	// Split into shards
	shards, err := backend.rse.Split(fdata)
	if err != nil {
		return err
	}
	// Create the parity shards
	if err := backend.rse.Encode(shards); err != nil {
		return err
	}

	// Save the parity blobs
	parityBlobs := shards[dataShards:]
	for _, parityBlob := range parityBlobs {
		_, parityBlobEncoded := backend.encodeBlob(parityBlob, flagParityBlob)

		n, err := f.Write(parityBlobEncoded)
		// backend.size += int64(len(parityBlobEncoded))
		if err != nil || n != len(parityBlobEncoded) {
			return fmt.Errorf("Error writing parity blob (%v,%v)", err, n)
		}
	}

	// Fsync
	if err = f.Sync(); err != nil {
		return err
	}

	if err := f.Close(); err != nil {
		return err
	}
	duration := time.Since(start)
	fmt.Printf("parity encoding done in %s\n", duration)
	return nil
}

// Put save a new blob
//
// If the blob is already stored, then Put will be a no-op.
// So it's not necessary to make call Exists before saving a new blob.
func (backend *BlobsFiles) Put(hash string, data []byte) (err error) {
	// Acquire the lock
	backend.Lock()
	defer backend.Unlock()

	backend.wg.Add(1)
	defer backend.wg.Done()

	if err := backend.lastError(); err != nil {
		return err
	}

	// 	// Ensure the data is not already stored
	bhash, err := hex.DecodeString(hash)
	if err != nil {
		return err
	}
	bposdata, err := backend.index.db.Get(nil, formatKey(blobPosKey, bhash))
	if err != nil {
		return fmt.Errorf("error getting BlobPos: %v", err)
	}
	// The index contains data, the blob is already stored
	if bposdata != nil {
		return nil
	}

	// Encode the blob
	blobSize, blobEncoded := backend.encodeBlob(data, flagBlob)

	var newBlobsFileNeeded bool

	// Ensure the blosfile size won't exceed the maxBlobsFileSize
	if backend.size+int64(blobSize+blobOverhead) > backend.maxBlobsFileSize {
		var f *os.File
		f = backend.current
		backend.current = nil
		newBlobsFileNeeded = true

		// This goroutine will write the parity blobs and close the file
		go func(f *os.File, size int) {
			// Write some parity blobs at the end of the blobsfile using Reed-Solomon erasure coding
			if err := backend.writeParityBlobs(f, size); err != nil {
				backend.setLastError(err)
			}
		}(f, int(backend.size))
	}

	// We're writing to two different files, so we can parallelized the writes
	go func() {
		if newBlobsFileNeeded {
			// Archive this blobsfile, start by creating a new one
			backend.n++
			backend.log.Debug("creating a new BlobsFile")
			if err := backend.wopen(backend.n); err != nil {
				panic(err)
			}
			// Re-open it (since we may need to read blobs from it)
			if err := backend.ropen(backend.n); err != nil {
				panic(err)
			}
			// Update the nimber of blobsfile in the index
			if err := backend.saveN(); err != nil {
				panic(err)
			}
		}

		// Save the blob in the BlobsFile
		n, err := backend.current.Write(blobEncoded)
		backend.size += int64(len(blobEncoded))
		if err != nil || n != len(blobEncoded) {
			backend.putErr <- fmt.Errorf("Error writing blob (%v,%v)", err, n)
		}

		// Fsync
		if err = backend.current.Sync(); err != nil {
			backend.putErr <- err
			return
		}

		backend.putErr <- nil
	}()

	go func() {
		// Save the blob in the index
		blobPos := &blobPos{n: backend.n, offset: backend.size, size: blobSize, blobSize: len(data)}
		if err := backend.index.setPos(hash, blobPos); err != nil {
			backend.putErr <- err
			return
		}

		backend.putErr <- nil
	}()

	// Wait for the two goroutines to finish
	for i := 0; i < 2; i++ {
		if err := <-backend.putErr; err != nil {
			return err
		}
	}

	// Update the expvars
	bytesUploaded.Add(backend.directory, int64(len(blobEncoded)))
	blobsUploaded.Add(backend.directory, 1)
	return
}

// Exists return true if the blobs is already stored.
func (backend *BlobsFiles) Exists(hash string) (bool, error) {
	blobPos, err := backend.index.getPos(hash)
	if err != nil {
		return false, err
	}
	if blobPos != nil {
		return true, nil
	}
	return false, nil
}

func (backend *BlobsFiles) decodeBlob(data []byte) (size int, blob []byte, flag byte) {
	flag = data[hashSize]
	compressionAlgFlag := data[hashSize+1]
	size = int(binary.LittleEndian.Uint32(data[hashSize+2 : blobOverhead]))
	blob = make([]byte, size)
	copy(blob, data[blobOverhead:])
	if backend.snappyCompression && flag == flagCompressed && compressionAlgFlag == flagSnappy {
		blobDecoded, err := snappy.Decode(nil, blob)
		if err != nil {
			panic(fmt.Errorf("Failed to decode blob with Snappy: %v", err))
		}
		flag = flagBlob
		blob = blobDecoded
	}
	h := blake2b.New256()
	h.Write(blob)
	if !bytes.Equal(h.Sum(nil), data[0:hashSize]) {
		panic(fmt.Errorf("Hash doesn't match %x != %x", h.Sum(nil), data[0:hashSize]))
	}
	return
}

func makeHeaderEOF(padSize int64) (h []byte) {
	// Write a hash with only zeroes
	h = make([]byte, blobOverhead)
	// EOF flag, empty second flag
	h[32] = flagEOF
	binary.LittleEndian.PutUint32(h[34:], uint32(padSize))
	return
}

func (backend *BlobsFiles) encodeBlob(blob []byte, flag byte) (size int, data []byte) {
	h := blake2b.New256()
	h.Write(blob)

	var compressionAlgFlag byte
	// Only compress regular blobs
	if backend.snappyCompression && flag == flagBlob {
		dataEncoded := snappy.Encode(nil, blob)
		flag = flagCompressed
		blob = dataEncoded
		compressionAlgFlag = flagSnappy
	}

	size = len(blob)
	data = make([]byte, len(blob)+blobOverhead)

	copy(data[:], h.Sum(nil))

	// set the flag
	data[hashSize] = flag
	data[hashSize+1] = compressionAlgFlag

	binary.LittleEndian.PutUint32(data[hashSize+2:], uint32(size))

	copy(data[blobOverhead:], blob)

	return
}

// BlobPos return the index entry for the given hash
func (backend *BlobsFiles) blobPos(hash string) (*blobPos, error) {
	return backend.index.getPos(hash)
}

// Get returns the blob fur the given hash
func (backend *BlobsFiles) Get(hash string) ([]byte, error) {
	if err := backend.lastError(); err != nil {
		return nil, err
	}

	// Fetch the index entry
	blobPos, err := backend.index.getPos(hash)
	if err != nil {
		return nil, fmt.Errorf("Error fetching GetPos: %v", err)
	}

	// No index entry found, returns an error
	if blobPos == nil {
		if err == nil {
			return nil, ErrBlobNotFound
		}
		return nil, err
	}

	// Read the encoded blob from the BlobsFile
	data := make([]byte, blobPos.size+blobOverhead)
	n, err := backend.files[blobPos.n].ReadAt(data, int64(blobPos.offset))
	if err != nil {
		return nil, fmt.Errorf("Error reading blob: %v / blobsfile: %+v", err, backend.files[blobPos.n])
	}

	// Ensure the data length is expcted
	if n != blobPos.size+blobOverhead {
		return nil, fmt.Errorf("Error reading blob %v, read %v, expected %v+%v", hash, n, blobPos.size, blobOverhead)
	}

	// Decode the blob
	blobSize, blob, _ := backend.decodeBlob(data)
	if blobSize != blobPos.size {
		return nil, fmt.Errorf("Bad blob %v encoded size, got %v, expected %v", hash, n, blobSize)
	}

	// Update the expvars
	bytesDownloaded.Add(backend.directory, int64(blobSize))
	blobsUploaded.Add(backend.directory, 1)

	return blob, nil
}

// Enumerate output all the blobs into the given chan (ordered lexicographically)
// TODO(tsileo) take a callback func(hash string, size int) error
func (backend *BlobsFiles) Enumerate(blobs chan<- *blob.SizedBlobRef, start, end string, limit int) error {
	defer close(blobs)
	backend.Lock()
	defer backend.Unlock()

	if err := backend.lastError(); err != nil {
		return err
	}

	// TODO(tsileo) send the size along the hashes ?
	// fmt.Printf("start=%v/%+v\n", start, formatKey(BlobPosKey, []byte(start)))
	s, err := hex.DecodeString(start)
	if err != nil {
		return err
	}
	enum, _, err := backend.index.db.Seek(formatKey(blobPosKey, s))
	// endBytes := formatKey(BlobPosKey, []byte(end))
	endBytes := []byte(end)
	// formatKey(BlobPosKey, []byte(end))
	if err != nil {
		return err
	}
	i := 0
	for {
		k, _, err := enum.Next()
		if err == io.EOF {
			break
		}
		hash := hex.EncodeToString(k[1:])
		// fmt.Printf("\n\n\nhash=%s\nend=%s\ncmp=%v\n\n\n", hash, endBytes, bytes.Compare([]byte(hash), endBytes))
		if bytes.Compare([]byte(hash), endBytes) > 0 || (limit != 0 && i > limit) {
			return nil
		}
		blobPos, err := backend.blobPos(hash)
		if err != nil {
			return nil
		}
		// Remove the BlobPosKey prefix byte
		sbr := &blob.SizedBlobRef{
			Hash: hex.EncodeToString(k[1:]),
			Size: blobPos.blobSize,
		}
		blobs <- sbr
		i++
	}
	return nil
}
