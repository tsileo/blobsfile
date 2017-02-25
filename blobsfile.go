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

*/
package blobsfile

// FIXME(tsileo): switch to canonical imports a4.io/blobsfile

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

const (
	// Blob flags
	Blob byte = 1 << iota
	Compressed
	ParityBlob
	EOF
)

const (
	// Supported compression algorithms
	Snappy byte = 1 << iota
)

var (
	openFdsVar      = expvar.NewMap("blobsfile-open-fds")
	bytesUploaded   = expvar.NewMap("blobsfile-bytes-uploaded")
	bytesDownloaded = expvar.NewMap("blobsfile-bytes-downloaded")
	blobsUploaded   = expvar.NewMap("blobsfile-blobs-uploaded")
	blobsDownloaded = expvar.NewMap("blobsfile-blobs-downloaded")
)

var (
	ErrBlobNotFound        = errors.New("blob not found")
	ErrParityBlobCorrupted = errors.New("a parity blob is corrupted")
	ErrBlobsfileCorrupted  = errors.New("blobsfile is corrupted")
)

type CorruptedError struct {
	blobs  []*BlobPos
	offset int64
	err    error
}

func (ce *CorruptedError) Blobs() []*BlobPos {
	return ce.blobs
}

func (ce *CorruptedError) Error() string {
	if len(ce.blobs) > 0 {
		return fmt.Sprintf("%d blobs are corrupt", len(ce.blobs))
	}
	return fmt.Sprintf("corrupted at offset %d: %v", ce.offset, ce.err)
}

func (ce *CorruptedError) FirstBadOffset() int64 {
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
	for {
		if shardSize+(shardSize*i) > ioffset {
			return i
		}
		i++
	}
	return 0
}

type Opts struct {
}

// Holds all the backend data
// FIXME(tsileo): BlobsFileBackend => BlobsFiles
type BlobsFileBackend struct {
	log log2.Logger
	// Directory which holds the blobsfile
	Directory string

	// Maximum size for a blobsfile (256MB by default)
	maxBlobsFileSize int64

	// Backend state
	reindexMode bool

	// Compression is disabled by default
	snappyCompression bool

	// The kv index that maintains blob positions
	index *BlobsIndex

	// Current blobs file opened for write
	n       int
	current *os.File
	// Size of the current blobs file
	size int64
	// All blobs files opened for read
	files map[int]*os.File

	rse reedsolomon.Encoder
	wg  sync.WaitGroup
	sync.Mutex
}

// New intializes a new BlobsFileBackend
// FIXME(tsileo): use a strict opts for helping keeping API compat
func New(dir string, maxBlobsFileSize int64, compression bool, wg sync.WaitGroup) (*BlobsFileBackend, error) {
	// Try to create the directory
	os.MkdirAll(dir, 0700)
	var reindex bool
	// Check if an index file is already present
	if _, err := os.Stat(filepath.Join(dir, "blobs-index")); os.IsNotExist(err) {
		// No index found
		reindex = true
	}
	index, err := NewIndex(dir)
	if err != nil {
		return nil, err
	}
	if maxBlobsFileSize == 0 {
		maxBlobsFileSize = defaultMaxBlobsFileSize
	}

	// Initialize the Reed-Solomon encoder
	enc, err := reedsolomon.New(dataShards, parityShards)
	if err != nil {
		return nil, err
	}

	backend := &BlobsFileBackend{
		Directory:         dir,
		snappyCompression: compression,
		index:             index,
		files:             make(map[int]*os.File),
		maxBlobsFileSize:  maxBlobsFileSize,
		rse:               enc,
		wg:                wg,
		reindexMode:       reindex,
	}
	backend.log = logger.Log.New("backend", backend.String())
	backend.log.Debug("Started")
	if err := backend.load(); err != nil {
		panic(fmt.Errorf("Error loading %T: %v", backend, err))
	}
	if backend.snappyCompression {
		backend.log.Debug("snappy compression enabled")
	}
	return backend, nil
}

func (backend *BlobsFileBackend) IterOpenFiles() (files []*os.File) {
	for _, f := range backend.files {
		files = append(files, f)
	}
	return files
}

func (backend *BlobsFileBackend) CloseOpenFiles() {
	for _, f := range backend.files {
		f.Close()
	}
}

func (backend *BlobsFileBackend) Close() {
	backend.wg.Wait()
	backend.log.Debug("closing index...")
	backend.index.Close()
}

// Remove the index
func (backend *BlobsFileBackend) RemoveIndex() {
	backend.index.Remove()
}

// GetN returns the total numbers of BlobsFile
func (backend *BlobsFileBackend) GetN() (int, error) {
	return backend.index.GetN()
}

func (backend *BlobsFileBackend) saveN() error {
	return backend.index.SetN(backend.n)
}

func (backend *BlobsFileBackend) restoreN() error {
	n, err := backend.index.GetN()
	if err != nil {
		return err
	}
	backend.n = n
	return nil
}

// Implements the Stringer interface
func (backend *BlobsFileBackend) String() string {
	return fmt.Sprintf("blobsfile-%v", backend.Directory)
}

func (backend *BlobsFileBackend) scanBlobsFile(n int, iterFunc func(*BlobPos, byte, string, []byte) error) error {
	corrupted := []*BlobPos{}

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
			return &CorruptedError{nil, offset, fmt.Errorf("failed to read hash: %v", err)}
		}
		if _, err := blobsfile.Read(flags); err != nil {
			return &CorruptedError{nil, offset, fmt.Errorf("failed to read flag: %v", err)}
		}
		// If we reached the EOF blob, break
		if flags[0] == EOF {
			break
		}
		if _, err := blobsfile.Read(blobSizeEncoded); err != nil {
			return &CorruptedError{nil, offset, fmt.Errorf("failed to read blob size: %v", err)}
		}
		blobSize := int64(binary.LittleEndian.Uint32(blobSizeEncoded))
		rawBlob := make([]byte, int(blobSize))
		read, err := blobsfile.Read(rawBlob)
		if err != nil || read != int(blobSize) {
			return &CorruptedError{nil, offset, fmt.Errorf("error while reading raw blob: %v", err)}
		}
		// XXX(tsileo): optional flag to `BlobPos`?
		blobPos := &BlobPos{n: n, offset: int(offset), size: int(blobSize)}
		offset += blobOverhead + blobSize
		var blob []byte
		if backend.snappyCompression {
			blobDecoded, err := snappy.Decode(nil, rawBlob)
			if err != nil {
				return &CorruptedError{nil, offset, fmt.Errorf("failed to decode blob: %v %v %v", err, blobSize, flags)}
			}
			blob = blobDecoded
		} else {
			blob = rawBlob
		}
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
		return &CorruptedError{corrupted, -1, nil}
	}

	return nil
}

func copyShards(i [][]byte) (o [][]byte) {
	for _, a := range i {
		// n := make([]byte, len(a))
		// copy(n[:], a)
		o = append(o, a)
	}
	return o
}

func (backend *BlobsFileBackend) checkBlobsFile(n int) error {
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
		if cerr, ok := err.(*CorruptedError); ok {
			badOffset := cerr.FirstBadOffset()
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

func (backend *BlobsFileBackend) dataShards(n int) ([][]byte, error) {
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

func (backend *BlobsFileBackend) parityShards(n int) ([][]byte, error) {
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
			return parityBlobs, ErrParityBlobCorrupted
		}
		parityBlobs = append(parityBlobs, blob)
	}
	return parityBlobs, nil
}

func (backend *BlobsFileBackend) checkParityBlobs(n int) error {
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

func (backend *BlobsFileBackend) scan(iterFunc func(*BlobPos, byte, string, []byte) error) error {
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
func (backend *BlobsFileBackend) reindex() error {
	backend.wg.Add(1)
	defer backend.wg.Done()
	backend.log.Info("re-indexing BlobsFiles...")
	n := 0
	blobsIndexed := 0

	iterFunc := func(blobPos *BlobPos, flag byte, hash string, _ []byte) error {
		// Skip parity blobs
		if flag == ParityBlob {
			return nil
		}
		if err := backend.index.SetPos(hash, blobPos); err != nil {
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
func (backend *BlobsFileBackend) load() error {
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
func (backend *BlobsFileBackend) wopen(n int) error {
	backend.log.Info("opening blobsfile for writing", "name", backend.filename(n))
	// Close the already opened file if any
	if backend.current != nil {
		if err := backend.current.Close(); err != nil {
			openFdsVar.Add(backend.Directory, -1)
			return err
		}
	}
	created := false
	if _, err := os.Stat(backend.filename(n)); os.IsNotExist(err) {
		created = true
	}
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
		// // Write the reserved bytes
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
	openFdsVar.Add(backend.Directory, 1)
	return nil
}

// Open a file for read
func (backend *BlobsFileBackend) ropen(n int) error {
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
	fmagic := make([]byte, len(headerMagic))
	_, err = f.Read(fmagic)
	if err != nil || headerMagic != string(fmagic) {
		return fmt.Errorf("magic not found in BlobsFile: %v or header not matching", err)
	}
	if _, err := f.Seek(int64(headerSize), os.SEEK_SET); err != nil {
		return err
	}
	backend.files[n] = f
	openFdsVar.Add(backend.Directory, 1)
	return nil
}

func (backend *BlobsFileBackend) filename(n int) string {
	return filepath.Join(backend.Directory, fmt.Sprintf("blobs-%05d", n))
}

// writeParityBlobs compute and write the 4 parity shards using Reed-Solomon 10,4 and write them at
// end the blobsfile, and write the "data size" (blobsfile size before writing the parity shards).
func (backend *BlobsFileBackend) writeParityBlobs() error {
	start := time.Now()
	// We write the data size at the end of the file
	if _, err := backend.current.Seek(0, os.SEEK_END); err != nil {
		return err
	}

	// Read the whole blobsfile
	fdata := make([]byte, backend.size)
	if _, err := backend.current.ReadAt(fdata, 0); err != nil {
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
	parityBlobs := shards[dataShards:len(shards)]
	for _, parityBlob := range parityBlobs {
		_, parityBlobEncoded := backend.encodeBlob(parityBlob, ParityBlob)

		n, err := backend.current.Write(parityBlobEncoded)
		backend.size += int64(len(parityBlobEncoded))
		if err != nil || n != len(parityBlobEncoded) {
			return fmt.Errorf("Error writing parity blob (%v,%v)", err, n)
		}
	}

	// Fsync
	if err = backend.current.Sync(); err != nil {
		panic(err)
	}
	duration := time.Since(start)
	fmt.Printf("parity encoding done in %s\n", duration)
	return nil
}

// Put save a new blob
func (backend *BlobsFileBackend) Put(hash string, data []byte) (err error) {
	// Acquire the lock
	backend.Lock()
	defer backend.Unlock()

	backend.wg.Add(1)
	defer backend.wg.Done()

	// Encode the blob
	blobSize, blobEncoded := backend.encodeBlob(data, Blob)

	// Ensure the blosfile size won't exceed the maxBlobsFileSize
	if backend.size+int64(blobSize+blobOverhead) > backend.maxBlobsFileSize {
		paddingLen := backend.maxBlobsFileSize - (backend.size + blobOverhead)
		headerEOF := makeHeaderEOF(paddingLen)
		n, err := backend.current.Write(headerEOF)
		if err != nil {
			return fmt.Errorf("failed to write EOF header: %v", err)
		}
		backend.size += int64(n)

		padding := make([]byte, paddingLen)
		n, err = backend.current.Write(padding)
		if err != nil {
			return fmt.Errorf("failed to write padding 0: %v", err)
		}
		backend.size += int64(n)

		// Write some parity blobs at the end of the blobsfile using Reed-Solomon erasure coding
		// XXX(tsileo): this process can be done async, but then how do we notify an error if it happens?
		if err := backend.writeParityBlobs(); err != nil {
			return err
		}

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

	// Save the blob in the index
	blobPos := &BlobPos{n: backend.n, offset: int(backend.size), size: blobSize}
	if err := backend.index.SetPos(hash, blobPos); err != nil {
		return err
	}

	// Actually save the blob
	n, err := backend.current.Write(blobEncoded)
	backend.size += int64(len(blobEncoded))
	if err != nil || n != len(blobEncoded) {
		return fmt.Errorf("Error writing blob (%v,%v)", err, n)
	}

	// Fsync
	if err = backend.current.Sync(); err != nil {
		panic(err)
	}

	// Update the expvars
	bytesUploaded.Add(backend.Directory, int64(len(blobEncoded)))
	blobsUploaded.Add(backend.Directory, 1)
	return
}

// Alias for exists
func (backend *BlobsFileBackend) Stat(hash string) (bool, error) {
	return backend.Exists(hash)
}

// Exists check if a blob is present
func (backend *BlobsFileBackend) Exists(hash string) (bool, error) {
	blobPos, err := backend.index.GetPos(hash)
	if err != nil {
		return false, err
	}
	if blobPos != nil {
		return true, nil
	}
	return false, nil
}

func (backend *BlobsFileBackend) decodeBlob(data []byte) (size int, blob []byte, flag byte) {
	flag = data[hashSize]
	compressionAlgFlag := data[hashSize+1]
	size = int(binary.LittleEndian.Uint32(data[hashSize+2 : blobOverhead]))
	blob = make([]byte, size)
	copy(blob, data[blobOverhead:])
	if backend.snappyCompression && flag == Compressed && compressionAlgFlag == Snappy {
		blobDecoded, err := snappy.Decode(nil, blob)
		if err != nil {
			panic(fmt.Errorf("Failed to decode blob with Snappy: %v", err))
		}
		flag = Blob
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
	// FIXME(tsileo): take the len of the padding as arg and write it as uint32 after the flag
	// Write a hash with only zeroes
	h = make([]byte, blobOverhead)
	h[32] = EOF
	binary.LittleEndian.PutUint32(h[33:], uint32(padSize))
	return
}

func (backend *BlobsFileBackend) encodeBlob(blob []byte, flag byte) (size int, data []byte) { // XXX(tsileo): flag as argument
	h := blake2b.New256()
	h.Write(blob)

	var compressionAlgFlag byte
	// Only compress regular blobs
	if backend.snappyCompression && flag == Blob {
		dataEncoded := snappy.Encode(nil, blob)
		flag = Compressed
		blob = dataEncoded
		compressionAlgFlag = Snappy
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
func (backend *BlobsFileBackend) BlobPos(hash string) (*BlobPos, error) {
	return backend.index.GetPos(hash)
}

// Get returns the blob fur the given hash
func (backend *BlobsFileBackend) Get(hash string) ([]byte, error) {
	// Fetch the index entry
	blobPos, err := backend.index.GetPos(hash)
	if err != nil {
		return nil, fmt.Errorf("Error fetching GetPos: %v", err)
	}

	// No index entry found, returns an error
	if blobPos == nil {
		if err == nil {
			return nil, ErrBlobNotFound
		} else {
			return nil, err
		}
	}

	// Read the encoded blob from the BlobsFile
	data := make([]byte, blobPos.size+blobOverhead)
	n, err := backend.files[blobPos.n].ReadAt(data, int64(blobPos.offset))
	if err != nil {
		return nil, fmt.Errorf("Error reading blob: %v / blobsfile: %+v", err, backend.files[blobPos.n])
	}

	// Ensure the data lenght is expcted
	if n != blobPos.size+blobOverhead {
		return nil, fmt.Errorf("Error reading blob %v, read %v, expected %v+%v", hash, n, blobPos.size, blobOverhead)
	}

	// Decode the blob
	blobSize, blob, _ := backend.decodeBlob(data)
	if blobSize != blobPos.size {
		return nil, fmt.Errorf("Bad blob %v encoded size, got %v, expected %v", hash, n, blobSize)
	}

	// Update the expvars
	bytesDownloaded.Add(backend.Directory, int64(blobSize))
	blobsUploaded.Add(backend.Directory, 1)

	return blob, nil
}

// Enumerate output all the blobs into the given chan (ordered lexicographically)
// TODO(tsileo) take a callback func(hash string, size int) error
func (backend *BlobsFileBackend) Enumerate(blobs chan<- *blob.SizedBlobRef, start, end string, limit int) error {
	defer close(blobs)
	backend.Lock()
	defer backend.Unlock()
	// TODO(tsileo) send the size along the hashes ?
	// fmt.Printf("start=%v/%+v\n", start, formatKey(BlobPosKey, []byte(start)))
	s, err := hex.DecodeString(start)
	if err != nil {
		return err
	}
	enum, _, err := backend.index.db.Seek(formatKey(BlobPosKey, s))
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
		blobPos, err := backend.BlobPos(hash)
		if err != nil {
			return nil
		}
		// Remove the BlobPosKey prefix byte
		sbr := &blob.SizedBlobRef{
			Hash: hex.EncodeToString(k[1:]),
			Size: blobPos.size,
		}
		blobs <- sbr
		i++
	}
	return nil
}
