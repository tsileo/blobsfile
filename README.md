# BlobsFile

[![Build Status](https://travis-ci.org/tsileo/blobsfile.png?branch=master)](https://travis-ci.org/tsileo/blobsfile)
&nbsp; &nbsp;[![Godoc Reference](https://godoc.org/github.com/tsileo/blobsfile?status.png)](https://godoc.org/github.com/tsileo/blobsfile)

*BlobsFile* an append-only (i.e. no update and no delete) content-addressed blob store (using [BLAKE2](https://blake2.net/) as hash function).
It draws inspiration from Facebook's [Haystack](http://202.118.11.61/papers/case%20studies/facebook.pdf), blobs are stored in a flat file and indexed in a small [kv](https://github.com/cznic/kv) database.

*BlobsFile* is [BlobStash](https://github.com/tsileo/blobstash)'s storage engine.

## Features

 - Append-only
 - Extra parity data is added to each BlobFile (using Reed-Solomon error correction), allowing the database to repair itself in case of corruption.

