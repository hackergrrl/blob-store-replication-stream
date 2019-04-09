# blob-store-replication-stream

> Replicate two
> [abstract-blob-store](https://github.com/maxogden/abstract-blob-store) compatible stores together (with caveats).

It would be very useful if any two blob stores could be replicated to each other
over a simple duplex stream.

That's just what this module does, but with one large caveat: the
`abstract-blob-store` interface doesn't provide a method for getting a list of
the names of all blobs in the store. Without this, a full sync between blob
stores isn't possible.

*The caveat*: any blob store you wish to use for replication must have a
`._list(cb)` function monkey patched onto it, which should return an array of
keys (strings) in the callback `cb`. This will generally mean delving into the
innards of each blob store you wish to support and figuring out how to add
this functionality.

## Usage

```js
var createReplicationStream = require('blob-store-replication-stream')

var StoreFs = require('fs-blob-store')
StoreFs.prototype._list = function (cb) {
  require('fs').readdir(this.path, cb)
}

var StoreMem = require('abstract-blob-store')
StoreMem.prototype._list = function (cb) {
  process.nextTick(cb, null, Object.keys(this.data))
}

var fs = StoreFs('./fs')
var mem = StoreMem()

var ws = fs.createWriteStream('foo', function (err, metadata) {
  console.log('fs key', metadata.key)
  var ws = mem.createWriteStream('bar', function (err, metadata) {
    console.log('mem key', metadata.key)
    doReplicate()
  })
  ws.end('yo mem world')
})
ws.end('hello fs world')

function doReplicate () {
  var r1 = createReplicationStream(fs)
  var r2 = createReplicationStream(mem)

  r1.pipe(r2).pipe(r1)

  r1.on('end', onDone)
  r2.on('end', onDone)

  var pending = 2
  function onDone () {
    if (!--pending) {
      fs.createReadStream('bar').pipe(process.stdout)
      mem.createReadStream('foo').pipe(process.stdout)
    }
  }
}
```

outputs

```
fs key foo
mem key bar
yo mem world
hello fs world
```

## API

```js
var createReplicationStream = require('blob-store-replication-stream')
```

### var stream = createReplicationStream(store[, opts])

Creates the duplex stream `stream` from the abstract-blob-store instance
`store`.

Pipe this into another replication stream, and have that other replication
stream also pipe into this, to create a full duplex channel of communication.

This function will throw an `Error` if no `store._list()` function is present.

Valid `opts` include:

- `mode`:
  - `'sync'`: (default) entries will sent and fetched. the two stores will contain the same blobs post-replication
  - `'pull'`: entries will only be fetched, not sent
  - `'push'`: entries will only be sent, not fetched
- `filter`: a function that takes a filename and returns `true` if it should be synced to the other side, or `false` if it shouldn't

In addition, `stream` will emit `'progress'` events with parameters `filesSent`
and `filesTotal` for tracking sync progress.

## Install

With [npm](https://npmjs.org/) installed, run

```
$ npm install blob-store-replication-stream
```

## License

ISC

