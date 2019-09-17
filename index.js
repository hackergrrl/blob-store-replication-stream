var lpstream = require('length-prefixed-stream')
var duplexify = require('duplexify')
var collect = require('collect-stream')
var missing = require('./missing')
var debug = require('debug')('blob-store-replication-stream')

function noop () {}

module.exports = function (store, opts) {
  opts = opts || {}
  var ID = Math.round(Math.random() * 50)

  var filesToXfer = 0
  var filesXferred = 0

  var encoder = lpstream.encode()
  var decoder = lpstream.decode({allowEmpty:true})

  var dup = duplexify(decoder, encoder)
  var localHaves = null
  var remoteHaves = null

  var state = 'wait-remote-haves'
  var numFilesToRecv = null
  var pendingFilename = null
  var filesSent = false
  var remoteDone = false
  var localDone = false

  // sync
  // 1. send your haves
  // 2. await their haves
  // 3. figure out what you want from them
  // 4. send your wants
  // 5. await their wants
  // 6. get their wants and start sending them (# of entries, then entries)

  // pull
  // 1. await their haves
  // 2. send your haves (just the ones you have in common)
  // 3. figure out what you want from them (everything you don't have)
  // 4. send your wants
  // 5. await their wants
  // 6. get their wants and start sending them (# of entries, then entries)

  // push
  // 1. await their haves
  // 2. send your haves
  // 3. figure out what you want from them (always nothing)
  // 4. send your wants
  // 5. await their wants
  // 6. get their wants and start sending them (# of entries, then entries)

  function onData (data) {
    if (data.toString() === '"done"') {
      debug(''+ID, 'remote done')
      remoteDone = true
    }
    switch (state) {
      case 'wait-remote-haves':
        state = 'wait-remote-wants'
        decoder.pause()
        handleRemoteHaves(data)
        sendWants()
        decoder.resume()
        break
      case 'wait-remote-wants':
        state = 'wait-remote-files-length'
        var remoteWants = handleRemoteWants(data)
        filesToXfer += remoteWants.length
        sendRequested(remoteWants, function () {
          debug('' + ID, 'ALL SENT')
          filesSent = true
          if (numFilesToRecv === 0) terminate()
        })
        break
      case 'wait-remote-files-length':
        state = 'wait-remote-file-name'
        numFilesToRecv = Number(JSON.parse(data.toString()))
        debug(''+ID, 'got # of remote files incoming', numFilesToRecv)
        if (numFilesToRecv === 0 && filesSent) terminate()
        break
      case 'wait-remote-file-name':
        state = 'wait-remote-file-data'
        pendingFilename = data.toString()
        debug(''+ID, 'got a filename', pendingFilename)
        break
      case 'wait-remote-file-data':
        var fn = pendingFilename
        debug(''+ID, 'recving a remote file', fn)
        decoder.pause()
        var ws = store.createWriteStream(fn, function (err) {
          if (err) return dup.emit('error', err)
          filesXferred++
          emitProgress()
          decoder.resume()
          debug(''+ID, 'recv\'d a remote file', fn)
          if (--numFilesToRecv === 0) {
            debug('' + ID, 'ALL RECEIVED')
            if (filesSent) terminate()
          }
        })
        ws.end(data)

        if (numFilesToRecv <= 1) state = 'wait-remote-done'
        else state = 'wait-remote-file-name'
        break
      case 'wait-remote-done':
        if (numFilesToRecv > 0 || !filesSent) break
        if (data.toString() === '"done"') {
          terminate()
        } else {
          console.error('unexpected msg', data)
        }
        break
    }
  }

  function terminate () {
    if (remoteDone) {
      debug('' + ID, 'replication done')
      debug('' + ID, 'TERMINATING')
      // TODO: terminate replication
      if (!localDone) encoder.write(JSON.stringify('done'))
      encoder.end()
    } else {
      encoder.write(JSON.stringify('done'))
      debug('' + ID, 'waiting for remote done')
      state = 'wait-remote-done'
    }
    localDone = true
  }

  ;(store.list || store._list).call(store, function (err, names) {
    if (err) return dup.emit('error', err)
    else {
      if (opts.filter) names = names.filter(opts.filter)
      debug('' + ID, 'lhave', names)
      localHaves = names

      // Defer on sending haves if in pull-only mode
      if (opts.mode !== 'pull') sendHaves()

      // begin reading
      decoder.on('data', onData)
    }
  })

  function sendHaves () {
    // send local haves
    debug('' + ID, 'sent local haves')
    encoder.write(JSON.stringify(localHaves))
  }

  function handleRemoteHaves (data) {
    debug('' + ID, 'got remote haves', data.toString())
    remoteHaves = JSON.parse(data.toString())

    // In push mode: deduplicate the entries both sides have in common; just
    // ask for the ones missing from the local store
    if (opts.mode === 'pull') {
      localHaves = intersect(localHaves, remoteHaves)
      sendHaves()
    }
  }

  function sendWants () {
    // send local wants
    var wants = missing(localHaves, remoteHaves)

    // In 'push' mode, we never want anything
    if (opts.mode === 'push') wants = []

    filesToXfer += wants.length
    debug('' + ID, 'wrote local wants', JSON.stringify(wants))
    encoder.write(JSON.stringify(wants))
  }

  function handleRemoteWants (data) {
    // recv remote wants
    debug('' + ID, 'got remote wants', data.toString())
    return JSON.parse(data.toString())
  }

  function sendRequested (toSend, done) {
    debug('' + ID, 'writing', toSend.length)
    encoder.write(JSON.stringify(toSend.length))
    debug('' + ID, 'wrote # of entries count')

    if (toSend.length === 0) return process.nextTick(done)

    function next (n) {
      var name = toSend[n]
      debug('' + ID, 'collecting', name)
      // TODO: stream content from disk straight to the encoder stream
      collect(store.createReadStream(name), function (err, data) {
        if (err) return dup.emit('error', err)
        var res = encoder.write(name)
        if (data.length) res = encoder.write(data)
        else encoder.write(Buffer.alloc(0))

        filesXferred++
        emitProgress()

        debug('' + ID, 'collected + wrote', name, err, data && data.length)


        if (n === toSend.length - 1) return done()

        if (!res) {
          encoder.once('drain', function () {
            next(n+1)
          })
        } else {
          next(n+1)
        }
      })
    }
    next(0)
  }

  return dup

  function emitProgress () {
    dup.emit('progress', filesXferred, filesToXfer)
  }
}

// [x], [x] -> [x]
// What is common to a and b?
function intersect (a, b) {
  var m = []
  var amap = {}
  a.forEach(function (v) { amap[v] = true })

  b.forEach(function (v) {
    if (amap[v]) m.push(v)
  })

  return m
}
