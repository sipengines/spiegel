'use strict'

const Globals = require('./globals')
const log = require('./log')
const sporks = require('sporks')
const utils = require('./utils')
const urlSafe = require('querystring').escape
const Synchronizer = require('squadron').Synchronizer

class ReplicatorListeners {
  constructor(spiegel, opts) {
    this._spiegel = spiegel
    this._slouch = spiegel._slouch
    this._globals = new Globals(spiegel)

    // only check configuration when opts is not empty
    // opts will be empty during spiegel installation and uninstallation
    if (Object.keys(opts).length) {
      this._config = utils.getOpt(opts, 'config', null)
      if (!this._config) throw new Error('No configuration file loaded')
      if (!this._config.replicator) throw new Error('Missing config.replicator')
      if (!this._config.replicator.source) throw new Error('Missing config.replicator.source')
      if (!this._config.replicator.target) throw new Error('Missing config.replicator.target')
      if (this._config.replicator.source.indexOf("{dbName}") < 0) {
        throw new Error("Missing {dbName} variable placeholder in config.replicator.source")
      }
      if (this._config.replicator.target.indexOf("{dbName}") < 0) {
        throw new Error("Missing {dbName} variable placeholder in config.replicator.target")
      }
    }

    // set replication document template
    this._replicatorDocTpl = this._config ? this._config.replicator : {}
    this._replicatorDocTpl.type = 'replicator'

    // set database exclusion regex
    if (this._config && this._config['exclude-databases']) {
      this._excludeDatabasesRegex = new RegExp(this._config['exclude-databases'])
    }

    // Used to synchronize calls so that batch processing is atomic
    this._synchronizer = new Synchronizer()

    // The maximum number of updates that will be processed in this batch
    this._batchSize = utils.getOpt(opts, 'batchSize', 100)

    // The time to wait after an update before the batch is considered done regardless of whether
    // there are any more updates
    this._batchTimeout = utils.getOpt(opts, 'batchTimeout', 1000)

    this._saveSeqAfterSeconds = utils.getOpt(opts, 'saveSeqAfterSeconds', 60)

    this._seqLastSaved = null

    this._lastSeq = null

    this._stopped = false

    this._dirtyReplicators = false

    this._resetForNextBatch()
  }

  _createAllReplicatorsView() {
    return this._slouch.doc.createOrUpdate(this._spiegel._dbName, {
      _id: '_design/all_replicators',
      views: {
        all_replicators: {
          map: [
            'function(doc) {',
            'if (doc.type && doc.type === "replicator")',
            '{ emit(doc._id, null); } }'
          ].join(' ')
        }
      }
    })
  }

  _destroyAllReplicatorsView() {
    return this._slouch.doc.getAndDestroy(
      this._spiegel._dbName,
      '_design/all_replicators'
    )
  }

  install() {
    return this._createAllReplicatorsView()
  }

  uninstall() {
    return this._destroyAllReplicatorsView()
  }

  _onError(err) {
    log.error(err)
  }

  _startBatchTimeout() {
    this._batchTimer = setTimeout(() => {
      // The batch timer expired so process the batch
      this._processBatch()
    }, this._batchTimeout)
  }

  _toDBName(change) {
    // Return URL safe DB name to avoid issues with slashes in db names
    return urlSafe(/:(.*)$/.exec(change.id)[1])
  }

  _isCreated(change) {
    // Return true if this is a created change event
    return /^created:.*$/.test(change.id)
  }

  _isDeleted(change) {
    // Return true if this is a deleted change event
    return /^deleted:.*$/.test(change.id)
  }

  _addToChangedDBs(change) {
    // We index by dbName to remove duplicates in the batch
    this._changedDBs[this._toDBName(change)] = {
      created: this._isCreated(change),
      deleted: this._isDeleted(change)
    }
  }

  async _resetForNextBatch() {
    // Stop the batch timer
    clearTimeout(this._batchTimer)

    // Reset the counter
    this._changeCount = 0

    // Clear any previous batch of updates
    this._changedDBs = []
  }

  async _processChangedDBs() {
    let dbNames = sporks.keys(this._changedDBs)

    await this._createReplicatorListeners(dbNames)
  }

  async _processBatchUnsynchronized() {
    await this._processChangedDBs()

    this._resetForNextBatch()

    await this._saveLastSeqIfNeeded()
  }

  async _processBatch() {
    await this._synchronizer.run(async() => {
      await this._processBatchUnsynchronized()
    })
  }

  async _onChangeUnsynchronized(change) {
    log.debug('Processing change ' + JSON.stringify(change))

    this._addToChangedDBs(change)

    this._lastSeq = change.seq

    if (this._changeCount === 0) {
      // The 1st update can take any amount of time, but after it is read, we want to start a
      // timer. If the timer expires then we want to consider the batch collected.
      this._startBatchTimeout()
    }

    if (this._changeCount === this._batchSize - 1) {
      // Wait until the batch has been processed so that our listening on the _global_changes is
      // paused until we are ready for the next set of changes.
      await this._processBatchUnsynchronized()
    } else {
      this._changeCount++
    }
  }

  async _onChange(change) {
    // We need to synchronize _onChange with _processBatch as there is some shared memory, e.g.
    // _changedDBs, _changeCount, ...
    await this._synchronizer.run(async() => {
      await this._onChangeUnsynchronized(change)
    })
  }

  _dbChangesIteratorEach() {
    return this._dbChangesIterator.each(async change => {
      // exclude databases from change feed
      if (this._excludeDatabasesRegex) {
        if (this._excludeDatabasesRegex.test(change.id)) return
      }

      // listen for created and deleted changes
      if (/^(created|deleted):(.*)$/.test(change.id)) {
        await this._onChange(change)
      }
    })
  }

  _buildReplicatorDoc(dbName) {
    const doc = {}
    Object.keys(this._replicatorDocTpl).forEach(key => {
      doc[key] = this._replicatorDocTpl[key]
    })
    doc._id = dbName
    doc.source = doc.source.replace('{dbName}', dbName)
    doc.target = doc.target.replace('{dbName}', dbName)
    if (this._dirtyReplicators) {
      doc.dirty = true
      doc.dirty_at = null
    }
    return doc
  }

  async _createReplicatorListeners(dbNames) {
    const filtered = await this._filterChangeListeners(dbNames)
    if (filtered.newDbs.length) {
      await this._bulkCreateReplicatorListeners(filtered.newDbs)
    }
    if (filtered.deletedDbs.length) {
      await this._bulkDeleteReplicatorListeners(filtered.deletedDbs)
    }
  }

  async _filterChangeListeners(dbNames) {
    // fetch all current replicators using the all_replicators view
    let response = await this._slouch.db.viewArray(
      this._spiegel._dbName,
      '_design/all_replicators',
      'all_replicators'
    )

    const newDbs = []
    const deletedDbs = []
    const dbIds = response.rows.map(row => row.id)

    // separate changes into newly created and deleted databases
    dbNames.forEach((dbName) => {
      if (this._changedDBs[dbName].created && dbIds.indexOf(dbName) < 0) {
        newDbs.push(dbName)
      }
      if (this._changedDBs[dbName].deleted && dbIds.indexOf(dbName) >= 0) {
        deletedDbs.push(dbName)
      }
    })
    return { newDbs, deletedDbs }
  }

  async _bulkCreateReplicatorListeners(dbNames) {
    const docs = []
    dbNames.forEach((dbName) => docs.push(this._buildReplicatorDoc(dbName)))
    return this._slouch.doc.bulkCreateOrUpdate(this._spiegel._dbName, docs)
  }

  async _bulkDeleteReplicatorListeners(dbNames) {
    for (const dbName of dbNames) {
      await this._slouch.doc.getAndDestroy(this._spiegel._dbName, dbName)
    }
  }

  // Separate out for easier unit testing
  _changes(opts) {
    return this._slouch.db.changes('_global_changes', opts)
  }

  async _setGlobal(name, value) {
    await this._globals.set(name, value)
  }

  // We save the lastSeq every so often so that we can avoid having to re-process all the updates in
  // the event that an UpdateListener is restarted or a new one starts up
  async _saveLastSeqIfNeeded() {
    // Is it time to save the lastSeq again?
    if (new Date().getTime() - this._seqLastSaved.getTime() >= this._saveSeqAfterSeconds * 1000) {
      await this._saveLastSeq()
    }
  }

  _listenToIteratorErrors(iterator) {
    iterator.on('error', err => {
      this._onError(err)
    })
  }

  async _listenToChanges() {
    this._dbChangesIterator = this._changes({
      feed: 'continuous',
      heartbeat: true,
      since: this._lastSeq ? this._lastSeq : undefined
    })

    this._listenToIteratorErrors(this._dbChangesIterator)

    await this._dbChangesIteratorEach()
  }

  _logFatal(err) {
    log.fatal(err)
  }

  async _listen() {
    try {
      await this._listenToChanges()
    } catch (err) {
      // Log fatal error here as this is in our listening loop, which is detached from our starting
      // chain of promises
      this._logFatal(err)
    }
  }

  _getLastSeq(fromGlobalChangesDb = false) {
    if (fromGlobalChangesDb) {
      return this._getGlobalChangesDbLastSeq()
    }
    return this._globals.get('replicators_lastSeq')
  }

  async _saveLastSeq() {
    log.info('Saving lastSeq=', this._lastSeq)
    await this._setGlobal('replicators_lastSeq', this._lastSeq)
    this._seqLastSaved = new Date()
  }

  async _saveUpdateListenerLastSeq() {
    await this._setGlobal('lastSeq', this._lastSeq)
  }

  _getInitFlag() {
    return this._globals.get('replicators_init')
  }

  _setInitFlag() {
    return this._globals.set('replicators_init', (new Date()).toISOString())
  }

  _getAllDbNames() {
    return this._slouch._req({
      uri: this._slouch._url + '/_all_dbs',
      method: 'GET',
      parseBody: true
    });
  }

  async _getGlobalChangesDbLastSeq() {
    const doc = await this._slouch._req({
      uri: this._slouch._url + '/_global_changes',
      method: 'GET',
      parseBody: true
    });
    return doc.update_seq
  }
  
  async _initializeIfNot() {
    log.info('Checking if the process is already initialized...')
    const init = await this._getInitFlag()

    // run the init process if this is a first run
    if (!init) {
      log.info('Process not initialized')
      log.info('Starting the initilization process')
      const _this = this
      let dbNames = await this._getAllDbNames().filter((dbName) => {
        if (_this._excludeDatabasesRegex) {
          if (_this._excludeDatabasesRegex.test("created:" + dbName)) return false
        }
        return true
      })
      log.info('Found ' + dbNames.length + " existing databases")
      this._dirtyReplicators = true
      for (const dbName of dbNames) {
        await this._onChange({id: "created:" + dbName, seq: null})
      }
      this._dirtyReplicators = false
      await this._setInitFlag()

      // Fetch and save the lastseq from _global_changes. This will prevent the
      // replicator-listener from reprocessing changes that were present before init.
      // The init process will dirty all replicators on creation; consequently processing
      // all prior change events.
      this._lastSeq = await this._getLastSeq(true)
      await this._saveLastSeq()

      // Save the lastseq for the update-listener. This will prevent the update-listener
      // from dirtying the replicators again due to exisiting change events before init.
      // If the update-listener is not stopped from doing this, the replicator process will
      // get triggered a second time; which is a waste of resources.
      await this._saveUpdateListenerLastSeq()
    }

    log.info('Process is initialized')
  }

  async start() {
    this._lastSeq = await this._getLastSeq()

    // We haven't actually saved the lastSeq but we need to initialize the value here so that we
    // will save the updated value in the future
    this._seqLastSaved = new Date()

    await this._initializeIfNot()

    this._listen()
  }

  async stop() {
    this._stopped = true
    if (this._dbChangesIterator) {
      this._dbChangesIterator.abort()
    }

    // Save the lastSeq so that we don't lose our place
    await this._saveLastSeq()
  }
}

module.exports = ReplicatorListeners
