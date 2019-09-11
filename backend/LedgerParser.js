var hfc = require('fabric-client');
var utils = require('fabric-client/lib/utils.js');
var logger = utils.getLogger('LedgerParser.js');
var async = require('async');
var callsite = require('callsite');
var path = require('path');
var util = require('util');
var assert = require('assert');

const transaction_key_prefix = 'tx_';
const statedb_savepoint = 'statedb_savepoint';
const transactiondb_savepoint = 'transactiondb_savepoint';





var __func = function() {
    var func = callsite()[1].getFunctionName();
    return (func ? func : 'anonymous') + '[' + callsite()[1].getLineNumber() + ']: ';
};



module.exports = class LedgerParser {

  constructor (configuration) {
    assert(configuration != null, 'configuration is null');

    this.walletPath = configuration['wallet-path'];
    this.username = configuration['username'];
    this.channelId = configuration['channel-id'];
    this.peerUrl = configuration['peer-url'];
    this.ordererUrl = configuration['orderer-url'];
    this.stateCouchdbUrl = configuration['state-couchdb-url'];
    this.transactionCouchdbUrl = configuration['transaction-couchdb-url'];
    this.transactionDbName = configuration['transaction-db-name'];
    this.resettransactionDb = (configuration['reset-transaction-db'] == 'true');

    this.stateNano = require('nano')(this.stateCouchdbUrl);
    this.stateDb = this.stateNano.use(this.channelId);
    this.transactionNano = require('nano')(this.transactionCouchdbUrl);

    this.stateDbSavePoint = {};
    this.transactionDbSavePoint = {};
    this.currentPoint = {};

    this.client = new hfc();
    this.channel = this.client.newChannel(this.channelId);
  }

  openTransactionDb(callback)  {
    assert(callback != null, 'callback is null');

    var self = this;

    if (this.resettransactionDb) {
      logger.info(__func() + 'removing existing transaction database: ' + this.transactionDbName);

      this.transactionNano.db.destroy(this.transactionDbName, function() {
        self.initializeTransactionDb(self, callback);
      });
    }
    else {
      self.initializeTransactionDb(self, callback);
    }
  }

  initializeTransactionDb(self, callback)  {
    assert(callback != null, 'callback is null');

    logger.info(__func() + 'creating transaction database: ' + this.transactionDbName);

    this.transactionNano.db.create(this.transactionDbName, function (err, body) {

      self.transactionDb = self.transactionNano.use(self.transactionDbName);

      if (!err) {
        self.transactionDb.insert(
          {
            _id: transactiondb_savepoint,
            BlockNum: 1,
            TxNum: -1
          },
          function(err, body) {
            if (err) {
              throw new Error(__func() + err);
            }
          }
        );
      }

      logger.info(__func() + 'transaction database ' + self.transactionDbName + (err ? ' exists' : ' was created'));

      callback();
    });
  }

  loadTransactions(callback) {
    var self = this;

    this.openTransactionDb(function () {
      self.parseLedger(callback);
    });
  }

  parseLedger(callback) {
    hfc.newDefaultKeyValueStore({
  	   path: path.join(__dirname, this.walletPath)
    }).then((wallet) => {
       this.client.setStateStore(wallet);
  	   return this.client.getUserContext(this.username, true);
    }).then((user) => {
  	   if (typeof user === 'undefined' || user === null || user.isEnrolled() === false) {
         throw new Error(__func() + 'getUserContext: ' + user);
  	   }
       logger.debug(__func() + 'getUserContext: ' + user);
       this.channel.addPeer(this.client.newPeer(this.peerUrl));
    	 this.channel.addOrderer(this.client.newOrderer(this.ordererUrl));
    	 this.channel.initialize();
    }).then(() => {
  	   logger.info(__func() + 'channel successfully initialized');
       this.step1_getStateDbSavePoint(callback);
    }).catch((err) => {
  	   logger.error(err);
    });
  }

  step1_getStateDbSavePoint(callback) {
      var self = this;


      this.stateDb.get(statedb_savepoint, function (err, body) {

        if (err) {
    	     throw new Error(__func() + err);
        }

        logger.debug(__func() + statedb_savepoint + util.inspect(body));
        self.stateDbSavePoint = body;

        self.step2_getTranactionDbSavePoint(self, callback);
      });
  }

  step2_getTranactionDbSavePoint(self, callback) {
      self.transactionDb.get(transactiondb_savepoint, function (err, body) {

        if (err) {
    	     throw new Error(__func() + err);
        }

        logger.debug(__func() + transactiondb_savepoint + util.inspect(body));
        self.transactionDbSavePoint = body;
        self.currentPoint.BlockNum = self.transactionDbSavePoint.BlockNum;
        self.currentPoint.TxNum = self.transactionDbSavePoint.TxNum;

        self.step3_parseBlocks(self, callback);
      });
  }

  step3_parseBlocks(self, callback) {

      var blocks = [];


      for (var i = self.transactionDbSavePoint.BlockNum; i <= this.stateDbSavePoint.BlockNum; i++) {
  	     blocks.push(i);
      }

      if (blocks.length > 0) {
  	     async.eachSeries(blocks, function(i, callback) {
  	        self.channel.queryBlock(i).then((block) => {
  		          self.parseBlock(self, block);
                callback(null);
              });
            }, function(err) {
              if (err) throw new Error(__func() + err);

              if (self.currentPoint.BlockNum != self.transactionDbSavePoint.BlockNum || self.currentPoint.TxNum != self.transactionDbSavePoint.TxNum) {
  		            self.transactionDb.insert({
                    _id: self.transactionDbSavePoint._id,
                    _rev: self.transactionDbSavePoint._rev,
                    BlockNum: self.currentPoint.BlockNum,
                    TxNum: self.currentPoint.TxNum
                  }, function(err, body) {
                    if (err) {
                      throw new Error(__func() + err);
                    }
                  });
  		              logger.info(__func() + self.transactionDbName + ' database was updated to ' + util.inspect(self.currentPoint));
                    callback();
  	          }
        	    else {
        		      logger.info(__func() + self.transactionDbName + ' database was not updated ' + util.inspect(self.currentPoint));
                  callback();
  	          }
              });
      }
  }

  /*
   * Function to parse each block and write out transaction data
   * into history database.
   *
   * This function is called for each block we are going to process,
   * starting from the one we left off last time.
   */
  parseBlock(self, block) {

      var blockNum = block.header.number.low;
      var txs = block.data.data;

      /*
       * history.BlockNum is the block index and history.TxNum is the TX index
       * we wrote into history database last time.
       *
       * We start from history.BlockNum again in case more TXs has been added
       * to block at history.BlockNum since we made the queryBlock call. This
       * is possible because:
       *
       * - we make the queryBlock call, and we get some blocks, and the last
       *   block have some TXs.
       * - but after that more TXs can be added to the last block, which we
       *   don't see until the next time we are run.
       */
      self.currentPoint.BlockNum = blockNum;
      self.currentPoint.TxNum = (blockNum == self.transactionDbSavePoint.BlockNum ? self.transactionDbSavePoint.TxNum : -1);

      while (self.currentPoint.TxNum + 1 < txs.length) {
        self.currentPoint.TxNum++;

      	var txData = {
      	    _id: transaction_key_prefix + self.currentPoint.BlockNum + '_' + self.currentPoint.TxNum,
      	    BlockNum: self.currentPoint.BlockNum, TxNum: self.currentPoint.TxNum,
      	    TxData: txs[self.currentPoint.TxNum]
      	};

        logger.info(__func() + '_id: ' + txData._id);
        //logger.info(__func() + 'txData: ' + JSON.stringify(txs[self.currentPoint.TxNum]));
        //console.log(txData.TxData.payload.header.channel_header.timestamp);

      	self.transactionDb.insert(
      	    txData,
            function(err, body) {
              if (err) {
                throw new Error(__func() + err);
              }
            });

        logger.debug(__func() + 'txData: ' + util.inspect(txData));
      }
  }

}
