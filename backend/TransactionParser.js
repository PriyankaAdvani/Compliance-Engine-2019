var hfc = require('fabric-client');
var utils = require('fabric-client/lib/utils.js');
var logger = utils.getLogger('TransactionParser.js');
var callsite = require('callsite');
const winston = require('winston')
const assert = require("assert");



var __func = function() {
    var func = callsite()[1].getFunctionName();
    return (func ? func : 'anonymous') + '[' + callsite()[1].getLineNumber() + ']: ';
};


winston.level = 'debug';


module.exports = class TransactionParser {

  constructor (configuration) {
    assert(configuration != null, 'configuration is null');

    this.assetDbName = configuration['asset-db-name'];
    this.assetCouchdbUrl = configuration['asset-couchdb-url'];
    this.transactionDbName = configuration['transaction-db-name'];
    this.transactionCouchdbUrl = configuration['transaction-couchdb-url'];

    this.assetNano = require('nano')(this.assetCouchdbUrl);
    this.transactionNano = require('nano')(this.transactionCouchdbUrl);
    this.transactionDb = this.transactionNano.use(this.transactionDbName);
  }

  openAssetDb(callback)  {
    assert(callback != null, 'callback is null');

    var self = this;

    logger.info(__func() + 'removing existing asset database: ' + this.assetDbName);

    this.assetNano.db.destroy(this.assetDbName, function() {
      self.initializeAssetDb(self, callback);
    });
  }

  initializeAssetDb(self, callback)  {
    assert(callback != null, 'callback is null');

    logger.info(__func() + 'creating asset database: ' + this.assetDbName);

    this.assetNano.db.create(this.assetDbName, function (err, body) {
      self.assetDb = self.assetNano.use(self.assetDbName);
      logger.info(__func() + 'asset database ' + self.assetDbName + (err ? ' exists' : ' was created'));
      callback();
    });
  }

  loadAssets(callback) {
    var self = this;


    this.loadCallback = callback;

    this.openAssetDb(function () {
      var boundCallback = (self.parseTransactions).bind(self);
      self.transactionDb.list({include_docs: true}, boundCallback);
    });
  }

  parseTransactions(err, body){
    var self = this;

    if (!err) {
      body.rows.forEach(function(doc) {
        //try {
          var txData = doc.doc.TxData;

          if(txData == null){
            console.log('Not a transaction document');
            console.log(doc);
            return;
          }

          var actionsStr = doc.doc.TxData.payload.data.actions;
          var channel_header = doc.doc.TxData.payload.header.channel_header;

          if(actionsStr == null){
            console.log('Not a transaction, ' + actionsStr + ' does not exist');
            return;
          }

          var actionsNum = actionsStr.length;

          for (var p = 0; p < actionsNum; p++){
            var extensionStr = actionsStr[p].payload.action.proposal_response_payload.extension;

            if(extensionStr == null){
              console.log('Not a transaction, ' + extensionStr + ' does not exist');
              return;
            }

            var ns_rwsetNum = extensionStr.results.ns_rwset.length;

            for (var i = 0; i < ns_rwsetNum; i++){
              var writeSets = extensionStr.results.ns_rwset[i].rwset.writes;
              if(writeSets == null){
                console.log('Not a transaction, ' + writeSets + ' does not exist');
                return;
              }

              var writesNum = extensionStr.results.ns_rwset[i].rwset.writes.length;

              for (var j = 0; j < writesNum; j++){

                var asset = {};
                asset.timestamp = channel_header.timestamp;
                asset.channel_id = channel_header.channel_id;
                asset.chaincode_id = extensionStr.events.chaincode_id;
                asset.tx_type = channel_header.type;
                asset.namespace = extensionStr.results.ns_rwset[i].namespace;
                asset.key = writeSets[j].key;
                asset.is_delete = writeSets[j].is_delete;

                try {
                    asset.value = JSON.parse(writeSets[j].value);
                } catch (e) {
                  console.log('Non JSON asset value - skipping the asset');
                  console.log(doc);
                  console.log(writeSets[j].value);
                  continue;
                }

                var docName = asset.key + '-' + asset.timestamp.toString();
                self.assetDb.insert(asset, docName, function(err, body, header) {
                  if (err) {
                    console.log('Asset insert error: ', err.message);
                    return;
                  }
                });
              }
            }
          }
        //}
        //catch(e) {
        //  console.log('Non transaction document - skipping the document');
        //  console.log(doc);
        //}
      });
    }
    //setTimeout(this.loadCallback(), 3000);
    this.loadCallback();
  }
}
