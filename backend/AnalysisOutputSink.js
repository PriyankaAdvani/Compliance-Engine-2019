var utils = require('fabric-client/lib/utils.js');
var logger = utils.getLogger('AnalysisOutputSink.js');
var callsite = require('callsite');
const assert = require("assert");



var __func = function() {
    var func = callsite()[1].getFunctionName();
    return (func ? func : 'anonymous') + '[' + callsite()[1].getLineNumber() + ']: ';
};


// Records output of analysis into db and/or console
module.exports = class AnalysisOutputSink {

  constructor(configuration) {
    assert(configuration != null, 'configuration is null');


    this.couchdbUrl = configuration['couchdb-url'];
    this.resultsDbName = configuration['results-db'];
    this.resetResultsDb = (configuration['reset-results-db'] == 'true');
    this.outputWriteIntoConsole = (configuration['write-into-console'] == 'true');

    this.nano = require('nano')(this.couchdbUrl);
    this.counter = 0;

    this.openResultsDb(function() {});
  }

  openResultsDb(callback)  {
    //assert(callback != null, 'callback is null');

    var self = this;

    if (this.resetResultsDb) {
      logger.info(__func() + 'removing existing results database: ' + this.resultsDbName);

      this.nano.db.destroy(this.resultsDbName, function() {
        self.initializeResultsDb(self, callback);
      });
    }
    else {
      self.initializeResultsDb(self, callback);
    }
  }

  initializeResultsDb(self, callback)  {
    //assert(callback != null, 'callback is null');

    logger.info(__func() + 'creating results database: ' + this.resultsDbName);

    this.nano.db.create(this.resultsDbName, function (err, body) {

      self.resultsDb = self.nano.use(self.resultsDbName);

      logger.info(__func() + 'results database ' + self.resultsDbName + (err ? ' exists' : ' was created'));

      callback();
    });
  }


  recordAnalysisOutput (asset, result) {
    assert(asset != null, 'asset is null');
    assert(result != null, 'result is null');

    this.counter++;
    this.recordAnalysisOutputIntoConsole(asset, result);
    this.recordAnalysisOutputIntoDB(asset, result);
  }

  recordAnalysisOutputIntoConsole (asset, result) {
    if (this.outputWriteIntoConsole) {
      console.log(
        "OUTPUT (" +
        this.counter +
        ") key: " +
        asset.key +
        " timestamp: " +
        asset.timestamp +
        ", result: " +
        JSON.stringify(result));
    }
  }

  recordAnalysisOutputIntoDB (asset, result) {
    var key = asset.key + '-' + asset.timestamp;
    var value = asset;


    asset.result = result;

    this.resultsDb.insert(value, key, function(err, body, header) {
      if (err) {
        logger.info(__func() + 'error', err);
        return;
      }
    });
  }
}
