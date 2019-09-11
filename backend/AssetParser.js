var hfc = require('fabric-client');
var utils = require('fabric-client/lib/utils.js');
var logger = utils.getLogger('AssetParser.js');
const AssetJoinEnumerator = require("./AssetJoinEnumerator");
var callsite = require('callsite');
var JSONPath = require('jsonpath-plus');
const winston = require('winston')
const assert = require("assert");



var __func = function() {
    var func = callsite()[1].getFunctionName();
    return (func ? func : 'anonymous') + '[' + callsite()[1].getLineNumber() + ']: ';
};


winston.level = 'debug';



module.exports = class AssetParser {

  constructor (configuration) {
    assert(configuration != null, 'configuration is null');

    this.assetDbName = configuration['asset-db-name'];
    this.assetCouchdbUrl = configuration['asset-couchdb-url'];

    this.assetNano = require('nano')(this.assetCouchdbUrl);

    this.rootAssets = null;
  }

  loadRootAssets(filter, callback) {
    assert(filter != null, 'filter is null');
    assert(callback != null, 'callback is null');

    var selector = this.createSelectorFromFilter(filter);


    logger.info("Root filter: ", JSON.stringify(filter));
    logger.info("Root selector: ", JSON.stringify(selector));

    this.getAssetsCallback = callback;

    this.queryDb(selector, callback);
  }

/* full query based - has the reuired functionality but does not return more than 25 records
there is a limit=25
http://docs.couchdb.org/en/latest/api/database/find.html?highlight=find
  queryDb(selector) {

    var boundCallback = (this.processRootAssets).bind(this);

    this.assetNano.request({
        db: this.assetDbName,
        method: 'POST',
        doc: '_find',
        headers: {limit: 10},
        body: selector
      },
      boundCallback);
  }
*/

  queryDb(selector) {

    var boundCallback = (this.processRootAssets).bind(this);

    var adb = this.assetNano.db.use(this.assetDbName);

    // the use of selector does not work - as long as there is json all records come!
    adb.fetch(selector, boundCallback);
  }

  createSelectorFromFilter(filter) {

    var selector = JSON.parse(JSON.stringify(filter));


    delete selector["asset-type"];

    for (var property in selector) {
      if (selector.hasOwnProperty(property)) {
        if (selector[property].length == 0) {
          delete selector[property];
        }
      }
    }

    var querySelector = {};
    querySelector.selector = selector;

    return querySelector;
  }

  processRootAssets(err, data) {
    if(!err) {
      // this applies to "_find"
      //this.rootAssets = data.docs;

      // this applies to "fetch"
      this.rootAssets = data.rows;

      //this.rootAssets = [this.rootAssets[0]];

      logger.info("Number of root assets: ", this.rootAssets.length);
      this.getAssetsCallback();
    }
    else {
      logger.error("Error loading root assets - ", err);
    }
  }

  getAssetJoinEnumerator (joinConfiguration) {
    assert(joinConfiguration != null, 'joinConfiguration is null');
    return new AssetJoinEnumerator(this.rootAssets, this.assetDbName, this.assetNano, joinConfiguration);
  }
}
