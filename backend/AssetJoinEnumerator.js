var hfc = require('fabric-client');
var utils = require('fabric-client/lib/utils.js');
var logger = utils.getLogger('AssetJoinEnumerator.js');
var AssetJoin = require('./AssetJoin.js');
var callsite = require('callsite');
var JSONPath = require('jsonpath-plus');
var async = require('async');
const winston = require('winston')
const assert = require("assert");



var __func = function() {
    var func = callsite()[1].getFunctionName();
    return (func ? func : 'anonymous') + '[' + callsite()[1].getLineNumber() + ']: ';
};


winston.level = 'debug';



module.exports = class AssetJoinEnumerator {

  constructor (rootAssets, assetDbName, assetNano, joinsConfiguration) {
    this.rootAssets = rootAssets;
    this.assetDbName = assetDbName;
    this.assetNano = assetNano;
    this.loadJoins(joinsConfiguration);

    this.count = this.rootAssets.length;
    this.index = 0;
  }

  loadJoins(joinsConfiguration) {
    this.joins = [];

    logger.info("Loading joins ...");

    for (var i = 0; i < joinsConfiguration.length; i++) {
        this.joins[i] = new AssetJoin(this.assetNano, joinsConfiguration[i]);
    }
  }

  hasNext () {
    return this.index < this.count;
  }

  getNext (callback) {
    var currentAsset = this.rootAssets[this.index];


    this.index++;
    logger.info("Loading asset: ", currentAsset._id);
    delete currentAsset._id;
    delete currentAsset._rev;

    this.loadJoinsAssets(currentAsset, callback);
  }

  loadJoinsAssets(currentAsset, callback) {
    async.eachSeries(this.joins, function(join, innerCallback)
    {
      join.loadJoinAssets (currentAsset, innerCallback);
    },
    function(err)
    {
      callback(currentAsset);
    });
  }
}
