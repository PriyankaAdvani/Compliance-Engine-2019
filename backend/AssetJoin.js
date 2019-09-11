var hfc = require('fabric-client');
var utils = require('fabric-client/lib/utils.js');
var logger = utils.getLogger('AssetJoin.js');
var callsite = require('callsite');
var JSONPath = require('jsonpath-plus');
const winston = require('winston')
const assert = require("assert");



var __func = function() {
    var func = callsite()[1].getFunctionName();
    return (func ? func : 'anonymous') + '[' + callsite()[1].getLineNumber() + ']: ';
};


winston.level = 'debug';



module.exports = class AssetJoin {

  constructor (assetNano, joinConfiguration) {
    this.assetDbName = joinConfiguration.filter["database"];
    this.assetNano = assetNano;
    this.joinConfiguration = joinConfiguration;

    logger.info("Join configuration ...");
    logger.info("assetDbName: ", this.assetDbName);
    logger.info("assetDbName: ", JSON.stringify(joinConfiguration));

    this.joiningSelector = this.createSelectorFromFilter(this.joinConfiguration.filter);
    logger.info("Join - filter based selector: ", JSON.stringify(this.joiningSelector));
    this.joiningSelector = this.appendJoinConditionToSelector(this.joiningSelector);
    logger.info("Join - condition based selector: ", JSON.stringify(this.joiningSelector));
  }

  loadJoinAssets (currentAsset, callback) {
    this.currentAsset = currentAsset;
    //logger.info("Loading joined assets for: ", this.currentAsset);

    var currentSelector = JSON.parse(JSON.stringify(this.joiningSelector));
    this.fillJoinQueryParameters(currentSelector);
    logger.info("Current selector: ", JSON.stringify(currentSelector));

    var boundCallback = (this.processJoinAsset).bind(this, callback, this.currentAsset);
    this.queryDb(currentSelector, boundCallback);
  }

  processJoinValue(key, value) {
      //console.log(key + " : "+value);
      if (value.length > 0 && value.includes('%')) {
        var startPosition = value.indexOf('%') + 1;
        var endPosition = value.lastIndexOf('%');
        var queryString = value.substring(startPosition, endPosition);
        var result = JSONPath({json: this.currentAsset, path: queryString});

        if (result.length > 0) {
            result = result[0];
            return result;
        }
      }
  }

  fillJoinQueryParameters(element) {
    for (var i in element) {
        var result = this.processJoinValue.apply(this, [i, element[i]]);

        if (result != undefined && result.length > 0) {
          element[i] = result;
        }

        if (element[i] !== null && typeof(element[i])=="object") {
            this.fillJoinQueryParameters(element[i]);
        }
    }
  }

  queryDb(selector, boundCallback) {

    this.assetNano.request({
        db: this.assetDbName,
        method: 'POST',
        doc: '_find',
        body: selector
      },
      boundCallback);
  }

  processJoinAsset(getNextCallback, rootAsset, err, data)
  {
    if(!err) {
      var joinAssets = data.docs;

      //logger.info("Number of join assets: ", joinAssets.length);
      this.appendJoinAssets(rootAsset, joinAssets);
      getNextCallback();
    }
    else {
      logger.error("Error loading join assets - ", err);
    }
  }

  appendJoinAssets(rootAsset, joinAssets) {

    var appentKey = this.joinConfiguration['append-attribute'];
    //logger.info("Append attribute: ", appentKey);


    for(var i = 0; i < joinAssets.length; i++) {
      delete joinAssets[i]._id;
      delete joinAssets[i]._rev;
    }

    rootAsset[appentKey] = joinAssets;
  }

  appendJoinConditionToSelector(selector) {
    var joinCondition = this.joinConfiguration['join-condition'];
    var keys = Object.keys(joinCondition);

    for(var j = 0; j < keys.length; j++){
      var key = keys[j];
      var conditionItem = JSON.parse(JSON.stringify(joinCondition[key]));
      selector.selector[key] = conditionItem;
    }

    return selector;
  }

  createSelectorFromFilter(filter) {

    var selector = JSON.parse(JSON.stringify(filter));


    delete selector["asset-type"];
    delete selector["database"];

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
}
