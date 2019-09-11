var hfc = require('fabric-client');
var utils = require('fabric-client/lib/utils.js');
var logger = utils.getLogger('AssetJoinEnumerator.js');
var callsite = require('callsite');
var JSONPath = require('jsonpath-plus');
const winston = require('winston')
const assert = require("assert");



var __func = function() {
    var func = callsite()[1].getFunctionName();
    return (func ? func : 'anonymous') + '[' + callsite()[1].getLineNumber() + ']: ';
};


winston.level = 'debug';



module.exports = class AssetJoinEnumerator {

  constructor (rootAssets, assetDbName, assetNano, joinConfiguration) {
    this.rootAssets = rootAssets;
    this.assetDbName = assetDbName;
    this.assetNano = assetNano;
    this.joinConfiguration = joinConfiguration;

    this.count = this.rootAssets.length;
    this.index = 0;

    this.joiningSelector = this.createSelectorFromFilter(this.joinConfiguration.filter);
    logger.info("Join - filter based selector: ", JSON.stringify(this.joiningSelector));
    this.joiningSelector = this.appendJoinConditionToSelector(this.joiningSelector);
    logger.info("Join - condition based selector: ", JSON.stringify(this.joiningSelector));
  }

  hasNext () {
    return this.index < this.count;
  }

  getNext (callback) {
    //this.getNextCallback = callback;
    this.currentAsset = this.rootAssets[this.index];
    logger.info("Loading asset: ", this.currentAsset._id);
    delete this.currentAsset._id;
    delete this.currentAsset._rev;

    var currentSelector = JSON.parse(JSON.stringify(this.joiningSelector));
    this.fillJoinQueryParameters(currentSelector);
    logger.info("Current selector: ", JSON.stringify(currentSelector));
    this.index++;

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
      getNextCallback(rootAsset);
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

/*
    if (joinAssets.length == 1) {
      rootAsset[appentKey] = joinAssets[0];
    }
    else {
      rootAsset[appentKey] = joinAssets;
    }
*/
    //logger.info("Joined asset: ", JSON.stringify(this.currentAsset));
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
