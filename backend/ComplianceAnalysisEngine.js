const RuleEngine = require("./RuleEngine");
var utils = require('fabric-client/lib/utils.js');
var logger = utils.getLogger('ComplianceAnalysisEngine.js');
var callsite = require('callsite');
const assert = require("assert");


var __func = function() {
    var func = callsite()[1].getFunctionName();
    return (func ? func : 'anonymous') + '[' + callsite()[1].getLineNumber() + ']: ';
};


/*
ComplianceAnalysisEngine
Loads assets from assetParser, executes compliance analysis, and sends results into the output sink
*/
module.exports = class ComplianceAnalysisEngine {

  constructor (analysisConfiguration, ledgerParser, transactionParser, assetParser, analysisOutputSink) {
    assert(analysisConfiguration != null, 'analysisConfiguration is null');
    //assert(analysisConfiguration.length > 0, 'analysisConfiguration has not defined asset types');
    assert(ledgerParser != null, 'ledgerParser is null');
    assert(transactionParser != null, 'transactionParser is null');
    assert(assetParser != null, 'assetParser is null');
    assert(analysisOutputSink != null, 'analysisOutputSink is null');

    logger.info(__func() + "....................................................");
    logger.info(__func() + "Initiating new ComplianceAnalysisEngine instance ...");

    this.analysisConfiguration = analysisConfiguration;
    this.ledgerParser = ledgerParser;
    this.transactionParser = transactionParser;
    this.assetParser = assetParser;
    this.analysisOutputSink = analysisOutputSink;
  }

  runAnalysis() {
    this.step1_parseLedger();
    //this.step3_checkRulesAndRecordResults();
  }

  step1_parseLedger() {
    logger.info(__func() + "Step 1 - Load transactions from ledger ...");

    var boundCallback = (this.step2_parseTransactions).bind(this);
//    this.ledgerParser.loadTransactions(boundCallback);
  }

  step2_parseTransactions() {
    logger.info(__func() + "Step 2 - Load assets from transactions ...");

    var boundCallback = (this.step3_checkRulesAndRecordResults).bind(this);
    this.transactionParser.loadAssets(boundCallback);
  }

  step3_checkRulesAndRecordResults() {
    logger.info(__func() + "Step 3 - Run rules and record results ...");

    var boundCallback = (this.runAnalysisForRootAssets).bind(this);

    //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    setTimeout(boundCallback, 1000);
  }

  runAnalysisForRootAssets () {
    var boundCallback = (this.iterateRootAssets).bind(this);
    this.assetParser.loadRootAssets(this.analysisConfiguration['root-filter'], boundCallback);
  }

  iterateRootAssets() {
    var assetJoinEnumerator = this.assetParser.getAssetJoinEnumerator(this.analysisConfiguration.joins);
    var ruleEngineWrapper = new RuleEngineWrapper(this.analysisConfiguration.rules, this.analysisOutputSink);


    ruleEngineWrapper.runAnalysis(assetJoinEnumerator);
  }
}


/*
  RuleEngineWrapper
  Wrappes the RuleEngine and invokes it for each asset in DataSource
*/

class RuleEngineWrapper {

  constructor(rulesConfiguration, analysisOutputSink) {
    assert(rulesConfiguration != null, 'rulesConfiguration is null');
    assert(rulesConfiguration.length > 0, 'rulesConfiguration is empty');
    assert(analysisOutputSink != null, 'analysisOutputSink is null');

    this.rulesConfiguration = rulesConfiguration;
    this.analysisOutputSink = analysisOutputSink;
    this.ruleEngine = new RuleEngine(rulesConfiguration);
  }

  runAnalysis (assetEnumerator) {
    assert(assetEnumerator != null, 'assetEnumerator is null');

    var boundCallback = (this.runAnalysisForAssetInstance).bind(this);


    while(assetEnumerator.hasNext()) {
      assetEnumerator.getNext(boundCallback);
    }
  }

  runAnalysisForAssetInstance (asset) {
    var result;


    //logger.info("Asset to process: ", JSON.stringify(asset));
    result = this.ruleEngine.processAsset(asset);
    this.analysisOutputSink.recordAnalysisOutput(asset, result);
  }
}
