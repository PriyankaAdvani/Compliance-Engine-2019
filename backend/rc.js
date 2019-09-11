const ComplianceAnalysisEngine = require("./complianceanalysisengine");
const LedgerParser = require("./LedgerParser");
const TransactionParser = require("./TransactionParser");
const AssetParser = require("./AssetParser");
const AnalysisOutputSink = require("./analysisoutputsink");
const winston = require('winston')
const assert = require("assert");
const config = require("config");

winston.level = 'debug';

processData();
function processData () {
  var ledgerParser = new LedgerParser(config.get('ledger-parser'));
  var transactionParser = new TransactionParser(config.get('transaction-parser'));
  var analysisOutputSink = new AnalysisOutputSink(config.get('analysis-output'));
  var assetParser = new AssetParser(config.get('asset-parser'));

  var cae = new ComplianceAnalysisEngine(config.get('analysis'), ledgerParser, transactionParser, assetParser, analysisOutputSink);
  cae.runAnalysis();
}
