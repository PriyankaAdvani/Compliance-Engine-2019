var utils = require('fabric-client/lib/utils.js');
var logger = utils.getLogger('ComplianceAnalysisEngine.js');
var callsite = require('callsite');
const nanorete = require("./nanorete")
const assert = require("assert");


var __func = function() {
    var func = callsite()[1].getFunctionName();
    return (func ? func : 'anonymous') + '[' + callsite()[1].getLineNumber() + ']: ';
};

module.exports = class RuleEngine {

	constructor(jsonRules) {
		assert(jsonRules != null, 'jsonRules is null');
		assert(jsonRules.length > 0, 'jsonRules is empty');

		logger.info(__func() + "Initiating new RuleEngine instance ...");


		this.loadNanoreteRules(jsonRules);
		this.loadResultTemplate();
		this.nanoreteEngine = new nanorete.service.JSSimpleRuleEngine();
		this.nanoreteEngine.ruleset(["asset", "result"], this.nanoreteRules);

		logger.info(__func() + "RuleEngine is ready");
	}

	processAsset(asset) {
		assert(asset != null, 'asset is null');

		var result = JSON.parse(this.resultTemplate);
		var parameters = {"asset" : asset, "result": result};

		//logger.info(__func() + "Processing asset: " + JSON.stringify(asset));

		this.nanoreteEngine.executeRules(parameters);

		logger.info(__func() + "Result: " + JSON.stringify(result));
		return result;
	}

	loadNanoreteRules (jsonRules) {
		logger.info(__func() + "Loading nanorete rules from json rules ...");

		this.nanoreteRules = [];
		var pthis = this;

		jsonRules.forEach(function(jsonRule) {
			var nanoreteRule = {};
			nanoreteRule.name = jsonRule.name;
			nanoreteRule.priority = jsonRule.priority;
			logger.info(__func() + JSON.stringify(nanoreteRule));
			logger.info(__func() + "when: " + jsonRule.when.toString());
			nanoreteRule.when = pthis.instantiateFunctionFromString(jsonRule.when);
			logger.info(__func() + "then: " + jsonRule.then.toString());
			nanoreteRule.then = pthis.instantiateFunctionFromString(jsonRule.then);
			pthis.nanoreteRules.push(nanoreteRule);
		});
	}

	instantiateFunctionFromString(functionString) {
		var functionParts = functionString.toString().match(/.*function\s*\(\s*([^/**/)]*)[/**/]*?\s*\)\s*([^]*)/m);
		var functionArguments = functionParts[1].split(/\s|,/);
		var functionBody;
		var newFunction;


		functionArguments = functionArguments.filter(function(e){return e});
		functionBody = functionParts[2];
		newFunction = new Function(functionArguments, functionBody);

		return newFunction;
	}

	loadResultTemplate() {
		var resultTemplate = {};

		this.nanoreteRules.forEach(function(nanoreteRule) {
			resultTemplate[nanoreteRule.name] = false;
		});

		resultTemplate = JSON.stringify(resultTemplate);

		logger.info(__func() + "Result template: " + resultTemplate);
		this.resultTemplate = resultTemplate;
	}
}
