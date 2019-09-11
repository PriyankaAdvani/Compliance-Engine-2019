const winston = require('winston')
const nanorete = require("./nanorete")


winston.level = 'debug';

module.exports = function (jsonRules) {

	this.nanoreteRules = loadNanoreteRules(jsonRules);
	this.resultTemplate = loadResultTemplate(this.nanoreteRules);
	this.nanoreteEngine = new nanorete.service.JSSimpleRuleEngine();
	this.nanoreteEngine.ruleset(["transaction", "asset", "result"], this.nanoreteRules);


	this.processAsset = function(transaction, asset) {
		var result = JSON.parse(this.resultTemplate);
		var parameters = {"transaction": asset, "asset" : asset.value, "result": result};

		winston.log('debug', "Processing asset ...");
		winston.log('debug', parameters.toString());

		nanoEngine.executeRules(parameters);

		winston.log('debug', result.toString());
		return result;
	}
}

function loadResultTemplate(nanoreteRules) {
	var resultTemplate = {};

	nanoreteRules.forEach(function(nanoreteRule) {
		resultTemplate[nanoreteRule.name] = false;
	});

	resultTemplate = JSON.stringify(resultTemplate);

	winston.log('debug', "Result template: " + resultTemplate);
	return resultTemplate;
}

function loadNanoreteRules (jsonRules) {
	winston.log('debug', "Loading nanorete rules from json rules ...");

	var nanoreteRules = [];

	jsonRules.forEach(function(jsonRule) {
		var nanoreteRule = {};
		nanoreteRule.name = jsonRule.name;
		nanoreteRule.priority = jsonRule.priority;
		nanoreteRule.when = instantiateFunctionFromString(jsonRule.when);
		nanoreteRule.then = instantiateFunctionFromString(jsonRule.then);
		nanoreteRules.push(currule);
	});

	winston.log('debug', nanoreteRules.toString());
	return nanoreteRules;
}

function instantiateFunctionFromString(functionString){
	var functionParts = functionString.toString().match(/.*function\s*\(\s*([^/**/)]*)[/**/]*?\s*\)\s*([^]*)/m);
	var functionArguments = functionParts[1].split(/\s|,/);


	functionArguments = functionArguments.filter(function(e){return e});
	functionBody = functionParts[2];
	newFunction = new Function(functionArguments, functionBody);

	return newFunction;
}



module.exports = {
	checkCompliance : function(rules, transaction, callback) {

		var nanoEngine = new nanorete.service.JSSimpleRuleEngine();


		var result = {};
var doc = {"transaction": transaction, "asset" : transaction.value, "result": result};

		function getBody(str){
			var results = str.toString().match(/.*function\s*\(\s*([^/**/)]*)[/**/]*?\s*\)\s*([^]*)/m);
			var args = results[1].split(/\s|,/);
			args = args.filter(function(e){return e});
			return {"args":args, "body":results[2]};
		}

		var objectrules = [];

		rules.forEach(function(rule) {
			var func_when = getBody(rule.when);
			var func_then = getBody(rule.then);
			var currule = {};
			currule.name = rule.name;
			currule.priority = rule.priority;
			currule.when = new Function(func_when.args, func_when.body);
			currule.then = new Function(func_then.args, func_then.body);

			result[rule.name] = false;

			//console.log(currule);
			//console.log(currule.when.toString());
			//console.log(currule.then.toString());

			objectrules.push(currule);

		});

		console.log(doc);

//console.log(result);
		nanoEngine.ruleset(["transaction", "asset", "result"], objectrules);
		nanoEngine.executeRules(doc);
console.log("...........");
		console.log(doc);
console.log("...........");

		callback(null, JSON.stringify(doc));
	},
};
