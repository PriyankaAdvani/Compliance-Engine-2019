var app = angular.module('myApp', ['ngRoute', 'ngMaterial', 'ngMessages', 'ngSanitize', 'ui.bootstrap', 'chart.js']);

app.config(function($routeProvider) {
    $routeProvider
        .when('/', {
            templateUrl:'views/main.html',
            controller:'MainController'
          })
        .when('/rules', {
            templateUrl:'views/ruleDetails.html',
            controller:'RuleController'
        })
        .when('/summaryAnalysis', {
            templateUrl:'views/summaryAnalysis.html',
            controller:'SummaryController'
        })
        .when('/summaryDetail', {
            templateUrl:'views/summaryDetail.html',
            controller:'ChartController'
        })
        .otherwise({
            redirectTo:'/'
        });
});
