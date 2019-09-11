/** Controller for the Summary Details Charts Configuration page summaryDetail.html **/
app.config(function($mdThemingProvider) {
  $mdThemingProvider.theme('blue')
    .primaryPalette('blue', {
      'default': '200',
      'hue-1': '100',
      'hue-2': '600',
      'hue-3': 'A100'
    });
  $mdThemingProvider.alwaysWatchTheme(true);
});

app.controller("ChartController", ['$scope', function($scope) {

  $scope.title = "DMV Ledger";
  $scope.theme = 'blue';

  //Default Chart-Data set to be Absolute
  $scope.data = {
    cb1: 'absolute'
  };

  //Default Chart-type set to be Bar
  $scope.chartLine = "bar";

  $scope.colours = ['#72C02C', '#3498DB', '#717984', '#F1C40F'];

  $scope.DataSetOverride = [{
      lineTension: 0.2,
      fill: false

  }];


//Chart-options for Absolute values
  $scope.options = {
      legend: { display: true },
      responsive: true,  // set to false to remove responsiveness. Default responsive value is true.
      scales: {
          yAxes: [
              {
                  id: 'y-axis-1',
                  display: true,
                  scaleLabel: {
                      display: true,
                      fontStyle: 'bold'
                  },
                  ticks: {
                      fontStyle: 'bold',
                      beginAtZero: true
                  }
          }],
          xAxes: [
              {
                  scaleLabel: {
                      display: true,
                      labelString: 'Month',
                      type: 'linear',
                      position: 'left',
                      fontColor: '#72C02C',
                      fontStyle: 'bold'
                  },
                  ticks: {
                      fontStyle: 'bold'
                  }
          }]
    }
  };

//Chart-options for Percentage values
  $scope.optionsP = {
        legend: { display: true },
        responsive: true,  // set to false to remove responsiveness. Default responsive value is true
        scales: {
            yAxes: [{
                ticks: {
                    fontStyle: 'bold',
                    min: 0,
                    max: this.max,// Your absolute max value
                    callback: function (value) {
                        console.log(this.max);
                        return (value / this.max * 100).toFixed(0) + '%'; // convert it to percentage
                    },
                },
                scaleLabel: {
                    display: true,
                    fontStyle: 'bold',
                    labelString: 'Percentage',
                },
            }],
            xAxes: [{
                scaleLabel: {
                    display: true,
                    labelString: 'Month',
                    type: 'linear',
                    position: 'left',
                    fontColor: '#72C02C',
                    fontStyle: 'bold'
                },
                ticks: {
                    fontStyle: 'bold'
                }
            }]
      }
  };


//Data Values and Details for Charts in SUmmary Details
$scope.ocw = {
    //ruleName: Name of the Rule
    //seriesC: Chart-series for Compliant
    //seriesNC: Chart-series for Non-Compliant
    //ta: Total assets
    //ca: Compliant assets
    //nca: Non-Compliant Assets
    //dataC: Absolute Compliant Data values
    //dataPC: Percentage Compliant Data Values
    //dataNC: Absolute Non-Compliant Data Values
    //dataPNC: Percentage Non-COmpliant Data values
    //labels: Chart-labels for charts

    "modules": [{
        "ruleName": "Valid-Registration",
        "seriesC": ["Compliant"],
        "seriesNC": ["Non-Compliant"],
        "ta": 780,
        "ca": 530,
        "nca": 250,
        "dataC": [["90", "69", "78", "49", "67", "27", "66", "84"]],
        "dataPC": [["90", "99", "80", "91", "76", "75", "60", "67"]],
        "dataNC": [["10", "19", "81", "41", "26", "35", "10", "28"]],
        "dataPNC": [["10", "19", "81", "41", "26", "35", "10", "27"]],
        "labels": ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug"],
      },
      {
        "ruleName": "Valid-Insurance",
        "seriesC": ["Compliant"],
        "seriesNC": ["Non-Compliant"],
        "ta": 1450,
        "ca": 850,
        "nca": 600,
        "dataC": [["10", "19", "20", "11", "26", "35", "40", "57"]],
        "dataPC": [["90", "99", "80", "91", "76", "75", "60", "67"]],
        "dataNC": [["0", "10", "12", "1", "18", "20", "10", "26"]],
        "dataPNC": [["0", "10", "12", "1", "18", "20", "10", "26"]],
        "labels": ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug"],
      },
      {
        "ruleName": "Valid-DrivingLicense",
        "seriesC": ["Compliant"],
        "seriesNC": ["Non-Compliant"],
        "ta": 1450,
        "ca": 850,
        "nca": 600,
        "dataC": [["10", "19", "20", "11", "26", "35", "40", "57"]],
        "dataPC": [["90", "99", "80", "91", "76", "75", "60", "67"]],
        "dataNC": [["0", "10", "12", "1", "18", "20", "10", "26"]],
        "dataPNC": [["0", "10", "12", "1", "18", "20", "10", "26"]],
        "labels": ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug"],
      },
      {
        "ruleName": "Fully Compliant",
        "seriesC": ["Compliant"],
        "seriesNC": ["Non-Compliant"],
        "ta": 1450,
        "ca": 850,
        "nca": 600,
        "dataC": [["90", "99", "80", "91", "76", "75", "60", "67"]],
        "dataPC": [["90", "99", "80", "91", "76", "75", "60", "67"]],
        "dataNC": [["23", "29", "12", "10", "16", "15", "30", "27"]],
        "dataPNC": [["23", "29", "12", "10", "16", "15", "30", "27"]],
        "labels": ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug"],
      }

    ]
  };


//On-click Function on CHarts
    $scope.onClick = function (points, evt) {
    console.log(points, evt);
    };


// Toggle Function to change the Chart type to Bar or Line
$scope.type = 'bar';

$scope.toggle = function (s) {
           $scope.type = s;
       };




}]);
