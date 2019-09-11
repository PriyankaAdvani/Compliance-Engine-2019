/** Controller for the Summary page summaryAnalysis.html **/

app.controller("SummaryController", ['$scope', function($scope) {

  $scope.title = "DMV Ledger";

  $scope.colours = ['#72C02C', '#3498DB', '#717984', '#F1C40F'];

    $scope.DataSetOverride = [{ yAxisID: 'y-axis-1' }]; //y-axis-1 is the ID defined in scales under options.

    $scope.options = {
          line : { tension : 0 },
          legend: { display: true },
          responsive: true,  // set to false to remove responsiveness. Default responsive value is true.
          scales: {
              yAxes: [
                  {
                    id: 'y-axis-1',
                    display: true,
                    scaleLabel: {
                        display: true,
                //        labelString: '# ',
                        fontStyle: 'bold'
                    },
                    ticks: {
                        fontStyle: 'bold',
                        beginAtZero: true,
                        stepSize: 250,
                        max: 1000
          //*              max: 100,
          //              callback: function(value) {
          //                return (value / this.max * 100).toFixed(0) + '%';
          //              }
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



  //Data Values and Details for Charts in SUmmary Details
  $scope.summary = {
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

      "result": [{
          "ruleName": "Compliance Summary Details",
          "seriesC": ["Compliant"],
          "seriesNC": ["Non-Compliant"],
          "ta": 780,
          "ca": 530,
          "nca": 250,
          "dataC": [["900", "789", "500", "925", "760", "600", "710", "87", "770"]],
          "dataNC": [["100", "190", "81", "410", "260", "150", "100", "127", "200"]],
          "labels": ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep"],
          "colours": [{ // default
            // ['#72C02C', '#3498DB', '#717984', '#F1C40F'];
            //"fillColor": "rgba(224, 108, 112, 1)",
            "fillColor": '#fff',
            "strokeColor": '#72C02C',
            "pointColor": '#72C02C',
            "pointStrokeColor": '#72C02C',
            //"strokeColor": "rgba(207,100,103,1)",
            //"pointColor": "rgba(220,220,220,1)",
            //"pointStrokeColor": "#fff",
            "pointHighlightFill": "#fff",
            "pointHighlightStroke": "rgba(151,187,205,0.8)"
          }],
        }]
    };

}]);
