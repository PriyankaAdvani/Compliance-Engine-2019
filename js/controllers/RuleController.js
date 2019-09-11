
/** Controller for the Rule Details page ruleDetails.html **/

app.controller('RuleController', ['$scope', '$uibModal', '$log','$http', '$mdDialog',
  function($scope, $uibModal, $log, $http, $mdDialog) {
    //$http.get('data/color.json').success(function(data) {});


  $scope.title = "DMV Ledger";

  //Json data for Rule Details
  $scope.rules = [
    {
      id: 1,
      name: 'Valid-Registration',
      description: 'The rule is defined in Javascript. The editor provides highlighting and syntax checking.',
      condition: 'if (asset.registration.length > 1) \n\t for (var i = 1; i < asset.registration.length; i++) \n\t\t if (parseInt(asset.insurance[i-1].value.to) <  parseInt(asset.insurance[i].value.from)) \n\t\t return false; \n return true;'
    },
    {
      id: 2,
      name: 'Valid-Insurance',
      description: 'The rule is defined in Javascript. The editor provides highlighting and syntax checking.',
      condition: 'if (asset.insurance.length > 1)\n\t for (var i = 1; i < asset.insurance.length; i++) \n\t\t if (parseInt(asset.insurance[i-1].value.to) < \n\t\t parseInt(asset.insurance[i].value.from)) \n\t\t return false; \n return true;'
    },
    {
      id: 3,
      name: 'Fully Compliant',
      description: 'The rule is defined in Javascript. The editor provides highlighting and syntax checking.',
      condition: 'if (asset.compliant.length > 1)\n\t for (var i = 1; i < asset.compliant.length; i++) \n\t\t if (parseInt(asset.insurance[i-1].value.to) < \n\t\t parseInt(asset.insurance[i].value.from)) \n\t\t return false; \n return true;'
    },
    {
      id: 4,
      name: 'Valid-DriverLicense',
      description: 'The rule is defined in Javascript. The editor provides highlighting and syntax checking.',
      condition: 'if (asset.dl.length > 1)\n\t for (var i = 1; i < asset.dl.length; i++) \n\t\t if (parseInt(asset.insurance[i-1].value.to)< \n\t\tparseInt(asset.insurance[i].value.from)) \n\t\t return false; \n return true;'
    },
    {
      id: 5,
      name: 'Valid-Insurance2',
      description: 'The rule is defined in Javascript. The editor provides highlighting and syntax checking.',
      condition: 'if (asset.insurance2.length > 1)\n for (var i = 1; i < asset.insurance2.length; i++) \n if (parseInt(asset.insurance[i-1].value.to) < \n parseInt(asset.insurance[i].value.from)) \n return false; \n return true;'
    },
    {
      id: 6,
      name: 'Valid-DriverLicense2',
      description: 'The rule is defined in Javascript. The editor provides highlighting and syntax checking.',
      condition: 'if (asset.dl2.length > 1)\n for (var i = 1; i < asset.dl2.length; i++) \n if (parseInt(asset.insurance[i-1].value.to) < \n parseInt(asset.insurance[i].value.from)) \n return false; \n return true;'
    },
    {
      id: 7,
      name: 'Valid-Insurance3',
      description: 'The rule is defined in Javascript. The editor provides highlighting and syntax checking.',
      condition: 'if (asset.insurance3.length > 1)\n for (var i = 1; i < asset.insurance3.length; i++) \n if (parseInt(asset.insurance[i-1].value.to) < \n parseInt(asset.insurance[i].value.from)) \n return false; \n return true;'
    },
    {
      id: 8,
      name: 'Valid-DriverLicense3',
      description: 'The rule is defined in Javascript. The editor provides highlighting and syntax checking.',
      condition: 'if (asset.dl3.length > 1)\n for (var i = 1; i < asset.dl3.length; i++) \n if (parseInt(asset.insurance[i-1].value.to) < \n parseInt(asset.insurance[i].value.from)) \n return false; \n\nreturn true;'
    },
    {
      id: 9,
      name: 'Valid-Insurance4',
      description: 'The rule is defined in Javascript. The editor provides highlighting and syntax checking.',
      condition: 'if (asset.insurance4.length > 1)\n for (var i = 1; i < asset.insurance4.length; i++) \n if (parseInt(asset.insurance[i-1].value.to) < \n parseInt(asset.insurance[i].value.from)) \n return false; \n return true;'
    },
    {
      id: 10,
      name: 'Valid-DriverLicense4',
      description: 'The rule is defined in Javascript. The editor provides highlighting and syntax checking.',
      condition: 'if (asset.dl4.length > 1)\n for (var i = 1; i < asset.dl4.length; i++) \n if (parseInt(asset.insurance[i-1].value.to) < \n parseInt(asset.insurance[i].value.from)) \n return false; \n return true;'
    },
    {
      id: 11,
      name: 'Valid-Insurance5',
      description: 'The rule is defined in Javascript. The editor provides highlighting and syntax checking.',
      condition: 'if (asset.insurance5.length > 1)\n for (var i = 1; i < asset.insurance5.length; i++) \n if (parseInt(asset.insurance[i-1].value.to) < \n parseInt(asset.insurance[i].value.from)) \n return false; \n return true;'
    },
    {
      id: 12,
      name: 'Valid-DriverLicense5',
      description: 'The rule is defined in Javascript. The editor provides highlighting and syntax checking.',
      condition: 'if (asset.dl5.length > 1)\n for (var i = 1; i < asset.dl5.length; i++) \n if (parseInt(asset.insurance[i-1].value.to) < \n parseInt(asset.insurance[i].value.from)) \n return false; \n return true;'
    },
    {
      id: 13,
      name: 'Valid-Insurance6',
      description: 'The rule is defined in Javascript. The editor provides highlighting and syntax checking.',
      condition: 'if (asset.insurance6.length > 1)\n for (var i = 1; i < asset.insurance6.length; i++) \n if (parseInt(asset.insurance[i-1].value.to) < \n parseInt(asset.insurance[i].value.from)) \n return false; \n return true;'
    },
    {
      id: 14,
      name: 'Valid-DriverLicense6',
      description: 'The rule is defined in Javascript. The editor provides highlighting and syntax checking.',
      condition: 'if (asset.dl6.length > 1)\n for (var i = 1; i < asset.dl6.length; i++) \n if (parseInt(asset.insurance[i-1].value.to) < \n parseInt(asset.insurance[i].value.from)) \n return false; \n return true;'
    },
    {
      id: 15,
      name: 'Valid-Insurance7',
      description: 'The rule is defined in Javascript. The editor provides highlighting and syntax checking.',
      condition: 'if (asset.insurance7.length > 1)\n for (var i = 1; i < asset.insurance7.length; i++) \n if (parseInt(asset.insurance[i-1].value.to) < \n parseInt(asset.insurance[i].value.from)) \n return false; \n return true;'
    }
  ];


//Function to Select/Unselect All Rules in the list
  $scope.checkAll = function () {
        if ($scope.selectedAll) {
            $scope.selectedAll = false;
        } else {
            $scope.selectedAll = true;
        }
        angular.forEach($scope.rules, function (rule) {
            rule.Selected = $scope.selectedAll;
        });
    };

//Function to Edit a rule in the list
    $scope.editRules = function(rule) {
          $scope.selected = rule;
          angular.element('#editModal').modal();
        }

//Function to Display rule details in the list
        $scope.showRules = function(rule) {
              $scope.selected = rule;
              angular.element('#showModal').modal();
            }
        $scope.isSelected = function(rule) {
          return $scope.selected === rule;
        }


//Function to Start Analysis of the rules
        $scope.showConfirm = function(ev) {
                var message = "";
                for (var i = 0; i < $scope.rules.length; i++) {
                  if ($scope.rules[i].Selected) {
                     var ruleName = $scope.rules[i].name;
                     message +=  "&nbsp;" + (i+1) + ": " + ruleName + "<br>";
                  }
                }


                if (message.length > 0) {
                    // Appending dialog to document.body to cover sidenav in docs app
                    var confirm = $mdDialog.confirm()
                          .title('Start Analysis!')
                          .htmlContent("Are you sure you want to start the Analysis?<br><br> Following rules are selected: <br>"+ message)
                          .ariaLabel('Lucky day')
                          .targetEvent(ev)
                          .ok('Yes, Sure!')
                          .cancel('No, Wait!');
                  }

                  else {
                        // Appending dialog to document.body to cover sidenav in docs app
                        var confirm = $mdDialog.alert()
                              .parent(angular.element(document.querySelector('#popupContainer')))
                               .clickOutsideToClose(true)
                               .title('Sorry!!')
                               .textContent('You have not selected any Rules!!')
                               .ariaLabel('Alert Dialog Demo')
                               .ok('Got it!')
                               .targetEvent(ev)
                  }


                  $mdDialog.show(confirm).then(function() {
                    $scope.status = 'You decided to get rid of your debt.';
                  }, function() {
                    $scope.status = 'You decided to keep your debt.';
                  });
          };


          function DialogController($scope, $mdDialog) {
            $scope.hide = function() {
              $mdDialog.hide();
            };

            $scope.cancel = function() {
              $mdDialog.cancel();
            };

            $scope.answer = function(answer) {
              $mdDialog.hide(answer);
            };
          }


//Function to Remove/Delete RULES
          $scope.removeRule = function(rule) {
            if (confirm("Are you sure to delete?")) {
                var i = $scope.rules.indexOf(rule);
                $scope.rules.splice(i, 1);
          }
          };


//Function to Add New RULES
$scope.open = function () {

    var uibModalInstance = $uibModal.open({
      templateUrl: 'views/addRule.html',
      controller: 'ModalInstanceCtrl',
      resolve: {
        data: function () {
          return $scope.rules;
        }
      }
    });

    uibModalInstance.result.then(function (data) {
      $scope.addRule = data;
      $scope.rules.push($scope.addRule);
     // $scope.data.push({'id':$scope.data.id,'name':$scope.data.name});
    }, function () {
      $log.info('Modal dismissed at: ' + new Date());
    });
  };


//Function to send user to top of the Page
$scope.backToTop = function() {
          $("html, body").animate({ scrollTop: 0 }, 1000);
      }

      $scope.currentPage = 0;
    $scope.pageSize = 10;

    $scope.numberOfPages=function(){
        return Math.ceil($scope.rules.length/$scope.pageSize);
    }

}]);

app.filter('startFrom', function() {
    return function(input, start) {
        start = +start; //parse to int
        return input.slice(start);
    }
});



app.controller('ModalInstanceCtrl', function ($scope, $uibModalInstance, data) {
$scope.addRule = {id: "",name:"",description:"",condition:""};
  $scope.ok = function () {
    $uibModalInstance.close($scope.addRule);

  };

  $scope.cancel = function () {
    $uibModalInstance.dismiss('cancel');
  };
});
