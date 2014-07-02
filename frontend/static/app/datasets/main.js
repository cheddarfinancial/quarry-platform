/*
 * Import Configuration
 */

quarry.config(['$stateProvider', function($stateProvider) {

    if (!window.user) {
        return
    }

    $stateProvider
        .state('datasets', {
            url: '/datasets',
            templateUrl: '/static/app/datasets/partials/base.html',
        })
        .state('datasets.raw', {
            url: '/raw',
            templateUrl: '/static/app/datasets/partials/raw.html',
            controller: 'DatasetsRawController'
        })
        .state('datasets.add_raw', {
            url: '/addraw/:name',
            templateUrl: '/static/app/datasets/partials/addraw.html',
            controller: 'DatasetsRawAddController'
        })
        .state('datasets.tables', {
            url: '/tables',
            templateUrl: '/static/app/datasets/partials/tables.html',
            controller: 'DatasetsTablesController'
        })

}])

/*
 * Datasets Controllers
 */

quarry.controller('DatasetsRawController', ["$scope", "Flint", function($scope, Flint) {

    $scope.loadingDatasets = true

    Flint.getRawDatasets()
        .success(function(res) {
            $scope.datasets = res.datasets
        })
        .error(function(res) {
            $scope.error = res.error
        })
        .finally(function() {
            $scope.loadingDatasets = false
        })

}])

quarry.controller('DatasetsRawAddController', ["$scope", "$stateParams", "Flint", function($scope, $stateParams, Flint) {

    $scope.name = $stateParams.name

    $scope.uploadFile = function() {

        $scope.uploadingFile = true
        delete $scope.error

        $scope.filename = $scope.file.name

        Flint.uploadFileIntoDataset($scope.name, $scope.file)
            .success(function(res) {
                console.log(res)
            })
            .error(function(res) {
                res.error = error
            })
            .finally(function() {
                $scope.uploadingFile = false
            })

    }

    $scope.dismissError = function() {
        delete $scope.error
    }

}])

quarry.controller('DatasetsTablesController', ["$scope", "Shark", function($scope, Shark) {

    $scope.loadingTables = true

    Shark.tables()
        .success(function(res) {
            $scope.tables = res.tables
        })
        .error(function(res) {
            $scope.error = res.error
        })
        .finally(function() {
            $scope.loadingTables = false
        })

}])
