/*
 * Export Configuration
 */

quarry.config(['$stateProvider', function($stateProvider) {

    // make sure we have a user before allowing access to the rest of the app
    if (!window.user) {
        return
    }

    // Set up them states
    $stateProvider

        //
        // Export States
        //

        .state('export', {
            url: '/export',
            templateUrl: '/static/app/export/partials/base.html',
        })

        //
        // Wizard States
        //

        .state('export.saved', {
            url: '/saved',
            templateUrl: '/static/app/export/partials/saved.html',
            controller: 'ExportSavedController'
        })
        .state('export.wizard', {
            url: '/wizard',
            templateUrl: '/static/app/export/partials/wizard.html',
            controller: 'ExportWizardController'
        })
        .state('export.wizard-database', {
            url: '/wizard-database',
            templateUrl: '/static/app/export/partials/database.html',
            controller: 'ExportWizardDatabaseController'
        })
        .state('export.wizard-mysql', {
            url: '/wizard-mysql',
            templateUrl: '/static/app/export/partials/mysql/export.html',
            controller: 'ExportWizardMySQLController'
        })
        .state('export.history', {
            url: '/history',
            templateUrl: '/static/app/export/partials/history.html',
            controller: 'ExportHistoryController'
        })

}])

/*
 * Import Controllers
 */

quarry.controller('ExportWizardController', ["$scope", function($scope) {
    // nothing to see here
}])

quarry.controller('ExportWizardDatabaseController', ["$scope", function($scope) {
    // nothing to see here
}])

quarry.controller('ExportWizardMySQLController', ["$scope", "Jaunt", "RedShirt", "Notifications", function($scope, Jaunt, RedShirt, Notifications) {

    $scope.step = "cluster"

    /*
     * Errors
     */

    $scope.dismissErrors = function() {
        delete $scope.errors
    }

    /*
     * Clusters
     */

    $scope.selectedCluster = function(cluster) {
        $scope.cluster = cluster
    }

    $scope.chooseCluster = function() {
        Jaunt.setCluster($scope.cluster)        
        fetchDatasets()
    }


    /*
     * Choose Dataset Step
     */

    var fetchDatasets = function() {

        $scope.status = "Getting list of available datasets..."
        Jaunt.datasets()
            .success(function(res) {
                delete $scope.status
                $scope.step = "datasets"
                $scope.datasets = res.datasets
            })
            .error(function(res) {
                $scope.status = "Failed to load datasets. Please retry..."
            })

        $scope.chooseDataset = function(dataset) {

            delete $scope.errors

            $scope.dataset = dataset
            $scope.step = "connect"

        }

    }

    /*
     * Connect Step
     */

    $scope.connectDatabase = function(address, username, password) {

        delete $scope.errors

        $scope.address = address
        $scope.username = username
        $scope.password = password

        $scope.status = "Testing connection to database..."
        Jaunt.inspect('mysql', {
            host: address,
            dbuser: username,
            password: password
        })
            .success(function(res) {
                $scope.step = "exportOptions"
            })
            .error(function(res) {
                $scope.errors = res.errors
            })
            .finally(function() {
                delete $scope.status
            })

    }

    /*
     * Import Options Step
     */

    $scope.beginExport = function(database, table, save) {

        delete $scope.errors

        $scope.database = database
        $scope.table = table

        var errors = []
        if (!database) {
            errors.push("You must enter a MySQL database name.")
        }
        if (!table) {
            errors.push("You must enter a MySQL table name.")
        }

        if (errors.length != 0) {
            $scope.errors = errors
            return
        }

        var options = {
            host: $scope.address,
            dbuser: $scope.username,
            password: $scope.password,
            dataset: $scope.dataset,
            database: $scope.database,
            table: $scope.table,
        }

        if (save) {

            $scope.status = "Saving job..."

            Jaunt.saveExport('mysql', options)
                .success(function() {
                    Notifications.local({
                        message: 'Export Job Saved'
                    })
                })
                .finally(function() {
                    delete $scope.status
                })

        } else {

            $scope.status = "Exporting data from '" + $scope.dataset +
                            "into " + database + "." + table + "..."

            Jaunt.export(
                'mysql', 
                options,
                function(currentProgress) {
                    if (currentProgress.running) {
                        if (Object.keys(currentProgress.progress).length && currentProgress.progress.currentStage) {
                                var progress = currentProgress.progress
                                currentStage = progress.currentStage
                                $scope.status = '' + currentStage.completeTasks + " of " + currentStage.totalTasks + " tasks complete on stage " +
                                                (progress.completeStages+1) + " of " + progress.totalStages + " total stages..."
                                $scope.stageProgress = progress.stageProgress * 97 + 3
                                $scope.taskProgress = currentStage.taskProgress * 97 + 3
                        } else {
                            $scope.status = "Running export..."
                        }
                    } else {
                        $scope.status = "Export finished..."
                    }
                },
                function(res) {
                    delete $scope.status
                    $scope.step = "success"
                },
                function(err) {
                    delete $scope.status
                    $scope.error = err.message
                }
            )

        }

    }

}])

quarry.controller("ExportSavedController", ["$scope", "Jaunt", function($scope, Jaunt) {

    $scope.loadingExports = true

    Jaunt.getSavedExports()
        .success(function(res) {
            $scope.datajobs = res.datajobs
        })
        .finally(function() {
            $scope.loadingExports = false
        })

}])

quarry.controller("ExportHistoryController", ["$scope", "$state", function($scope, $state) {

    $scope.actions = {
        "job_complete": {
            "name": "View Results",
            "action": function(history) {
                $state.go(
                    'spark.results',
                    {
                        jobName: history.data.jobName,
                        handle: history.job_handle

                    }
                )
            }
        }
    }

}])
