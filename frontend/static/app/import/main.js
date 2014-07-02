/*
 * Import Configuration
 */

quarry.config(['$stateProvider', function($stateProvider) {

    // make sure we have a user before allowing access to the rest of the app
    if (!window.user) {
        return
    }

    // Set up them states
    $stateProvider

        //
        // Import States
        //

        .state('import', {
            url: '/import',
            templateUrl: '/static/app/import/partials/base.html',
        })

        //
        // Wizard States
        //

        .state('import.wizard', {
            url: '/wizard',
            templateUrl: '/static/app/import/partials/wizard.html',
            controller: 'ImportWizardController'
        })
        .state('import.saved', {
            url: '/saved',
            templateUrl: '/static/app/import/partials/saved.html',
            controller: 'ImportSavedController'
        })
        .state('import.wizard-database', {
            url: '/wizard-database',
            templateUrl: '/static/app/import/partials/database.html',
            controller: 'ImportWizardDatabaseController'
        })
        .state('import.wizard-mysql', {
            url: '/wizard-mysql',
            templateUrl: '/static/app/import/partials/mysql/import.html',
            controller: 'ImportWizardMySQLController'
        })
        .state('import.history', {
            url: '/history',
            templateUrl: '/static/app/import/partials/history.html',
            controller: 'ImportHistoryController'
        })
        .state('import.streaming', {
            url: '/streaming',
            templateUrl: '/static/app/import/partials/streaming.html',
            controller: 'ImportStreamingController'
        })

}])

/*
 * Jaunt Factory
 */

quarry.factory("Jaunt", ['$http', '$timeout', 'Flint', function($http, $timeout, Flint) {

    return {

        setCluster: function(cluster) {
            this.cluster = cluster
            Flint.setCluster(cluster)
        },

        deleteDatajob: function(id) {
            return $http.post("/api/jaunt/datajob/"+id+"/delete")
        },

        getDatajob: function(id) {
            return $http.get("/api/jaunt/datajobs/"+id)
        },

        getSavedImports: function(offset, count) {
            return $http.get("/api/jaunt/datajobs/import", {
                params: {
                    count: count,
                    offset: offset
                }
            })
        },

        getSavedExports: function(offset, count) {
            return $http.get("/api/jaunt/datajobs/export", {
                params: {
                    count: count,
                    offset: offset
                }
            })
        },

        list: function(db) {
            return $http.gett("/api/jaunt/list")
        },

        datasets: function(db) {
            return $http.get("/api/jaunt/datasets", {
                params: {
                    cluster: this.cluster
                }
            })
        },

        inspect: function(db, options) {
            return $http.post("/api/jaunt/"+db+"/inspect", options)
        },

        describe: function(db, options) {
            return $http.post("/api/jaunt/"+db+"/describe", options)
        },

        saveImport: function(db, options, progressCallback, dataCallback, errorCallback) {
            return this._saveJob("import", db, options)
        },

        saveExport: function(db, options, progressCallback, dataCallback, errorCallback) {
            return this._saveJob("export", db, options)
        },

        import: function(db, options, progressCallback, dataCallback, errorCallback) {
            this._execute("import", db, options, progressCallback, dataCallback, errorCallback)
        },

        export: function(db, options, progressCallback, dataCallback, errorCallback) {
            this._execute("export", db, options, progressCallback, dataCallback, errorCallback)
        },

        _saveJob: function(type, db, options) {
            options.save = true
            return $http.post("/api/jaunt/"+db+"/"+type, options)
        },

        _execute: function(type, db, options, progressCallback, dataCallback, errorCallback) {
            options.cluster = this.cluster
            $http.post("/api/jaunt/"+db+"/"+type, options)
                .success(function(res) {
                    Flint._watchHandle(res.handle, progressCallback, dataCallback, errorCallback)
                })
                .error(function(res) {
                    errorCallback(res.error)
                })
        }

    }

}])

/*
 * Import Controllers
 */

quarry.controller('ImportWizardController', ["$scope", function($scope) {
    // nothing to see here
}])

quarry.controller('ImportWizardDatabaseController', ["$scope", function($scope) {
    // nothing to see here
}])

quarry.controller('ImportWizardMySQLController', ["$scope", "Jaunt", "RedShirt", "Notifications", function($scope, Jaunt, RedShirt, Notifications) {

    $scope.step = "cluster"

    /*
     * Errors
     */

    $scope.dismissErrors = function() {
        delete $scope.errors
    }

    /*
     * Cluster
     */

    $scope.selectedCluster = function(cluster) {
        $scope.cluster = cluster
    }

    $scope.chooseCluster = function() {
        Jaunt.setCluster($scope.cluster)
        $scope.step = "inspect"
    }

    /*
     * Inspect Step
     */

    $scope.inspectDatabase = function(address, username, password) {

        delete $scope.errors

        $scope.address = address
        $scope.username = username
        $scope.password = password

        $scope.status = "Gathering information about your database..."
        Jaunt.inspect('mysql', {
            host: address,
            dbuser: username,
            password: password
        })
            .success(function(res) {
                $scope.datasets = res.datasets
                $scope.step = "import"
            })
            .error(function(res) {
                $scope.errors = res.errors
            })
            .finally(function() {
                delete $scope.status
            })

    }

    /*
     * Choose Dataset Step
     */

    $scope.chooseDataset = function(dataset) {

        delete $scope.errors

        $scope.dataset = dataset
        $scope.status = "Gathering information about your dataset..."

        Jaunt.describe('mysql', {
            host: $scope.address,
            dbuser: $scope.username,
            password: $scope.password,
            database: dataset.database,
            table: dataset.table
        })
            .success(function(res) {
                $scope.columns = res.dataset
                $scope.splitBy = $scope.columns[0].name
                $scope.selectedColumns = {}
                $scope.sharkTableName = $scope.dataset.database + "_" + $scope.dataset.table
                for (var i = 0, len = $scope.columns.length; i < len; i++) {
                    $scope.selectedColumns[$scope.columns[i].name] = true
                }
                $scope.step = "importOptions"
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

    $scope.beginImport = function(selectedColumns, splitBy, sharkTableName, importType, save) {

        $scope.selectedColumns = selectedColumns
        $scope.splitBy = splitBy
        $scope.sharkTableName = sharkTableName
        $scope.importType = importType

        delete $scope.errors
        delete $scope.stageProgress
        delete $scope.taskProgress

        var errors = []
        if (!splitBy) {
            errors.push("You must select a column to split by")
        }
        if (!sharkTableName) {
            errors.push("You must enter a name for your dataset")
        }

        if (errors.length != 0) {
            $scope.errors = errors
            return
        }

        var options = {
            host: $scope.address,
            dbuser: $scope.username,
            password: $scope.password,
            database: $scope.dataset.database,
            table: $scope.dataset.table,
            sharkTable: sharkTableName,
            splitBy: splitBy,
            importType: importType
        }

        if (save) {

            $scope.status = "Saving job..."

            Jaunt.saveImport('mysql', options)
                .success(function() {
                    Notifications.local({
                        message: 'Import Job Saved'
                    })
                })
                .finally(function() {
                    delete $scope.status
                })

        } else {

            $scope.status = "Importing data from " + $scope.dataset.database + "." + $scope.dataset.table + " into " + sharkTableName+"..."

            Jaunt.import(
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
                            $scope.status = "Running import..."
                        }
                    } else {
                        $scope.status = "Import finished..."
                    }
                },
                function(res) {
                    delete $scope.status
                    $scope.step = "success"
                },
                function(res) {
                    delete $scope.status
                    $scope.errors = [res.error]
                }
            )

        }

    }

}])

quarry.controller("ImportSavedController", ["$scope", "Jaunt", function($scope, Jaunt) {

    $scope.loadingImports = true

    Jaunt.getSavedImports()
        .success(function(res) {
            $scope.datajobs = res.datajobs
        })
        .finally(function() {
            $scope.loadingImports = false 
        })

}])

quarry.controller("ImportHistoryController", ["$scope", "$state", function($scope, $state) {

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

quarry.controller("ImportStreamingController", ["$scope", "$http", "RedShirt", function($scope, $http, RedShirt) {

    $scope.loadingStreamers = true

    RedShirt.getStreamers()
        .success(function(res) {
            $scope.streamers = res.streamers
        })
        .finally(function() {
            delete $scope.loadingStreamers
        })

    $scope.organization = user.account.organization.toLowerCase()
    $scope.secret = ""
    $scope.loadingSecret = true

    $http.get("/api/account/secret")
        .success(function(res) {
            $scope.secret = res.secret
        })
        .finally(function() {
            delete $scope.loadingSecret
        })

}])

