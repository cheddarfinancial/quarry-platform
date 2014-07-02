// route config
quarry.config(['$stateProvider', function($stateProvider) {

    // make sure we have a user before allowing access to the rest of the app
    if (!window.user) {
        return
    }

    // Set up them states
    $stateProvider
    
        //
        // SQL States
        //

        .state('sql', {
            url: '/sql',
            templateUrl: '/static/app/sql/partials/base.html',
        })
        .state('sql.editor', {
            url: '/editor?queryId&run',
            templateUrl: '/static/app/sql/partials/editor.html',
            controller: 'SQLEditorController',
        })
        .state('sql.running', {
            url: '/running?jobType',
            templateUrl: '/static/app/jobs/partials/manager.html',
            controller: 'JobsManagerController'
        })
        .state('sql.saved', {
            url: '/saved',
            templateUrl: '/static/app/sql/partials/saved.html',
            controller: 'SQLSavedController'
        })
        .state('sql.history', {
            url: '/history',
            templateUrl: '/static/app/sql/partials/history.html',
            controller: 'SQLHistoryController'
        })
        .state('sql.results', {
            url: '/results/:handle/:jobName',
            templateUrl: '/static/app/sql/partials/results.html',
            controller: 'SQLResultsController'
        })

}])

/*
 * Shark Factory
 */

quarry.factory("Shark", ['$http', '$timeout', function($http, $timeout) {

    return {

        setCluster: function(cluster) {
            this.cluster = cluster
        },

        getHistory: function(offset, count) {
            return $http.get("/api/shark/histories", {
                params: {
                    count: count,
                    offset: offset
                }
            })
        },

        savedQueries: function(offset, count) {
            return $http.get("/api/shark/queries")
        },

        savedQuery: function(queryId) {
            return $http.get("/api/shark/query/"+queryId)
        },

        updateQuery: function(queryId, query) {
            return $http.post("/api/shark/query/"+queryId+"/update", {
                cluster: this.cluster,
                query: query
            })
        },

        saveQuery: function(title, description, sql) {
            return $http.post("/api/shark/query/save", {
                title: title,
                description: description,
                query: sql
            })
        },

        deleteQuery: function(queryId) {
            return $http.post("/api/shark/query/"+queryId+"/delete")
        },

        descTable: function(tableName) {
            return $http.get("/api/shark/table/"+tableName+"/schema", {
                params: {
                    cluster: this.cluster
                }    
            })
        },

        getTables: function(cluster) {
            return $http.get("/api/shark/tables", {
                params: {
                    cluster: this.cluster
                }    
            })
        },

        cacheTable: function(table, progressCallback, dataCallback, errorCallback) {

            var self = this

            $http.post("/api/shark/table/"+table+"/cache", {
                cluster: this.cluster
            })
                .success(function(res) {
                    self._watchHandle(res.handle, progressCallback, dataCallback, errorCallback)
                })
                .error(function(res) {
                    errorCallback(res)
                })

        },

        uncacheTable: function(table, progressCallback, dataCallback, errorCallback) {

            var self = this

            $http.post("/api/shark/table/"+table+"/uncache", {
                cluster: this.cluster
            })
                .success(function(res) {
                    self._watchHandle(res.handle, progressCallback, dataCallback, errorCallback)
                })
                .error(function(res) {
                    errorCallback(res)
                })

        },

        dropTable: function(table, progressCallback, dataCallback, errorCallback) {

            var self = this

            return $http.post("/api/shark/table/"+table+"/drop", {
                cluster: this.cluster
            })

        },

        getResultFiles: function(handle) {
            return $http.get("/api/shark/resultfiles", {
                params: {
                    handle: handle
                }
            })
        },

        getResult: function(handle, callback) {
            return $http.get("/api/shark/results", {
                params: {
                    handle: handle
                }
            })
        },

        getCursor: function(handle, callback) {

            var self = this

            $http.get("/api/shark/cursor", {
                params: {
                    cluster: this.cluster,
                    handle: handle
                }
            })
                .success(function(res) {
                    var handle = res.handle
                    callback({
                        handle: handle,
                        fetchN: function(count) {
                            return $http.get("/api/shark/fetchn/"+count, {
                                params: {
                                    cluster: self.cluster,
                                    handle: handle
                                }
                            })
                        }
                    })
                })
                .error(function(res) {
                    errorCallback(res.results)
                })

        },

        execute: function(sql, python, options, progressCallback, dataCallback, errorCallback) {

            var self = this

            $http.post("/api/shark/sql", {
                query: sql,
                python: python,
                options: JSON.stringify(options),
                cluster: this.cluster
            })
                .success(function(res) {
                    self._watchHandle(res.handle, progressCallback, dataCallback, errorCallback)
                })
                .error(function(res) {
                    errorCallback(res)
                })

        },

        _watchHandle: function(handle, progressCallback, dataCallback, errorCallback) {
            
            var self = this

            $http.get("/api/shark/progress", {
                params: {
                    cluster: this.cluster,
                    handle: handle
                }
            })
                .success(function(res) {
                    progressCallback(res)
                    if (!res.running) {
                        $http.get("/api/shark/results", {
                            params: {
                                handle: handle
                            }
                        })
                            .success(function(results) {

                                dataCallback({
                                    results: results,
                                    handle: handle
                                })

                            })
                            .error(function(res) {
                                errorCallback(res.results)
                            })

                    } else {
                        $timeout(function() {
                            self._watchHandle(handle, progressCallback, dataCallback, errorCallback)
                        }, 1000)
                    }
                })
                .error(function(res) {
                    errorCallback(res)
                })


        }

    }

}])

quarry.controller("SQLEditorController", ["$scope", "$stateParams", "$modal", "Shark", "Notifications", function($scope, $stateParams, $modal, Shark, Notifications) {

    $scope.loadedQuery = false

    $scope.selectedCluster = function(clusterName) {

        $scope.cluster = clusterName
        Shark.setCluster($scope.cluster)

        // if we don't have a cluster, SHUT DOWN EVERYTHING
        if (!clusterName) {

            delete $scope.tables

        } else if (!$scope.tables) {

            Shark.getTables()
                .success(function(res) {

                    $scope.tables = []

                    res.tables.forEach(function(name) {
                        $scope.tables.push({
                            name: name,
                            loadingSchema: false,
                            showingSchema: false
                        })
                    })

                })

        }

    }

    $scope.noClusters = function() {

        var modal = $modal.open({
            templateUrl: 'static/app/sql/partials/launchClusterModal.html',
            controller: ['$scope', function(modalScope) {
                modalScope.dismiss = function() {
                    modal.dismiss()
                }
            }],
            backdrop: 'static'
        })

    }

    if ($stateParams.queryId && !$scope.loadedQuery) {

        Shark.savedQuery($stateParams.queryId)
            .success(function(res) {

                $scope.queryId = res.query.id
                $scope.sql = res.query.sql

            })
            .error(function(res) {

                $scope.error = res.error

            })
            .finally(function() {
                
                $scope.loadedQuery = true

            })

    }

    $scope.showEditor = true
    $scope.priority = "default"

    $scope.sqlEditorOptions = {
        lineWrapping : true,
        lineNumbers: true,
        mode: 'sql'
    }

    $scope.pythonEditorOptions = {
        lineWrapping : true,
        lineNumbers: true,
        mode: 'python'
    }

    if ($stateParams.queryId) {
   
        $scope.sql = "Loading saved query..."
        $scope.python = "Loading saved python code..."

    }

    $scope.descTable = function(table) {
        
        if (table.loadingSchema) {

            return

        } else if (table.showingSchema) {

            table.showingSchema = false
            return

        } else if (table.schema) {

            table.showingSchema = true
            return

        } else {

            table.loadingSchema = true

            Shark.descTable(table.name)       
                .success(function(schema) {
                    table.schema = schema
                    table.loadingSchema = false
                    table.showingSchema = true
                })

        }

    }

    $scope.dismissError = function() {
        delete $scope.error
    }

    $scope.executeQuery = function(sql, python) {

        sql = sql || ""
        python = python || ""

        $scope.sql = sql
        $scope.python = python

        delete $scope.error
        delete $scope.results
        delete $scope.showResults
        delete $scope.handle
        delete $scope.stageProgress
        delete $scope.taskProgress
        $scope.showEditor = false
        $scope.status = "Launching query..."

        Shark.execute(
            sql, 
            python,
            {
                'jobName': $scope.jobName || "Untitled",
                'priority': $scope.priority
            },
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
                        $scope.status = "Running query..."
                    }
                } else {
                    $scope.status = "Query finished, fetching results..."
                }
            },
            function(results) {

                $scope.showResults = true

                delete $scope.status
                delete $scope.taskProgress
                delete $scope.stageProgress

                $scope.handle = results.handle

            },
            function(err) {
                delete $scope.results
                delete $scope.status
                $scope.showEditor = true
                $scope.error = err.error
            }
        )
    }

    $scope.clearResults = function() {
        $scope.showEditor = true
        delete $scope.handle
        delete $scope.showResults
    }

    $scope.toggleEditor = function() {
        $scope.showEditor = !$scope.showEditor
    }

    /*
     * SAVE QUERY MODAL
     */

    $scope.saveQuery = function(sql, python) {

        if ($scope.queryId) {

            Shark.updateQuery($scope.queryId, sql)
                .success(function(res) {
                    Notifications.local({
                        message: "Query saved."
                    })
                })

        } else {

            var saveQueryModal = $modal.open({
                templateUrl: 'static/app/sql/partials/saveQueryModal.html',

                controller: ['$scope', function(modalScope) {
                    
                    modalScope.query = {}

                    modalScope.dismissSaveQueryModal = function() {
                        saveQueryModal.close()
                    }

                    modalScope.saveQuery = function(title, description) {

                        modalScope.savingQuery = true
                        delete modalScope.saveQueryError

                        Shark.saveQuery(title, description, $scope.sql)
                            .success(function(res) {
                                Notifications.local({
                                    message: "Query saved."
                                })
                                saveQueryModal.close()
                            })
                            .error(function(res) {
                                modalScope.saveQueryError = res.error
                            })
                            .finally(function(res) {
                                modalScope.savingQuery = false
                            })

                    }

                    modalScope.dismissSaveError = function() {
                        delete modalScope.saveQueryError
                    }

                }]
            });

        }

    }

    /*
     * ALTER TABLE
     */

    $scope.cacheTable = function(table) {
        table.schema.cached = true
        Shark.cacheTable(table.name)
    }

    $scope.uncacheTable = function(table) {
        table.schema.cached = false
        Shark.uncacheTable(table.name)
    }

    $scope.dropTable = function(table) {
        $scope.tables = $scope.tables.filter(function(_table) {
            return _table.name != table.name
        })
        Shark.dropTable(table.name)
    }

}])

quarry.controller("SQLSavedController", ["$scope", "Shark", function($scope, Shark) {

}])

quarry.controller("SQLHistoryController", ["$scope", "$state", function($scope, $state) {

    $scope.actions = {
        "job_complete": {
            "name": "View Results",
            "action": function(history) {
                $state.go(
                    'sql.results',
                    {
                        jobName: history.data.jobName,
                        handle: history.job_handle

                    }
                )
            }
        }
    }

}])

quarry.controller("SQLResultsController", ["$scope", "$stateParams", function($scope, $stateParams) {

    $scope.handle = $stateParams.handle
    $scope.jobName = $stateParams.jobName

}])
