// route config
quarry.config(['$stateProvider', function($stateProvider) {

    // make sure we have a user before allowing access to the rest of the app
    if (!window.user) {
        return
    }

    // Set up them states
    $stateProvider
    
        //
        // Spark States
        //

        .state('spark', {
            url: '/spark',
            templateUrl: '/static/app/spark/partials/base.html',
        })
        .state('spark.editor', {
            url: '/editor?jobId',
            templateUrl: '/static/app/spark/partials/editor.html',
            controller: 'SparkEditorController'
        })
        .state('spark.running', {
            url: '/running?jobType',
            templateUrl: '/static/app/jobs/partials/manager.html',
            controller: 'JobsManagerController'
        })
        .state('spark.saved', {
            url: '/saved',
            templateUrl: '/static/app/spark/partials/saved.html',
            controller: 'SparkSavedController'
        })
        .state('spark.history', {
            url: '/history',
            templateUrl: '/static/app/spark/partials/history.html',
            controller: 'SparkHistoryController'
        })
        .state('spark.results', {
            url: '/results/:handle/:jobName',
            templateUrl: '/static/app/spark/partials/results.html',
            controller: 'SparkResultsController'
        })

}])

/*
 * Flint Factory
 */

quarry.factory("Flint", ['$http', '$timeout', "$q", function($http, $timeout, $q) {

    return {

        setCluster: function(cluster) {
            this.cluster = cluster
        },

        getHistory: function(offset, count) {
            return $http.get("/api/histories/spark", {
                params: {
                    count: count,
                    offset: offset
                }
            })
        },

        savedJobs: function(offset, count) {
            return $http.get("/api/flint/jobs/saved", {
                params: {
                    offset: offset,
                    count: count
                }
            })
        },

        savedJob: function(jobId) {
            return $http.get("/api/flint/job/"+jobId)
        },

        updateJob: function(jobId, body) {
            return $http.post("/api/flint/job/"+jobId+"/update", {
                code: body
            })
        },

        deleteJob: function(jobId) {
            return $http.delete("/api/flint/job/"+jobId+"/delete")
        },

        saveJob: function(title, description, body) {
            return $http.post("/api/flint/jobs/save", {
                title: title,
                description: description,
                code: body
            })
        },

        createRawDataset: function(name) {
            return $http.post('/api/flint/rawdataset/create', {
                name: name
            })
        },

        getRawDatasets: function() {
            return $http.get("/api/flint/rawdatasets")
        },

        getRawDataset: function(name) {
            return $http.get("/api/flint/rawdataset/"+name)
        },

        uploadFileIntoDataset: function(name, file) {
            var q = $q.defer()

            $http.get("/api/flint/rawdataset/"+name+"/uploadurl")
                .success(function(res) {

                    var formData = new FormData(),
                        params = res.params

                    formData.append("policy", params.policy)
                    formData.append("signature", params.signature)
                    formData.append("key", params.key)
                    formData.append("AWSAccessKeyId", params.AWSAccessKeyId)
                    formData.append("acl", params.acl)
                    formData.append("success_action_status", params.success_action_status)

                    formData.append("file", file)

                    $http.post(res.url, formData, {
                        transformRequest: angular.identity,
                        headers: {'Content-Type': undefined}
                    })
                        .success(function(res) {
                            q.resolve(res)
                        })
                        .error(function(res) {
                            q.reject(res)
                        })
                })
                .error(function(res) {
                    q.reject(res)
                })

            var promise = q.promise

            promise.success = function(fn) {
                promise.then(fn)
                return promise
            }

            promise.error = function(fn) {
                promise.then(null, fn)
                return promise
            }

            return promise 
        },

        getStatus: function() {
            return $http.get("/api/flint/spark/status", {
                params: {
                    cluster: this.cluster
                }
            })
        },

        getProgress: function(jobType) {
            return $http.get("/api/flint/spark/progress", {
                params: {
                    cluster: this.cluster,
                    jobType: jobType
                }
            })
        },

        downloadResults: function(handle) {
            return $http.get("/api/flint/spark/job/async/results", {
                params: {
                    handle: handle,
                    download: 1
                }
            })
        },

        getResults: function(handle) {
            return $http.get("/api/flint/spark/job/async/results", {
                params: {
                    handle: handle,
                    cluster: this.cluster
                }
            })
        },

        cancelJob: function(jobId) {
            return $http.post("/api/flint/spark/job/async/cancel", {
                handle: jobId,
                cluster: this.cluster
            })
        },

        execute: function(body, options, progressCallback, dataCallback, errorCallback) {

            var self = this
            var jobName = Date.now()

            $http.post("/api/flint/spark/jobs/upload", {
                 file: body,
                 name: jobName,
                 cluster: self.cluster
            })
                .success(function(res) {
                    $http.post("/api/flint/spark/job/"+jobName+"/run", {
                        options: JSON.stringify(options),
                        cluster: self.cluster
                    })
                        .success(function(res) {
                            self._watchHandle(res.handle, progressCallback, dataCallback, errorCallback)
                        })
                        .error(function(res) {
                            errorCallback(res)
                        })
                })
                .error(function(res) {
                    errorCallback(res)
                })

        },

        _watchHandle: function(handle, progressCallback, dataCallback, errorCallback) {
            
            var self = this

            $http.get("/api/flint/spark/job/async/progress", {
                params: {
                    handle: handle,
                    cluster: self.cluster
                }
            })
                .success(function(res) {
                    progressCallback(res)
                    if (!res.running) {
                        self._fetchData(handle, dataCallback, errorCallback)
                    } else {
                        $timeout(function() {
                            self._watchHandle(handle, progressCallback, dataCallback, errorCallback)
                        }, 1000)
                    }
                })
                .error(function(res) {
                    errorCallback(res)
                })

        },

        _fetchData: function(handle, dataCallback, errorCallback) {

            var self = this

            $http.get("/api/flint/spark/job/async/results", {
                params: {
                    handle: handle,
                    cluster: self.cluster
                }
            })
                .success(function(res) {
                    dataCallback(res)
                })
                .error(function(res) {
                    errorCallback(res.results)
                })

        }

    }

}])

/*
 * Flint Controllers
 */

quarry.controller('SparkEditorController', ["$scope", "$stateParams", "$modal", "Flint", "RedShirt", function($scope, $stateParams, $modal, Flint, RedShirt) {

    $scope.selectedCluster = function(clusterName) {

        $scope.cluster = clusterName
        Flint.setCluster($scope.cluster)

    }

    $scope.noClusters = function() {

    }

    if ($stateParams.jobId) {

        Flint.savedJob($stateParams.jobId)
            .success(function(res) {

                $scope.jobId = res.job.id
                $scope.python = res.job.code

            })
            .error(function(res) {

                $scope.error = res.error

            })

    }

    $scope.sparkEditorOptions = {
        lineWrapping : true,
        lineNumbers: true,
        mode: 'python'
    }

    $scope.showEditor = true
    $scope.priority = "default"

    if ($stateParams.jobId) {
   
        $scope.python = "Loading saved job..."

    } else {

        $scope.python = "" +                        
            "[ ]\n" +
            "# List your pip dependencies as a json-readable array\n" +
            "# For example, to include PyMySQL (version 0.6.1) and\n" +
            "# requests (version 2.2.1) your array would look like\n" +
            '# [ "PyMySQL==0.6.1", "requests==2.2.1" ]\n' +
            "\n" +
            "# You can import dependencies using standard imports e.g.\n" +
            "# import requests\n" +
            "# import PyMySQL\n" +
            "\n" +
            "\n" +
            "def run(sc, options):\n" +
            "    \"\"\"\n" +
            "    Runs a custom spark job and other python code\n" +
            "    \n" +
            "    Params:\n" +
            "        sc: a SparkContext\n" +
            "        options: a ditonary of user defined options\n" +
            "    Returns:\n" +
            "        a python object that will be json-serialized and delivered\n" +
            "            over http\n" +
            "    \"\"\"\n" +
            "\n" +
            "    # Returns the sum of all numbers in the given range\n" +
            "    return sc.parallelize(range(100)).reduce(lambda x, y: x + y)\n"

    }

    $scope.toggleEditor = function() {
        $scope.showEditor = !$scope.showEditor
    }

    $scope.testJob = function() {

        $scope.executeJob({
            'sparkler.test.rows': 1000
        })

    }

    $scope.executeJob = function(options) {

        options = options || {}
        options.jobName = $scope.jobName || "Untitled"
        options.priority = $scope.priority

        var body = $scope.python

        delete $scope.results
        delete $scope.error
        delete $scope.stageProgress
        delete $scope.taskProgress
        $scope.showEditor = false
        $scope.status = "Launching job..."

        Flint.execute(
            body,
            options,
            function(currentProgress) {
                if (currentProgress.running) {
                    if (Object.keys(currentProgress.progress).length && currentProgress.progress.currentStage) {
                        var progress = currentProgress.progress
                        $scope.status = '' + progress.currentStage.completeTasks + " of " + progress.currentStage.totalTasks + " tasks complete on stage " + 
                                        (progress.completeStages+1) + " of " + progress.totalStages + " total stages..."
                        $scope.stageProgress = progress.stageProgress * 97 + 3
                        $scope.taskProgress = progress.stageProgress * 97 + 3                        
                    } else {
                        $scope.status = "Running job..."
                    }
                } else {
                    $scope.status = "Job finished, fetching results..."
                }
            },
            function(res) {

                resultStr = JSON.stringify(res.results, null, 4)

                delete $scope.status
                $scope.results = resultStr

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
        delete $scope.results
        $scope.showEditor = true
    }

    /*
     * SAVE JOB MODAL
     */

    $scope.saveJob = function() {

        if ($scope.jobId) {

            Flint.updateJob($scope.jobId, $scope.python)
                .success(function(res) {

                })

        } else {

            var saveJobModal = $modal.open({
                templateUrl: 'static/app/spark/partials/saveJobModal.html',

                controller: ['$scope', function(modalScope) {
                    
                    modalScope.job = {}

                    modalScope.dismissSaveJobModal = function() {
                        saveJobModal.close()
                    }

                    modalScope.saveJob = function(title, description) {

                        modalScope.savingJob = true
                        delete modalScope.saveJobError

                        Flint.saveJob(title, description, $scope.python)
                            .success(function(res) {
                                // TODO pop up a notification that the save worked
                                saveJobModal.close()
                            })
                            .error(function(res) {
                                modalScope.saveJobError = res.error
                            })
                            .finally(function(res) {
                                modalScope.savingJob = false
                            })

                    }

                    modalScope.dismissSaveError = function() {
                        delete modalScope.saveJobError
                    }

                }]
            });

        }

    }

    $scope.dismissError = function() {
        delete $scope.error
    }

}])

quarry.controller("SparkSavedController", ["$scope", "Flint", function($scope, Flint) {

}])

quarry.controller("SparkHistoryController", ["$scope", "$state", function($scope, $state) {

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

quarry.controller("SparkResultsController", ["$scope", "$stateParams", "Flint", function($scope, $stateParams, Flint) {

    var handle = $stateParams.handle

    $scope.loadingResults = true
    $scope.jobName = $stateParams.jobName

    Flint.getResults(handle)
        .success(function(res) {
            $scope.results = JSON.stringify(res.results, undefined, 2)
        })
        .error(function(res) {
            $scope.error = res.error
        })
        .finally(function() {
            $scope.loadingResults = false
        })

    $scope.downloadResults = function() {

        Flint.downloadResults(handle)
            .success(function(res) {
                window.location.assign(res.url)
            })

    }

}])
