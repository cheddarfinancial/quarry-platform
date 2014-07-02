/*
 * Workflow Configuration
 */

quarry.config(['$stateProvider', function($stateProvider) {

    // make sure we have a user before allowing access to the rest of the app
    if (!window.user) {
        return
    }

    // Set up them states
    $stateProvider

        //
        // Workflow States
        //

        .state('workflow', {
            url: '/workflow',
            templateUrl: '/static/app/workflow/partials/base.html',
        })
        .state('workflow.editor', {
            url: '/editor?workflowId',
            templateUrl: '/static/app/workflow/partials/editor.html',
            controller: 'WorkflowEditorController'
        })
        .state('workflow.saved', {
            url: '/saved',
            templateUrl: '/static/app/workflow/partials/saved.html',
            controller: 'WorkflowSavedController'
        })
        .state('workflow.history', {
            url: '/history',
            templateUrl: '/static/app/workflow/partials/history.html',
            controller: 'WorkflowHistoryController'
        })
        .state('workflow.running', {
            url: '/running',
            templateUrl: '/static/app/workflow/partials/running.html',
            controller: 'WorkflowRunningController'
        })

}])

/*
 * Lego Factory
 */

quarry.factory("Lego", ['$http', '$timeout',  function($http, $timeout) {

    return {

        getWorkflows: function(offset, count) {
            return $http.get("/api/lego/workflows", {
                params: {
                    offset: offset,
                    count: count
                }
            })
        },

        getWorkflow: function(id) {
            return $http.get("/api/lego/workflow/"+id)
        },

        cancelWorkflow: function(handle) {
            return $http.post("/api/lego/workflow/cancel", {
                handle: handle
            })
        },

        _stepExclude: ['query', 'job', 'datajob'],

        _sanitizedSteps: function(steps) {
            var self = this
            steps = JSON.parse(JSON.stringify(steps))
            steps.forEach(function(step) {
                delete step.$$hashKey
                for (var key in step) {
                    if (key.charAt(0) == '_') {
                        delete step[key]
                    }
                }
                self._stepExclude.forEach(function(prop) {
                    delete step[prop]
                })

                // clean up sql step based on it's queryType
                if (step.type == "sql") {
                    if (step.queryType == "saved") {
                        delete step.sql
                    } else if (step.queryType == "ad-hoc") {
                        delete step.queryId
                    }
                }

            })
            return steps
        },

        _notifyUsers: function(notifyType, notifyUsers) {
            if (notifyType == "none") {
                return []
            } else if (notifyType == "all") {
                return [-1]
            } else if (notifyType == "custom") {
                return notifyUsers
            }
        },

        _cluster: function(cluster) {
            var cluster = JSON.parse(JSON.stringify(cluster))
            if (cluster.action == "pick") {
                cluster.name = cluster.pickedName
            }
            delete cluster.pickedName
        },

        createWorkflow: function(workflow) {
            var steps = this._sanitizedSteps(workflow.steps)
            var notifyUsers = this._notifyUsers(workflow.notify, workflow.notifyUsers)
            var cluster = this._cluster(workflow.cluster)
            return $http.post('/api/lego/workflow/new', {
                title: workflow.title,
                description: workflow.description,
                steps: JSON.stringify(steps),
                cluster: JSON.stringify(workflow.cluster),
                notify_users: JSON.stringify(notifyUsers)
            })
        },

        editWorkflow: function(id, workflow) {
            var steps = this._sanitizedSteps(workflow.steps)
            var notifyUsers = this._notifyUsers(workflow.notify, workflow.notifyUsers)
            return $http.post('/api/lego/workflow/'+id+'/edit', {
                title: workflow.title,
                description: workflow.description,
                steps: JSON.stringify(steps),
                cluster: JSON.stringify(workflow.cluster),
                notify_users: JSON.stringify(notifyUsers)
            })
        },

        deleteWorkflow: function(id) {
            return $http.post('/api/lego/workflow/'+id+'/delete')
        },

        executeHeadless: function(workflowId) {
            return $http.post("/api/lego/workflow/"+workflowId+"/run")
        },

        execute: function(workflowId, progressCallback, completionCallback, errorCallback) {

            var self = this

            $http.post("/api/lego/workflow/"+workflowId+"/run")
                .success(function(res) {
                    self._watchHandle(res.handle, progressCallback, completionCallback, errorCallback)
                })
                .error(function(res) {
                    errorCallback(res)
                })

        },

        _watchHandle: function(handle, progressCallback, completionCallback, errorCallback) {

            var self = this

            $http.get("/api/lego/workflow/handle_info", {
                params: {
                    handle: handle
                }
            })
                .success(function(res) {
                    progressCallback(res)
                    if (res.finished) {
                        completionCallback(res)
                    } else {
                        $timeout(function() {
                            self._watchHandle(handle, progressCallback, completionCallback, errorCallback)
                        }, 1000)
                    }
                })
                .error(function(res) {
                    errorCallback(res)
                })

        },

        getRunningWorkflows: function() {
            return $http.get("/api/lego/workflows/running")
        }

    }

}])

/*
 * Workflow Controllers
 */

quarry.controller('WorkflowEditorController', ["$scope", "$stateParams", "Lego", function($scope, $stateParams, Lego) {

    $scope.sqlEditorOptions = {
        lineWrapping : true,
        lineNumbers: true,
        mode: 'sql'
    }

    $scope.thisUser = user
    $scope.workflow = {
        cluster: {
            action: 'start',
            workers: 1,
            name: "",
        },
        notifyUsers: [],
        notify: "none",
        steps: []
    }

    $scope.selectedCluster = function(name) {
        $scope.workflow.cluster.pickedName = name
    }

    $scope.stepTypes = {
        sql: {
            description: "Run a SQL query",
        },
        python: {
            description: "Run a Python Spark job",
        },
        import: {
            description: "Run a Data Import job",
        },
        export: {
            description: "Run a Data Export job",
        }
    }

    var _id = 0
    $scope.addNewStep = function() {
        $scope.workflow.steps.push({
            _id: _id,
            type: "sql",
            queryType: "saved",
            sql: "",
            editorLoaded: function(editor) {
                this.editor = editor
            }
        })
        _id += 1
    } 

    if ($stateParams.workflowId) {

        $scope.workflowId = $stateParams.workflowId
        $scope.loadingWorkflow = true

        Lego.getWorkflow($scope.workflowId)
            .success(function(res) {
                res.workflow.steps.forEach(function(step) {
                    step._id = _id
                    _id += 1
                })
                $scope.workflow = res.workflow
                if ($scope.workflow.notify_users.length == 0) {
                    $scope.workflow.notify = "none"
                } else if ($scope.workflow.notify_users[0] == -1) {
                    $scope.workflow.notify = "all"
                } else {
                    $scope.workflow.notify = "custom"
                }
            })
            .finally(function() {
                $scope.loadingWorkflow = false
            })

    }

    $scope.removeStep = function(stepId) {
        $scope.workflow.steps = $scope.workflow.steps.filter(function(elem) {
            return elem._id != stepId
        })
    }

    $scope.stepTypeChanged = function(step) {
        delete step.id
        delete step.queryType
        if (step.type == 'sql') {
            step.queryType = "saved"
        }
    }

    $scope.queryTypeChanged = function(step) {
        console.log(this.editor)
    }

    $scope.saveWorkflow = function(workflow) {

        $scope.savingWorkflow = true
        delete $scope.errors

        var request

        if ($scope.workflowId) {
            request = Lego.editWorkflow($scope.workflowId, workflow)
        } else {
            request = Lego.createWorkflow(workflow)
        }

        request
            .success(function(res) {
                $scope.workflowId = res.workflow.id
            })
            .error(function(res) {
                $scope.errors = res.errors
            })
            .finally(function() {
                $scope.savingWorkflow = true
            })
    }

    $scope.dismissError = function(section, index) {

        if (index) {
            delete $scope.errors[section][index]
        } else {
            delete $scope.errors[section]
        }

    }

    $scope.pickQueryFn = function(step) {
        return function(query) {
            step.id = query.id
            step.query = query
            delete step._picking
        }
    }

    $scope.pickJobFn = function(step) {
        return function(job) {
            step.id = job.id
            step.job = job
            delete step._picking
        }
    }

    $scope.pickDatajobFn = function(step) {
        return function(datajob) {
            step.id = datajob.id
            step.datajob = datajob
            delete step._picking
        }
    }

}])

quarry.controller("WorkflowRunningController", ["$scope", "$timeout", "Lego", function($scope, $timeout, Lego) {

    var workflowsTimer

    $scope.$on("destroy", function() {
        $timeout.cancel(workflowsTimer)
    })

    var getRunningWorkflows = function() {

        Lego.getRunningWorkflows()
            .success(function(res) {
                $scope.workflows = res.workflows
            })
            .finally(function() {
                $timeout(getRunningWorkflows, 5000)
            })

    }
    getRunningWorkflows()

    $scope.cancelWorkflow = function(workflow) {
        $scope.workflows = $scope.workflows.filter(function(_workflow) {
            return _workflow.handle != workflow.handle
        })
        Lego.cancelWorkflow(workflow.handle)
    }

}])

quarry.controller("WorkflowSavedController", ["$scope", "$state", "Lego", function($scope, $state, Lego) {

    $scope.loadingWorkflows = true

    $scope.offset = 0
    $scope.count = 20

    Lego.getWorkflows($scope.offset, $scope.count)
        .success(function(res) {
            $scope.workflows = res.workflows
        })
        .finally(function() {
            $scope.loadingWorkflows = false 
        })

    $scope.deleteWorkflow = function(workflowId) {
        Lego.deleteWorkflow(workflowId)
        $scope.workflows = $scope.workflows.filter(function(workflow) {
            return workflow.id != workflowId
        })
    }

    $scope.execute = function(workflow) {
        workflow.starting = true
        Lego.executeHeadless(workflow.id)
            .success(function() {
                $state.go('workflow.running')
            })
            .finally(function() {
                workflow.starting = false  
            })
    }

}])

quarry.controller("WorkflowHistoryController", ["$scope", "$state", function($scope, $state) {

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

