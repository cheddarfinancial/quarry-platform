// route config
quarry.config(['$stateProvider', function($stateProvider) {

    // make sure we have a user before allowing access to the rest of the app
    if (!window.user) {
        return
    }

    // Set up them states
    $stateProvider
    
        //
        // Job States
        //

        .state('jobs', {
            url: '/jobs',
            templateUrl: '/static/app/jobs/partials/base.html',
        })
        .state('jobs.manager', {
            url: '/manager',
            templateUrl: '/static/app/jobs/partials/manager.html',
            controller: 'JobsManagerController'
        })
        .state('jobs.inspect', {
            url: '/inspect/:clusterId/:jobId',
            templateUrl: '/static/app/jobs/partials/inspect.html',
            controller: 'JobsInspectorController'
        })

}])

/*
 * Job Controllers
 */

quarry.controller('JobsManagerController', ["$scope", "$modal", "$timeout", "$stateParams", "Flint", function($scope, $modal, $timeout, $stateParams, Flint) {

    var jobType = $stateParams.jobType,
        progressPromise = null

    $scope.selectedCluster = function(clusterName) {

        $timeout.cancel(progressPromise)

        $scope.cluster = clusterName

        Flint.setCluster($scope.cluster)

        updateStatus()

    }


    $scope.noClusters = function() {

        var modal = $modal.open({
            templateUrl: 'static/app/partials/spark/launchClusterModal.html',
            controller: ['$scope', function(modalScope) {
                modalScope.dismiss = function() {
                    modal.dismiss()
                }
            }],
            backdrop: 'static'
        })

    }

    $scope.status = "Loading cluster status..."

    var updateStatus = function() {

        Flint.getProgress(jobType)
            .success(function(res) {
                $scope.progress = res.progress
            })
            .error(function(res) {
                $scope.error = res.error
            })
            .finally(function() {
                delete $scope.status
            })

        progressPromise = $timeout(updateStatus, 5000)
 
    }

    $scope.cancelJob = function(jobId) {
        Flint.cancelJob(jobId)
        delete $scope.progress.jobs[jobId]
    }

    $scope.dismissError = function() {
        delete $scope.error
    }

}])
