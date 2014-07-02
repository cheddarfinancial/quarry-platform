/*
 * Instances Configuration
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

        .state('instances', {
            url: '/instances',
            templateUrl: '/static/app/instances/partials/base.html',
        })

        //
        // Wizard States
        //

        .state('instances.overview', {
            url: '/overview',
            templateUrl: '/static/app/instances/partials/overview.html',
            controller: 'InstancesOverviewController'
        })
        .state('instances.wizard', {
            url: '/wizard',
            templateUrl: '/static/app/instances/partials/wizard.html',
            controller: 'InstancesWizardController'
        })

}])

/*
 * Jaunt Factory
 */

quarry.factory("RedShirt", ['$http', '$timeout', function($http, $timeout) {

    return {

        instances: function() {
            return $http.get("/api/redshirt/instances")
        },

        spots: function() {
            return $http.get("/api/redshirt/spots")
        },

        clusters: function() {
            return $http.get("/api/redshirt/clusters")
        },

        setClusterWorkers: function(cluster, workers) {
            return $http.post("/api/redshirt/cluster/" + cluster + "/workers/" + workers)
        },

        getStreamers: function() {
            return $http.get("/api/redshirt/streamers")
        },

        shutdownCluster: function(clusterName) {
            return $http.post("/api/redshirt/cluster/"+clusterName+"/shutdown")
        },

        stopCluster: function(clusterName) {
            return $http.post("/api/redshirt/cluster/"+clusterName+"/stop")
        },

        startCluster: function(clusterName) {
            return $http.post("/api/redshirt/cluster/"+clusterName+"/start")
        },

        rebootCluster: function(clusterName) {
            return $http.post("/api/redshirt/cluster/"+clusterName+"/reboot")
        },

        prices: function() {
            return $http.get("/api/redshirt/prices")
        },

        terminateInstance: function(id) {
            return $http.post("/api/redshirt/instance/"+id+"/terminate")
        },

        cancelSpotRequest: function(id) {
            return $http.post("/api/redshirt/spot/"+id+"/cancel")
        },

        launch: function(opts) {
            return $http.post("/api/redshirt/launch", opts)
        },

        launchCluster: function(opts) {
            return $http.post("/api/redshirt/launch/cluster", opts)
        }

    }

}])

/*
 * RedShirt Controllers
 */

quarry.controller('InstancesOverviewController', ["RedShirt", "$scope", "$timeout", function(RedShirt, $scope, $timeout) {

    var clusterTimer;

    $scope.$on('destroy', function() {
        $timeout.cancel(clusterTimer)
    })

    $scope.loadingClusters = true

    var loadClusters = function() {

        RedShirt.clusters()
            .success(function(res) {
                if (Object.keys(res.clusters).length) {
                    $scope.clusters = res.clusters
                }
            })
            .error(function(res) {

            })
            .finally(function() {
                $scope.loadingClusters = false

                // TODO need a global place for setting timer durations
                clusterTimer = $timeout(loadClusters, 5000)
            })

    }
    loadClusters()

    $scope.editingWorkers = {}
    $scope.alterWorkers = function(id, workers) {
        $scope.editingWorkers[id] = {
            "workers": workers,
            "altering": false
        }
    }

    $scope.updateWorkers = function(id) {

        $scope.editingWorkers[id].altering = true

        RedShirt.setClusterWorkers(id, $scope.editingWorkers[id].workers)
            .finally(function() {
                delete $scope.editingWorkers[id]
                $timeout.cancel(clusterTimer)
                loadClusters()
            })

    }

    $scope.cancelUpdateWorkers = function(id) {
        delete $scope.editingWorkers[id]
    }

    $scope.shutdownCluster = function(clusterName) {
        RedShirt.shutdownCluster(clusterName)
        delete $scope.clusters[clusterName]
    }

    $scope.rebootCluster = function(clusterName) {
        RedShirt.rebootCluster(clusterName)
            .finally(function() {
                $timeout.cancel(clusterTimer)
                loadClusters()
            })
    }

    $scope.stopCluster = function(clusterName) {
        RedShirt.stopCluster(clusterName)
            .finally(function() {
                $timeout.cancel(clusterTimer)
                loadClusters()
            })
    }

    $scope.startCluster = function(clusterName) {
        RedShirt.startCluster(clusterName)
            .finally(function() {
                $timeout.cancel(clusterTimer)
                loadClusters()
            })
    }

    $scope.terminateInstance = function(id) {
        RedShirt.terminateInstance(id)
    }

    $scope.cancelSpotRequest = function(id) {
        RedShirt.cancelSpotRequest(id)
    }

}])

quarry.controller('InstancesWizardController', ["RedShirt", "$scope", function(RedShirt, $scope) {
    
    $scope.workers = 1
    $scope.clusterName = ''
    $scope.spot = false

    $scope.dismissError = function() {
        delete $scope.error
    }

    $scope.launchCluster = function(clusterName, workers, spot) {

        delete $scope.error

        if (isNaN(parseInt(workers))) {
            $scope.error = "You must enter a valid number of workers to launch!"
            return
        }

        $scope.launching = true

        var opts = {
            clusterName: clusterName,
            workers: workers
        }

        if (spot) {
            opts.spot = true
        }

        RedShirt.launchCluster(opts)
            .success(function(res) {
                location.hash = "/instances/overview"
            })
            .error(function(res) {
                $scope.error = res.error
            })
            .finally(function() {
                $scope.launching = false
            })

   }

}])
