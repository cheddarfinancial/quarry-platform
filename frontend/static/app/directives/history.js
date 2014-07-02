quarry.directive('history', ['$http', function($http) {

    return {
        scope: {
            type: '@type',
            name: '@name',
            actions: '='
        },
        templateUrl: 'static/app/partials/history.html',
        link: function(scope, element, attrs) {

            scope.eventMap = {
                "job_complete": "Job Finished",
                "job_start": "Job Started"
            }

            scope.offset = 0
            scope.stepSize = 20
            scope.histories = []

            scope.fetchHistory = function(offsetChange) {

                scope.offset += offsetChange

                scope.loadingHistory = true
                scope.pageForward = false
                scope.pageBackward = (scope.offset != 0)

                $http.get('/api/histories/'+scope.type, {
                    params: {
                        offset: scope.offset,
                        count: scope.stepSize
                    }
                })
                    .success(function(res) {
                        if (!scope.histories || res.histories.length > 0) {
                            scope.histories = res.histories
                            if (scope.histories.length == scope.stepSize) {
                                scope.pageForward = true
                            }
                        }
                    })
                    .error(function(res) {
                        scope.error = res.error
                    })
                    .finally(function() {
                        scope.loadingHistory = false
                    })

            }
            scope.fetchHistory(0)

        }
    }

}])
