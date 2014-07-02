quarry.directive('resultsManager', ["Shark", function(Shark) {

    var FETCH_COUNT = 100

    return {
        templateUrl: "/static/app/sql/partials/resultsManager.html",
        scope: {
            handle: '=handle',
        },
        link: function(scope, element, attrs) {

            scope.$watch('handle', function(oldValue, newValue) {

                delete scope.rows 
                delete scope.loadingResults
                delete scope.cursor
                delete scope.pageForward
                delete scope.files
                scope.offset = 0

                if (!scope.handle) {
                    return
                }

                scope.fetchResults()

            })

            scope.offset = 0

            scope.fetchResults = function() {

                if (scope.rows) {
                    scope.offset += 100
                }

                scope.loadingResults = true

                if (scope.cursor) {

                    scope.cursor.fetchN(FETCH_COUNT)
                        .success(function(res) {
                            scope.loadingResults = false
                            scope.rows = res.rows
                            scope.pageForward = (res.rows.length >= FETCH_COUNT)
                        })

                } else {

                    Shark.getResult(scope.handle)
                        .success(function(res) {

                            scope.columns = res.columns

                            Shark.getCursor(scope.handle, function(cursor) {
                                    
                                scope.cursor = cursor
                                scope.cursor.fetchN(FETCH_COUNT)
                                    .success(function(res) {
                                        scope.loadingResults = false
                                        scope.rows = res.rows
                                        scope.pageForward = (res.rows.length >= FETCH_COUNT)
                                    })

                            })

                        })
                        .error(function(res) {
                            scope.loadingResults = false
                            scope.error = res.error
                        })

                }

            }

            scope.getResultFiles = function() {

                Shark.getResultFiles(scope.handle)
                    .success(function(res) {
                        scope.files = res.files
                    })

            }

        }
    }

}])
