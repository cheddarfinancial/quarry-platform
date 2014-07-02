quarry.directive('queries', ['$http', 'Shark', function($http, Shark) {

    var fullTemplateUrl = 'static/app/sql/partials/queries-full.html',
        pickerTemplateUrl = 'static/app/sql/partials/queries-picker.html'

    return {
        scope: {
            onSelect: '&onSelect',
        },
        template: '<div ng-include="contentUrl"></div>',
        link: function(scope, element, attrs) {

            scope.offset = 0
            scope.stepSize = 20
            scope.queries = []

            scope.fetchQueries = function(offsetChange) {

                scope.offset += offsetChange

                scope.loadingQueries = true
                scope.pageForward = false
                scope.pageBackward = (scope.offset != 0)

                if (attrs.template == 'picker') {
                    scope.contentUrl = pickerTemplateUrl
                    scope.onSelect = scope.onSelect()
                } else {
                    scope.contentUrl = fullTemplateUrl
                }

                Shark.savedQueries(scope.offset, scope.stepSize)
                    .success(function(res) {
                        if (!scope.queries || res.queries.length > 0) {
                            scope.queries = res.queries
                            if (scope.queries.length == scope.stepSize) {
                                scope.pageForward = true
                            }
                        }
                    })
                    .error(function(res) {
                        scope.error = res.error
                    })
                    .finally(function() {
                        scope.loadingQueries = false
                    })

            }
            scope.fetchQueries(0)

            scope.deleteQuery = function(queryId) {
                scope.queries = scope.queries.filter(function(query) {
                    return query.id != queryId
                })
                Shark.deleteQuery(queryId)
            }

            scope.pickQuery = function(query) {
                scope.onSelect(query)
            }

        }
    }

}])
