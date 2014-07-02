quarry.directive('clusterManager', ['RedShirt', '$cookieStore', '$timeout', function(RedShirt, $cookieStore, $timeout) {

    var inlineTemplateUrl = "/static/app/instances/partials/cluster-manager-inline.html",
        fullTemplateUrl = "/static/app/instances/partials/cluster-manager-full.html"

    return {
        scope: {
            onSelect: '&onSelect',
            onEmpty: '&onEmpty',
            hideWarning: '='
        },
        template: '<div ng-include="contentUrl"></div>',
        link: function(scope, element, attrs) {

            var clusterTimer

            scope.$on('destroy', function() {

                $timeout.cancel(clusterTimer)

            })

            // set up the template
            if (attrs.template == 'full') {
                scope.contentUrl = fullTemplateUrl
            } else {
                scope.contentUrl = inlineTemplateUrl
            }

            // unpack methods
            scope.onSelect = scope.onSelect()
            scope.onEmpty = scope.onEmpty()

            scope.status = 'Loading clusters...'
            scope.data = {}
            scope.selectedCluster = null

            var getClusters = function() {

                RedShirt.clusters()
                    .success(function(res) {

                        delete scope.status

                        if (Object.keys(res.clusters).length == 0) {

                            if (!scope.clusters || scope.clusters.length) {
                                scope.clusters = []
                                scope.onEmpty && scope.onEmpty()
                            }

                        } else {

                            var defaultCluster = $cookieStore.get('defaultCluster')

                            scope.clusters = res.clusters

                            var cluster = ""
                            if (scope.clusters[defaultCluster]) {
                                cluster = defaultCluster
                            } else {
                                cluster = scope.clusters[Object.keys(scope.clusters)[0]].name
                            }

                            scope.onSelectCluster(cluster)

                        }
                    })
                    .finally(function() {
                        clusterTimer = $timeout(getClusters, 5000)
                    })

            }
            getClusters()

            scope.onSelectCluster = function(cluster) {

                scope.selectedCluster = cluster
                scope.data.selectedCluster = cluster
                $cookieStore.put('defaultCluster', scope.selectedCluster)

                scope.aliveNow = scope.clusters[cluster].alive

                if (
                    (!scope.alivePrev && scope.aliveNow && scope.selectedPrev == scope.selectedCluster) ||
                    (scope.aliveNow && scope.selectedPrev != scope.selectedCluster)
                   ) {
                    scope.onSelect(scope.selectedCluster)
                } else if (!scope.aliveNow && scope.alivePrev) {
                    scope.onSelect()
                }

                scope.alivePrev = scope.clusters[cluster].alive
                scope.selectedPrev = cluster
            }

        },
    }
}])
