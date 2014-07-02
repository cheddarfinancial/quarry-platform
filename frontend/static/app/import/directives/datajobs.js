quarry.directive('datajobs', ['Jaunt', function(Jaunt) {

    var fullTemplateUrl = 'static/app/import/partials/datajobs-full.html',
        pickerTemplateUrl = 'static/app/import/partials/datajobs-picker.html'

    return {
        scope: {
            onSelect: '&onSelect',
        },
        template: '<div ng-include="contentUrl"></div>',
        link: function(scope, element, attrs) {

            scope.offset = 0
            scope.stepSize = 20
            scope.datajobs = []
            scope.jobType = attrs.jobType || "import"

            scope.fetchDatajobs = function(offsetChange) {

                scope.offset += offsetChange

                scope.loadingDatajobs = true
                scope.pageForward = false
                scope.pageBackward = (scope.offset != 0)

                if (attrs.template == 'picker') {
                    scope.contentUrl = pickerTemplateUrl
                    scope.onSelect = scope.onSelect()
                } else {
                    scope.contentUrl = fullTemplateUrl
                }
                
                var request
                if (scope.jobType == "import") {
                    request = Jaunt.getSavedImports(scope.offset, scope.stepSize)
                } else if (scope.jobType == "export") {
                    request = Jaunt.getSavedExports(scope.offset, scope.stepSize)
                } else {
                    throw "Unknown job type "+scope.jobType
                }
            
                request
                    .success(function(res) {
                        if (!scope.datajobs || res.datajobs.length > 0) {
                            scope.datajobs = res.datajobs
                            if (scope.datajobs.length == scope.stepSize) {
                                scope.pageForward = true
                            }
                        }
                    })
                    .error(function(res) {
                        scope.error = res.error
                    })
                    .finally(function() {
                        scope.loadingDatajobs = false
                    })

            }
            scope.fetchDatajobs(0)

            scope.deleteDatajob = function(datajobId) {
                scope.datajobs = scope.datajobs.filter(function(datajob) {
                    return datajob.id != datajobId
                })
                Jaunt.deleteDatajob(datajobId)
            }

            scope.pickDatajob = function(datajob) {
                scope.onSelect(datajob)
            }

        }
    }

}])
