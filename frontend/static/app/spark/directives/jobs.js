quarry.directive('jobs', ['Flint', function(Flint) {

    var fullTemplateUrl = 'static/app/spark/partials/jobs-full.html',
        pickerTemplateUrl = 'static/app/spark/partials/jobs-picker.html'

    return {
        scope: {
            onSelect: '&onSelect',
        },
        template: '<div ng-include="contentUrl"></div>',
        link: function(scope, element, attrs) {

            scope.offset = 0
            scope.stepSize = 20
            scope.jobs = []

            scope.fetchJobs = function(offsetChange) {

                scope.offset += offsetChange

                scope.loadingJobs = true
                scope.pageForward = false
                scope.pageBackward = (scope.offset != 0)

                if (attrs.template == 'picker') {
                    scope.contentUrl = pickerTemplateUrl
                    scope.onSelect = scope.onSelect()
                } else {
                    scope.contentUrl = fullTemplateUrl
                }

                Flint.savedJobs(scope.offset, scope.stepSize)
                    .success(function(res) {
                        if (!scope.jobs || res.jobs.length > 0) {
                            scope.jobs = res.jobs
                            if (scope.jobs.length == scope.stepSize) {
                                scope.pageForward = true
                            }
                        }
                    })
                    .error(function(res) {
                        scope.error = res.error
                    })
                    .finally(function() {
                        scope.loadingJobs = false
                    })

            }
            scope.fetchJobs(0)

            scope.deleteJob = function(jobId) {
                scope.jobs = scope.jobs.filter(function(job) {
                    return job.id != jobId
                })
                Flint.deleteJob(jobId)
            }

            scope.pickJob = function(job) {
                scope.onSelect(job)
            }

        }
    }

}])
