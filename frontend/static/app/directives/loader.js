quarry.directive('loader', function() {
    return {
        templateUrl: "/static/app/partials/loader.html",
        scope: {
            message: '=message',
            taskProgress: '=taskProgress',
            stageProgress: '=stageProgress',
            enabled: '=enabled'
        }
    }
})
