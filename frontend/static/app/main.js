var quarry = angular.module('quarry', ['ui.router', 'ui.codemirror', 'ui.bootstrap', 'd3',
                                       'ngCookies'])

/*
 * CONFIG
 */

// http config
quarry.config(['$httpProvider', function($httpProvider) {

    // set up post requests to use standard encoding instead of json
    $httpProvider.defaults.headers.post['Content-Type'] = 'application/x-www-form-urlencoded;charset=utf-8'

    var param = function(obj) {
        var query = '', name, value, fullSubName, subName, subValue, innerObj, i;
      
        for (name in obj) {

            value = obj[name];
        
            if(value instanceof Array) {

                for(i=0; i<value.length; ++i) {
                    subValue = value[i];
                    fullSubName = name + '[' + i + ']';
                    innerObj = {};
                    innerObj[fullSubName] = subValue;
                    query += param(innerObj) + '&';
                }

            } else if(value instanceof Object) {
        
                for(subName in value) {
                    subValue = value[subName];
                    fullSubName = name + '[' + subName + ']';
                    innerObj = {};
                    innerObj[fullSubName] = subValue;
                    query += param(innerObj) + '&';
                }

            } else if(typeof(value) !== 'undefined' && value !== null) {

                query += encodeURIComponent(name) + '=' + encodeURIComponent(value) + '&';

            }

        }
      
            return query.length ? query.substr(0, query.length - 1) : query;
    }
 
    // override $http service's default transformRequest
    $httpProvider.defaults.transformRequest = [function(data) {
        return angular.isObject(data) && String(data) !== '[object File]' ? param(data) : data
    }]

}])

// route config
quarry.config(['$stateProvider', '$urlRouterProvider', '$httpProvider', function($stateProvider, $urlRouterProvider, $httpProvider) {

    // make sure we have a user before allowing access to the rest of the app
    if (!window.user) {

        // for any unmatched url, redirect to /
        $urlRouterProvider.otherwise("/auth/login")

        $stateProvider
            .state('auth', {
                url: '/auth',
                templateUrl: '/static/app/partials/auth/base.html'
            })
            .state('auth.login', {
                url: '/login',
                templateUrl: '/static/app/partials/auth/login.html',
                controller: 'LoginController'
            })
            .state('auth.signup', {
                url: '/signup',
                templateUrl: '/static/app/partials/auth/signup.html',
                controller: 'SignupController'
            })

        return
    }

    // for any unmatched url, redirect to /
    $urlRouterProvider.otherwise("/")

    // Set up them states
    $stateProvider
    
        // Index States

        .state('index', {
            url: '/',
            templateUrl: '/static/app/partials/index.html',
            controller: 'IndexController'
        })
        .state('profile', {
            url: '/profile',
            templateUrl: '/static/app/partials/profile.html',
            controller: 'ProfileController'
        })

}])

/*
 * Notifications Factory
 */

quarry.factory("Notifications", ['$http', '$timeout', function($http, $timeout) {

    var listeners = []
    var id = 0

    return {

        local: function(notification) {
            notification.id = "_"+id
            id += 1
            listeners.forEach(function(callback) {
                callback(notification)
            })
            return notification.id
        },

        global: function() {

        },

        listen: function(callback) {
            listeners.push(callback)
        }

    }

}])

/*
 * Controllers
 */

quarry.controller("IndexController", ["$scope", "$http", function($scope, $http) {

}])

/*
 * Auth Controllers
 */

quarry.controller('LoginController', ["$scope", "$http", function($scope, $http) {

    $scope.email = ""
    $scope.password = ""
    $scope.keep = false

    $scope.dismissError = function() {
        delete $scope.error
    }

    $scope.signin = function(email, password, keep) {

        $scope.email = email
        $scope.password = password
        $scope.keep = keep

        delete $scope.error

        if (!email || !password) {
            $scope.error = "You must enter both an email and a password to sign in"
            return
        }

        $scope.signingIn = true

        $http.post("/api/login", {
            email: email,
            password: password,
            keep: keep
        })
            .success(function(res) {
                window.location = window.nextUrl || '/'
            })
            .error(function(res) {
                $scope.signingIn = false
                $scope.error = res.error
            })

    }

}])

quarry.controller('SignupController', ["$scope", "$http", function($scope, $http) {

    $scope.name = ""
    $scope.email = ""
    $scope.password = ""
    $scope.inviteCode = ""
    $scope.organization = ""

    $scope.dismissError = function() {
        delete $scope.error
    }

    $scope.signup = function(name, email, password, inviteCode, organization) {
        
        $scope.name = name 
        $scope.email = email
        $scope.password = password
        $scope.inviteCode = inviteCode
        $scope.organization = organization

        delete $scope.error

        if (!name || !email || !password || !inviteCode) {
            $scope.error = "You must enter a username, an email, a password, and an invite code to sign up"
            return
        }

        $scope.signingUp = true

        $http.post("/api/signup", {
            name: name,
            email: email,
            password: password,
            inviteCode: inviteCode,
            organization: organization
        })
            .success(function(res) {
                window.location = window.nextUrl || '/'
            })
            .error(function(res) {
                $scope.signingUp = false
                $scope.error = res.error
            })

    }

}])

quarry.controller('NotificationsController', ["$scope", "$sce", "$http", "$timeout", "Notifications", function($scope, $sce, $http, $timeout, Notifications) {

    $scope.notifications = []    

    $scope.dismissNotification = function(id) {
        $scope.notifications = $scope.notifications.filter(function(notification) {
            return notification.id != id
        })
        if (id.charAt(0) == "_") {
            // local notifications
            return
        }
        $http.post("/api/notification/" + id + "/read")
    }

    var getNotifications = function() {
        $http.get("/api/notifications")
            .success(function(res) {
                res.notifications.forEach(function(notification) {
                    notification.message = $sce.trustAsHtml(notification.message)
                })
                $scope.notifications = $scope.notifications.concat(res.notifications)
            })
            .finally(function() {
                $timeout(getNotifications, 5000)
            })
    }
    getNotifications()

    Notifications.listen(function(notification) {
        var notification = {
            id: notification.id,
            message: $sce.trustAsHtml(notification.message)
        }
        $scope.notifications.push(notification)
    })

}])

quarry.controller('ProfileController', ["$scope", "$http", function($scope, $http) {

    $scope.accountStatus = "Loading account..."

    $http.get("/api/account/storage")
        .success(function(res) {
            $scope.storage = '' + res.storageUsed
        })
        .error(function(res) {
            $scope.storage = "N/A"
        })

    $http.get("/api/account")
        .success(function(res) {

            $scope.account = res.account

            $scope.updateAccount = function() {

                delete $scope.accountError
                $scope.accountStatus = "Saving account..."

                $http.post("/api/account/update", {
                    organization: $scope.account.organization,
                })
                    .error(function() {
                        $scope.accountError = "Something went wrong!"
                    })
                    .finally(function() {
                        delete $scope.accountStatus
                    })

            }

        })
        .finally(function() {
            delete $scope.accountStatus
        })

    $scope.dismissError = function() {
        delete $scope.accountError
    }


    $scope.userStatus = "Loading profile..."

    $http.get("/api/user/me")
        .success(function(res) {

            $scope.user = res.user

            $scope.updateUser = function() {

                delete $scope.userError
                $scope.userStatus = "Saving profile..."

                $http.post("/api/user/me/update", {
                    name: $scope.user.name,
                    phone_number: $scope.user.phone_number,
                    email: $scope.user.email
                })
                    .error(function() {
                        $scope.userError = "Something went wrong!"
                    })
                    .finally(function() {
                        delete $scope.userStatus
                    })

            }

        })
        .finally(function() {
            delete $scope.userStatus
        })

    $scope.dismissUserError = function() {
        delete $scope.userError
    }

}])
