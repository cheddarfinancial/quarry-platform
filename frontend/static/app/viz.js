var d3Module = angular.module('d3', [])
    .factory('d3', [function() {
        return d3
    }])
    .factory('nv', [function() {
        return nv
    }])


d3Module
    .directive('d3BarChart', ['nv', '$window', function(nv, $window) {

        return {
            restrict: 'EA',
            scope: {},
            link: function(scope, element, attrs) {

                var svg = d3.select(element[0]).append('svg')

                nv.addGraph(function() {

                    var chart = nv.models.multiBarChart()
                        .transitionDuration(350)
                        .reduceXTicks(true)   //If 'false', every single x-axis tick label will be rendered.
                        .rotateLabels(0)      //Angle to rotate x-axis labels.
                        .showControls(true)   //Allow user to switch between 'Grouped' and 'Stacked' mode.
                        .groupSpacing(0.1)    //Distance between each group of bars.

                        chart.yAxis
                            .tickFormat(d3.format(',.1f'))

                        svg
                            .datum([
                                    {
                                        key: "Test 1",
                                        values: [
                                            {
                                                x: 'A',
                                                y: 1
                                            },
                                            {
                                                x: 'B',
                                                y: 1
                                            },
                                            {
                                                x: 'C',
                                                y: 1
                                            }
                                        ]
                                    },
                                    {
                                        key: "Test 2",
                                        values: [
                                            {
                                                x: 'A',
                                                y: 2
                                            },
                                            {
                                                x: 'B',
                                                y: 2
                                            },
                                            {
                                                x: 'C',
                                                y: 2
                                            }
                                        ]
                                    },
                                    {
                                        key: "Test 3",
                                        values: [
                                            {
                                                x: 'A',
                                                y: 3
                                            },
                                            {
                                                x: 'B',
                                                y: 3
                                            },
                                            {
                                                x: 'C',
                                                y: 3
                                            }
                                        ]
                                    },
                             ])
                            .call(chart)

                    nv.utils.windowResize(chart.update)

                    return chart
                })

            }    
        }

    }])
