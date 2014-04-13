package com.mycompany.myproject

import org.vertx.groovy.core.http.HttpServer
import org.vertx.groovy.core.http.HttpServerRequest
import org.vertx.groovy.core.http.RouteMatcher
import org.vertx.groovy.platform.Verticle
import org.vertx.java.core.json.impl.Json

class RestApiVerticle extends Verticle {

    def start() {

        HttpServer server = vertx.createHttpServer()

        server.requestHandler(buildRoutes())

        server.listen(9090)
    }


    private Closure buildRoutes() {
        RouteMatcher routeMatcher = new RouteMatcher()

        routeMatcher.options('/') { HttpServerRequest request ->

            def mapOfOptions = [
                    'GET#/states': 'List of available states',
                    'GET#/:state/cities': 'List of cities belonging to a state',
                    'GET#/:state/:city': 'City details : population and coordinates'
            ]

            String encodedMapOfOptions = Json.encode(mapOfOptions)

            request.response.putHeader('Content-Type', 'application/json')
            request.response.end(encodedMapOfOptions)
        }


        routeMatcher.get('/states', this.&getStates)
        routeMatcher.get('/:state/cities', this.&getCitiesOfState)
        routeMatcher.get('/:state/:city', this.&getCityOfState)

        routeMatcher.asClosure()
    }


    def getStates(HttpServerRequest request) {

        request.pause()

        vertx.eventBus.send('mongo.persistor', [
                action: 'command',
                command: "{ distinct:'zips', key: 'state' }"
        ]) { resp ->
            request.response.end(Json.encode(resp.body.result.values))
        }
    }

    def getCitiesOfState(HttpServerRequest request) {

        request.pause()

        vertx.eventBus.send('mongo.persistor', [
                action: 'find',
                collection: 'zips',
                matcher: [
                        state: request.params.state
                ],
                keys: [
                        city: 1,
                        pop: 1,
                        _id: 0
                ]
        ]) { resp ->
            request.response.end(Json.encode(resp.body.results))
        }
    }

    def getCityOfState(HttpServerRequest request) {

        request.pause()

        vertx.eventBus.send('mongo.persistor', [
                action: 'findone',
                collection: 'zips',
                matcher: [
                        state: request.params.state,
                        city:request.params.city
                ],
                keys: [
                        city: 1,
                        pop: 1,
                        loc: 1,
                        state: 1,
                        _id: 0
                ]
        ]) { resp ->
            request.response.end(Json.encode(resp.body.result))
        }
    }
}
