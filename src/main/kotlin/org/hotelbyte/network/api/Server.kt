package org.hotelbyte.network.api

import io.vertx.core.http.HttpMethod
import io.vertx.core.http.HttpServerRequest
import io.vertx.core.http.HttpServerResponse
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import nl.komponents.kovenant.task
import org.apache.http.client.utils.URLEncodedUtils
import org.hotelbyte.network.api.params.Pages
import org.hotelbyte.network.api.service.*
import java.net.URI
import java.time.LocalDateTime

/**
 * Main class
 */
class Server : io.vertx.core.AbstractVerticle()  {

    private var logger = io.vertx.core.logging.LoggerFactory.getLogger(this.javaClass)

    companion object Routes {
        const val account       = "/accounts/:account"
        const val accounts      = "/accounts"
        const val totalAccounts = "/accounts/total"
    }

    /**
     * Run HTTP server
     */
    override fun start() {

        // Create server and routes
        val server = vertx.createHttpServer()
        val router = Router.router(vertx)
        val totalAccountsRoute = router.route(HttpMethod.GET, Server.Routes.totalAccounts).produces("application/json")
        val accountRoute = router.route(HttpMethod.GET, Server.Routes.account).produces("application/json")
        val accountsRoute = router.route(HttpMethod.GET, Server.Routes.accounts).produces("application/json")

        // TODO handle with configuration for dev environments
        // accountsRoute.handler(CorsHandler.create("explorer\\.hotelbyte\\.org").allowedMethod(HttpMethod.GET))

        // Prepare async services
        logger.info("Server started at ${LocalDateTime.now()}")
        val web3Service = GhbcService.Web3.get()
        val accountService = AccountService(web3Service, RedisClientService(vertx))

        // Fire the observers and account finder
        val sync = System.getProperty("sync")
        if (sync != null) {
            task { accountService.findAllAccountsFromBlocks() }
        }

        BlockObserver(web3Service, accountService)
        TransactionObserver(web3Service, accountService)

        // Total accounts
        totalAccountsRoute.handler({ routingContext ->
            val response = routingContext.response()
            addResponseHeaders(routingContext, response)

            // Response total of accounts
            accountService.getTotal(response)
        })

        // Single account
        accountRoute.handler({ routingContext ->
            val response = routingContext.response()
            val request = routingContext.request()
            val account = request.getParam("account")
            addResponseHeaders(routingContext, response)
            if (account != null) {
                accountService.getAccount(account, response)
            }
        })

        // This handler will be called for accounts request
        accountsRoute.handler({ routingContext ->
            val response = routingContext.response()
            addResponseHeaders(routingContext, response)

            val pages = Pages.Handler.pagination(routingContext.request().queryParameters())
            if (logger.isDebugEnabled) logger.debug("range to ${pages.to} from ${pages.from}")
            // Response end with page
            accountService.findAccountPage(pages, response)
        })

        server.requestHandler({ router.accept(it) }).listen(8086)
    }

    /**
     * Override query parameters
     */
    fun HttpServerRequest.queryParameters() : Map<String, List<String>> {
        val params = URLEncodedUtils.parse(URI(this.uri()), "UTF-8")
        val map = mutableMapOf<String, MutableList<String>>()
        for(param in params){
            if(map[param.name] == null){
                map[param.name] = mutableListOf()
            }
            map[param.name]?.add(param.value)
        }
        return map
    }

    private fun addResponseHeaders(routingContext:RoutingContext, response: HttpServerResponse) {
        // Response always with JSON
        response.putHeader("content-type", routingContext.acceptableContentType)
        response.putHeader("Access-Control-Allow-Origin", "*")
    }
}