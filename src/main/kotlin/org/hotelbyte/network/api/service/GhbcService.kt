package org.hotelbyte.network.api.service

import org.web3j.protocol.Web3j
import org.web3j.protocol.http.HttpService

/**
 * Wrapper for Hotelbyte network
 * Thanks to Web3j team! nice work.
 */
open class GhbcService() {

    // Singleton service
    companion object Web3 {
        // Always localhost
        private val web3 = Web3j.build(HttpService("http://localhost:30199"))
        fun get(): Web3j {
            return web3
        }
    }
}