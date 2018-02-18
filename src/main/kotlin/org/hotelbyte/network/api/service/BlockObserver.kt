package org.hotelbyte.network.api.service

import org.web3j.protocol.Web3j

/**
 * Block observer, get the updates from the network
 */
class BlockObserver(web3: Web3j, accountService: AccountService) {

    private val logger = io.vertx.core.logging.LoggerFactory.getLogger(this.javaClass)

    init {
        web3.blockObservable(true).subscribe({x ->
            accountService.updateFromBlock(x.block)
        })

        logger.info("Block observer started")
    }
}