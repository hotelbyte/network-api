package org.hotelbyte.network.api.service

import org.web3j.protocol.Web3j
import org.web3j.protocol.core.methods.response.TransactionReceipt

/**
 * Transaction observer keep updated the last transaction
 */
class TransactionObserver(web3: Web3j, accountService: AccountService) {

    private var logger = io.vertx.core.logging.LoggerFactory.getLogger(this.javaClass)

    init {
        web3.transactionObservable().subscribe({x ->
            val txReceiptResult = web3.ethGetTransactionReceipt(x.hash).sendAsync().get()
            var transactionReceipt: TransactionReceipt? = null
            if (txReceiptResult != null && txReceiptResult.transactionReceipt != null) {
                transactionReceipt = txReceiptResult.transactionReceipt.get()
            }
            val timestampBlock = web3.ethGetBlockByHash(x.blockHash, false).sendAsync().get().block.timestamp
            accountService.updateFromTransaction(x.from, x.hash, true, transactionReceipt, timestampBlock)
            accountService.updateFromTransaction(x.to, x.hash, false, null, timestampBlock)
        })

        logger.info("Transaction observer started")
    }
}