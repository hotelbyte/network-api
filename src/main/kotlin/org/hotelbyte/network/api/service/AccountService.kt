package org.hotelbyte.network.api.service

import com.google.gson.Gson
import io.vertx.core.http.HttpServerResponse
import org.hotelbyte.network.api.dto.AccountDto
import org.hotelbyte.network.api.dto.TransactionDto
import org.hotelbyte.network.api.params.Pages
import org.web3j.protocol.Web3j
import org.web3j.protocol.core.DefaultBlockParameter
import org.web3j.protocol.core.DefaultBlockParameterName
import org.web3j.protocol.core.methods.response.EthBlock
import org.web3j.protocol.core.methods.response.TransactionReceipt
import java.math.BigInteger
import java.time.Duration
import java.time.LocalDateTime
import kotlin.collections.ArrayList

/**
 * This service is used to keep updated the accounts of the network
 */
open class AccountService(web3: Web3j, redisClient: RedisClientService) {
    private var logger = io.vertx.core.logging.LoggerFactory.getLogger(this.javaClass)

    private val web3Client = web3
    private val _redisClient = redisClient

    init {
        _redisClient.connect()
    }

    /**
     * Keeps mined blocks
     */
    fun updateFromBlock(block: EthBlock.Block) {
        updateAccountInfo(block.miner, block.number, null, "account", block.timestamp)
    }

    /**
     * Keeps sender and receiver transactions
     */
    fun updateFromTransaction(account: String,
                              hash: String,
                              sender: Boolean,
                              transactionReceipt: TransactionReceipt?,
                              firstSeen: BigInteger) {
        val transaction = TransactionDto(hash, sender)
        updateAccountInfo(account, null, transaction, "account", firstSeen)
        if (transactionReceipt != null) {
            if (transactionReceipt.contractAddress != null) {
                // Save this address like a contract address
                updateAccountInfo(transactionReceipt.contractAddress, null, transaction, "contract", firstSeen)
            }
        }
    }

    /**
     * Async task to find all the accounts of the network
     * TODO create method to check the last sync avoid full sync again
     */
    fun findAllAccountsFromBlocks() {

        val now = LocalDateTime.now()

        // Get last block number
        val lastBlock = web3Client.ethBlockNumber().sendAsync().get()

        // Find all the transactions
        web3Client.replayTransactionsObservable(DefaultBlockParameter.valueOf(BigInteger.valueOf(0)),
                DefaultBlockParameter.valueOf(lastBlock.blockNumber)).subscribe({

            val txReceiptResult = web3Client.ethGetTransactionReceipt(it.hash).sendAsync().get()
            var transactionReceipt: TransactionReceipt? = null
            if (txReceiptResult != null && txReceiptResult.transactionReceipt != null) {
                transactionReceipt = txReceiptResult.transactionReceipt.get()
            }

            val timestampBlock = web3Client.ethGetBlockByHash(it.blockHash, false).sendAsync().get().block.timestamp
            // pass receipt only with the sender of the transaction
            updateFromTransaction(it.from, it.hash, true, transactionReceipt, timestampBlock)
            updateFromTransaction(it.to, it.hash, false, null, timestampBlock)

            logTotalTime(now, it.blockNumber, lastBlock.blockNumber, "from transactions")
        })

        // Find all the blocks
        web3Client.replayBlocksObservable(DefaultBlockParameter.valueOf(BigInteger.valueOf(0)),
                DefaultBlockParameter.valueOf(lastBlock.blockNumber), false).subscribe({
            updateFromBlock(it.block)

            // Check if is the last block
            logTotalTime(now, it.block.number, lastBlock.blockNumber, "from blocks")
        })

        logger.info("Job account finder started")
    }

    /**
     * Find account page from Redis
     */
    fun findAccountPage(pages: Pages, response: HttpServerResponse) {
        _redisClient.redis?.zrange("accounts:sorted", pages.from, pages.to, { asyncResult ->
            if (!asyncResult.succeeded()) {
                logger.error(asyncResult.cause())
            }
            if (asyncResult.result() != null) {
                response.end(Gson().toJson(asyncResult.result()))
            } else {
                response.end("[]")
            }
        })
    }

    /**
     * Counts the sorted list
     */
    fun getTotal(response: HttpServerResponse) {
        _redisClient.redis?.zcount("accounts:sorted", Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, { asyncResult ->
            if (!asyncResult.succeeded()) {
                logger.error(asyncResult.cause())
            }

            if (asyncResult.result() != null) {
                response.end(Gson().toJson(asyncResult.result()))
            } else {
                response.end("[]")
            }
        })
    }

    /**
     * Get a single account with details
     */
    fun getAccount(account: String, response: HttpServerResponse) {
        _redisClient.redis?.hget("accounts:detailed", account, { asyncResult ->
            if (!asyncResult.succeeded()) {
                logger.error(asyncResult.cause())
            }

            if (asyncResult.result() != null) {
                response.end(asyncResult.result())
            } else {
                response.end("[]")
            }
        })
    }

    /**
     * Updates the account with the last balance, transactions and blocks mined.
     */
    private fun updateAccountInfo(account: String,
                                  blockNumber: BigInteger?,
                                  transaction: TransactionDto?,
                                  type: String,
                                  firstSeen: BigInteger) {

        // First get current balance
        val ethGetBalance = web3Client.ethGetBalance(account, DefaultBlockParameterName.LATEST).sendAsync().get()

        val gson = Gson()

        // Find accountDto, add block mined and update balance
        _redisClient.redis?.hget("accounts:detailed", account, { record ->
            if (!record.succeeded()) {
                logger.error(record.cause())
            }

            val accountDto: AccountDto?
            if (record.result() != null) {
                accountDto = gson.fromJson(record.result(), AccountDto::class.java)
                if (blockNumber != null && !accountDto.blocksMined.contains(blockNumber)) {
                    accountDto.blocksMined.add(blockNumber)
                }
                if (transaction != null && !accountDto.transactions.contains(transaction)) {
                    accountDto.transactions.add(transaction)
                }
                accountDto.amount = ethGetBalance.balance.toString()
            } else {
                val blocks = ArrayList<BigInteger>()
                if (blockNumber != null) {
                    blocks.add(blockNumber)
                }
                val transactions = ArrayList<TransactionDto>()
                if (transaction != null) {
                    transactions.add(transaction)
                }
                accountDto = AccountDto(account,
                        ethGetBalance.balance.toString(),
                        blocks,
                        transactions,
                        null,
                        type,
                        firstSeen)

                // Add to page only the first time
                // TODO get criteria for the score
                _redisClient.redis?.zadd("accounts:sorted", 1.toDouble(), accountDto.address, { zaddResult ->
                    if (!zaddResult.succeeded()) {
                        logger.error(zaddResult.cause())
                    }
                })
            }

            // Save account object
            _redisClient.redis?.hset("accounts:detailed", account, gson.toJson(accountDto), { hsetResult ->
                if (!hsetResult.succeeded()) {
                    logger.error(hsetResult.cause())
                }
            })
        })
    }

    private fun logTotalTime(startTime: LocalDateTime, blockNumber: BigInteger, lastBlockNumber: BigInteger, msg: String) {
        if (blockNumber.compareTo(lastBlockNumber) == 0) {
            val total = Duration.between(startTime, LocalDateTime.now())
            logger.info("Account finder arrive to the last block, great! Total time: $total $msg")
        }
    }
}