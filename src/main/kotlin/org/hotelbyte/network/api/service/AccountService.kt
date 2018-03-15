package org.hotelbyte.network.api.service

import com.google.gson.Gson
import io.vertx.core.http.HttpServerResponse
import org.hotelbyte.network.api.dto.AccountDto
import org.hotelbyte.network.api.dto.ContractDto
import org.hotelbyte.network.api.dto.TransactionDto
import org.hotelbyte.network.api.params.Pages
import org.web3j.protocol.Web3j
import org.web3j.protocol.core.DefaultBlockParameter
import org.web3j.protocol.core.DefaultBlockParameterName
import org.web3j.protocol.core.methods.response.EthBlock
import org.web3j.protocol.core.methods.response.EthBlockNumber
import org.web3j.protocol.core.methods.response.TransactionReceipt
import java.lang.Exception
import java.math.BigInteger
import java.time.Duration
import java.time.LocalDateTime

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

    companion object Accounts {
        const val accountsSorted = "accounts:sorted"
        const val accountsDetailed = "accounts:detailed"
    }

    /**
     * Keeps mined blocks
     */
    fun updateFromBlock(block: EthBlock.Block) {
        updateAccountInfo(block.miner, block.number, null, null, "account", block.timestamp)
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
        // If we have receipt look for contract address
        if (transactionReceipt != null) {
            if (transactionReceipt.contractAddress != null) {
                // Save this address like a contract address and link to sender address
                val contractDto = ContractDto(transactionReceipt.contractAddress)
                updateAccountInfo(account, null, transaction, contractDto, "account", firstSeen)
                updateAccountInfo(transactionReceipt.contractAddress, null, transaction, null, "contract", firstSeen)
            } else {
                // We have a receipt OK but we don't have contract, save as is
                updateAccountInfo(account, null, transaction, null, "account", firstSeen)
            }
        } else {
            // No receipt we have an unconfirmed transaction save it anyway
            updateAccountInfo(account, null, transaction, null, "account", firstSeen)
        }
    }

    /**
     * Task to find all the accounts of the network
     * Async call
     */
    fun findAllAccountsFromBlocks() {
        val startBlock = BigInteger.valueOf(0)
        val endBlock = BigInteger.valueOf(10000)
        // Find max block for data look up
        val lastBlock = web3Client.ethBlockNumber().sendAsync().get()
        // Fire replay block finder
        findBlockByBatch(startBlock, endBlock, lastBlock)
        logger.info("Account finder started at $startBlock to $endBlock with max ${lastBlock.blockNumber}")
    }

    /**
     * TODO add Datadog?
     */
    private fun findBlockByBatch(startBlockNumber: BigInteger, endBlockNumber: BigInteger, lastBlock: EthBlockNumber) {
        val startTime = LocalDateTime.now()
        // Find all the blocks
        web3Client.replayBlocksObservable(DefaultBlockParameter.valueOf(startBlockNumber),
                DefaultBlockParameter.valueOf(endBlockNumber), true, true).subscribe({
            if (it != null && it.block != null) {
                try {
                    // Miner
                    updateFromBlock(it.block)

                    // Senders and receivers
                    if (it.block.transactions != null && it.block.transactions.size > 0) {
                        it.block.transactions.forEach({ tx ->
                            val txObject = tx.get()
                            if (txObject is EthBlock.TransactionObject) {
                                // Find receipt
                                val txReceiptResult = web3Client.ethGetTransactionReceipt(txObject.hash).sendAsync().get()
                                var transactionReceipt: TransactionReceipt? = null
                                if (txReceiptResult != null && txReceiptResult.transactionReceipt != null) {
                                    transactionReceipt = txReceiptResult.transactionReceipt.get()
                                }
                                // Get timestamp block
                                val timestampBlock = it.block.timestamp
                                // pass receipt only with the sender of the transaction
                                updateFromTransaction(txObject.from, txObject.hash, true, transactionReceipt, timestampBlock)
                                if (txObject.to != null) {
                                    updateFromTransaction(txObject.to, txObject.hash, false, null, timestampBlock)
                                }
                            }
                        })
                    }
                } catch (e: Exception) {
                    logger.error("Replay block ", e)
                }

                // Check if is the last block
                if (it.block.number.compareTo(endBlockNumber) == 0 && it.block.number.compareTo(lastBlock.blockNumber) == -1) {
                    val total = Duration.between(startTime, LocalDateTime.now())
                    val nextStart = endBlockNumber.plus(BigInteger.valueOf(1))
                    val nextEnd = endBlockNumber.plus(BigInteger.valueOf(10000))
                    logger.info("Spent $total starting at $nextStart to $nextEnd ")
                    findBlockByBatch(nextStart, nextEnd, lastBlock)
                }

                if (it.block.number.compareTo(lastBlock.blockNumber) == 0) {
                    logger.info("Full scan of blockchain done!")
                }
            } else {
                logger.error("Something is wrong block is null")
            }
        })
    }

    /**
     * Find account page from Redis
     */
    fun findAccountPage(pages: Pages, response: HttpServerResponse) {
        synchronized(this, {
            _redisClient.redis?.zrange(AccountService.Accounts.accountsSorted, pages.from, pages.to, { asyncResult ->
                if (!asyncResult.succeeded()) {
                    logger.error(asyncResult.cause())
                }
                if (asyncResult.result() != null) {
                    response.end(Gson().toJson(asyncResult.result()))
                } else {
                    response.end("{[]}")
                }
            })
        })
    }

    /**
     * Counts the sorted list
     */
    fun getTotal(response: HttpServerResponse) {
        synchronized(this, {
            _redisClient.redis?.zcount(AccountService.Accounts.accountsSorted, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, { asyncResult ->
                if (!asyncResult.succeeded()) {
                    logger.error(asyncResult.cause())
                }

                if (asyncResult.result() != null) {
                    response.end(Gson().toJson(asyncResult.result()))
                } else {
                    response.end("{[]}")
                }
            })
        })
    }

    /**
     * Get a single account with details
     */
    fun getAccount(account: String, response: HttpServerResponse) {
        synchronized(this, {
            _redisClient.redis?.hget(AccountService.Accounts.accountsDetailed, account, { asyncResult ->
                if (!asyncResult.succeeded()) {
                    logger.error(asyncResult.cause())
                }

                if (asyncResult.result() != null) {
                    response.end(Gson().toJson(asyncResult.result()))
                } else {
                    response.end("{[]}")
                }
            })
        })
    }

    /**
     * Updates the account with the last balance, transactions and blocks mined.
     */
    private fun updateAccountInfo(account: String,
                                  blockNumber: BigInteger?,
                                  transaction: TransactionDto?,
                                  contract: ContractDto?,
                                  type: String,
                                  firstSeen: BigInteger) {

        val redis = _redisClient.redis

        // Save account object in Redis
        synchronized(this, {
            // Find account on local hash map and create the first entry with zadd on Redis
            val accountDtoFromLocal = findOrCreateAccountDtoFromHashMap(account, type, blockNumber,
                    transaction, contract, firstSeen)

            redis?.transaction()?.hexists(Accounts.accountsDetailed, account, {
                if (it.succeeded()) {
                    if (it.result() == "1") {
                        redis.hget(Accounts.accountsDetailed, account, {
                            if (it.succeeded()) {
                                if (it.result() != null) {
                                    val accountDto = Gson().fromJson(it.result(), AccountDto::class.java)

                                    // Merge remote with local
                                    if (accountDtoFromLocal != null) {
                                        var update = false
                                        if (!accountDto.transactions.isEmpty()) {
                                            accountDto.transactions.forEach {
                                                if (!accountDtoFromLocal.transactions.contains(it)) {
                                                    accountDtoFromLocal.transactions.add(it)
                                                    update = true
                                                }
                                            }
                                        } else {
                                            // we don't have remote transactions check local transactions
                                            update = !accountDtoFromLocal.transactions.isEmpty()
                                        }
                                        if (!accountDto.blocksMined.isEmpty()) {
                                            accountDto.blocksMined.forEach {
                                                if (!accountDtoFromLocal.blocksMined.contains(it)) {
                                                    accountDtoFromLocal.blocksMined.add(it)
                                                    update = true
                                                }
                                            }
                                        } else {
                                            update = !accountDtoFromLocal.blocksMined.isEmpty()
                                        }
                                        if (accountDtoFromLocal.firstSeen.compareTo(accountDto.firstSeen) == 1) {
                                            accountDtoFromLocal.firstSeen = accountDto.firstSeen
                                            update = true
                                        }
                                        if (update) {
                                            hset(accountDtoFromLocal)
                                        }
                                    } else {
                                        logger.warn("Local account is null, it's impossible")
                                    }
                                } else {
                                    logger.warn("Exist OK, but hget NOT $account")
                                }
                            } else {
                                logger.error(it.cause())
                            }
                        })
                    } else {
                        // Add to page only the first time
                        // TODO get criteria for the score
                        _redisClient.redis?.zadd(Accounts.accountsSorted, 1.toDouble(), accountDtoFromLocal?.address, { zaddResult ->
                            if (!zaddResult.succeeded()) {
                                logger.error(zaddResult.cause())
                            }
                        })
                        hset(accountDtoFromLocal)
                    }
                } else {
                    logger.error(it.cause())
                }
            })
        })
    }

    private fun hset(accountDto: AccountDto?) {
        synchronized(this, {
            val json = Gson().toJson(accountDto)
            _redisClient.redis?.hset(Accounts.accountsDetailed, accountDto?.address, json, { hsetResult ->
                if (!hsetResult.succeeded()) {
                    logger.error(hsetResult.cause())
                }
            })
        })
    }

    /**
     * Find an account inside the local concurrent hash map
     */
    private fun findOrCreateAccountDtoFromHashMap(account: String, type: String, blockNumber: BigInteger?,
                                                  transaction: TransactionDto?, contract: ContractDto?, firstSeen: BigInteger): AccountDto? {
        var accountDto: AccountDto?
        val blocks = ArrayList<BigInteger>()
        if (blockNumber != null) {
            blocks.add(blockNumber)
        }
        val transactions = ArrayList<TransactionDto>()
        if (transaction != null) {
            transactions.add(transaction)
        }
        val contracts = ArrayList<ContractDto>()
        if (contract != null) {
            contracts.add(contract)
        }
        val ethGetBalance = web3Client.ethGetBalance(account, DefaultBlockParameterName.LATEST).sendAsync().get()
        accountDto = AccountDto(account,
                ethGetBalance.balance.toString(),
                blocks,
                transactions,
                contracts,
                null,
                type,
                firstSeen)

        return accountDto
    }
}