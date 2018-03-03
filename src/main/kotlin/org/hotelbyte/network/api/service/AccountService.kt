package org.hotelbyte.network.api.service

import com.google.common.cache.Cache
import com.google.gson.Gson
import io.vertx.core.http.HttpServerResponse
import org.hotelbyte.network.api.dto.AccountDto
import org.hotelbyte.network.api.dto.TransactionDto
import org.hotelbyte.network.api.params.Pages
import org.litote.kmongo.async.findOne
import org.litote.kmongo.async.getCollection
import org.litote.kmongo.async.json
import org.litote.kmongo.async.updateOneById
import org.litote.kmongo.findOne
import org.litote.kmongo.getCollection
import org.web3j.protocol.Web3j
import org.web3j.protocol.core.DefaultBlockParameter
import org.web3j.protocol.core.DefaultBlockParameterName
import org.web3j.protocol.core.methods.response.EthBlock
import org.web3j.protocol.core.methods.response.TransactionReceipt
import java.math.BigInteger
import java.time.Duration
import java.time.LocalDateTime
import kotlin.collections.ArrayList
import com.google.common.cache.CacheBuilder


/**
 * This service is used to keep updated the accounts of the network
 */
open class AccountService(web3: Web3j) {
    private var logger = io.vertx.core.logging.LoggerFactory.getLogger(this.javaClass)

    private val web3Client = web3

    data class Account(val _id: String, val detail: AccountDto)

    private val asyncClient = org.litote.kmongo.async.KMongo.createClient()
    private val asyncCollection = asyncClient.getDatabase("network").getCollection<Account>()
    private val syncClient = org.litote.kmongo.KMongo.createClient()
    private val syncCollection = syncClient.getDatabase("network").getCollection<Account>()

    private val accountCache: Cache<String, AccountDto> = CacheBuilder.newBuilder().maximumSize(100).build<String, AccountDto>()
    private val balanceCache: Cache<String, BigInteger> = CacheBuilder.newBuilder().maximumSize(100).build<String, BigInteger>()


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
        web3Client.catchUpToLatestTransactionObservable(DefaultBlockParameter.valueOf(BigInteger.valueOf(0))).subscribe({
            try {
                val txReceiptResult = web3Client.ethGetTransactionReceipt(it.hash).sendAsync().get()
                var transactionReceipt: TransactionReceipt? = null
                if (txReceiptResult != null && txReceiptResult.transactionReceipt != null) {
                    transactionReceipt = txReceiptResult.transactionReceipt.get()
                }

                val timestampBlock = web3Client.ethGetBlockByHash(it.blockHash, false).sendAsync().get().block.timestamp
                // pass receipt only with the sender of the transaction
                updateFromTransaction(it.from, it.hash, true, transactionReceipt, timestampBlock)
                if (it.to != null) {
                    updateFromTransaction(it.to, it.hash, false, null, timestampBlock)
                }
                logTotalTime(now, it.blockNumber, lastBlock.blockNumber, "from transactions")
            } catch (e: Throwable) {
                logger.error("Tx error: ", e)
            }
        })

        // Find all the blocks
        web3Client.catchUpToLatestBlockObservable(DefaultBlockParameter.valueOf(BigInteger.valueOf(0)), false).subscribe({
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
        var details: ArrayList<AccountDto> = arrayListOf()
        asyncCollection.find().skip(pages.skips).limit(pages.limit).forEach({
            details.add(it.detail)
        }, { _, t ->
            if (t != null) {
                logger.error("Error getting count", t)
            }
            response.end(Gson().toJson(details))
        })
    }

    /**
     * Counts the sorted list
     */
    fun getTotal(response: HttpServerResponse) {
        asyncCollection.count({ result, t ->
            if (t != null) {
                logger.error("Error getting count", t)
            }

            if (result != null) {
                response.end(Gson().toJson(result))
            } else {
                response.end("[]")
            }
        })
    }

    /**
     * Get a single account with details
     */
    fun getAccount(account: String, response: HttpServerResponse) {
        asyncCollection.findOne("{_id:${account.json}}", { result, t ->
            if (t != null) {
                logger.error("Error getting one", t)
            }
            if (result != null) {
                response.end(Gson().toJson(result))
            } else {
                response.end("[]")
            }
        })
    }


    /**
     * Updates the account with the last balanceCache, transactions and blocks mined.
     */
    private fun updateAccountInfo(account: String,
                                  blockNumber: BigInteger?,
                                  transaction: TransactionDto?,
                                  type: String,
                                  firstSeen: BigInteger) {
        // Find accountDto, add block mined and update balanceCache
        val accountBalance = balanceCache.get(account, {
            web3Client.ethGetBalance(account, DefaultBlockParameterName.LATEST).sendAsync().get().balance
        })
        synchronized(this, {
            var accountDto: AccountDto?
            accountDto = accountCache.get(account, {
                getAccountDto(account)
            })
            if (accountDto != null) {
                // We received a contract or already is a contract
                if ((type == "contract" && accountDto.type == "account") || (type == "account" && accountDto.type == "contract")) {
                    logger.warn("collision with account $account")
                }
                if (blockNumber != null) {
                    accountDto.blocksMined.add(blockNumber)
                }
                if (transaction != null) {
                    accountDto.transactions.add(transaction)
                }
                accountDto.amount = accountBalance.toString()
                asyncCollection.updateOneById(accountDto.address, Account(accountDto.address, accountDto), { result, t ->
                    if (t != null) {
                        logger.error("Error update account", t);
                    }
                })

            } else {
                val blocks = LinkedHashSet<BigInteger>()
                if (blockNumber != null) {
                    blocks.add(blockNumber)
                }
                val transactions = LinkedHashSet<TransactionDto>()
                if (transaction != null) {
                    transactions.add(transaction)
                }
                accountDto = AccountDto(account,
                        accountBalance.toString(),
                        blocks,
                        transactions,
                        null,
                        type,
                        firstSeen)
                // Add to page only the first time
                asyncCollection.insertOne(Account(accountDto.address, accountDto), { result, t ->
                    if (t != null) {
                        logger.error("Error new account", t);
                    }
                })
            }
            accountCache.put(account, accountDto)
        })
    }

    private fun getAccountDto(account: String): AccountDto? {
        val dbAccount = syncCollection.findOne("{_id:${account.json}}")
        if (dbAccount != null) {
            return dbAccount.detail
        } else {
            return null
        }
    }

    private fun logTotalTime(startTime: LocalDateTime, blockNumber: BigInteger, lastBlockNumber: BigInteger, msg: String) {
        if (blockNumber.toInt() % 10000 == 0) {
            val total = Duration.between(startTime, LocalDateTime.now())
            logger.info("Partial elapsed $blockNumber/$lastBlockNumber! Total time: $total $msg")
        }
        if (blockNumber.compareTo(lastBlockNumber) == 0) {
            val total = Duration.between(startTime, LocalDateTime.now())
            logger.info("Account finder arrive to the last block, great! Total time: $total $msg")
        }
    }
}