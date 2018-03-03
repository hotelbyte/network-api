package org.hotelbyte.network.api.service

import com.google.gson.Gson
import com.mongodb.client.DistinctIterable
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
import java.lang.Exception
import java.math.BigInteger
import java.time.Duration
import java.time.LocalDateTime
import kotlin.collections.ArrayList

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


    private var pendingCreation: HashSet<String> = hashSetOf()

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
        var details: ArrayList<AccountDto> = arrayListOf()
        asyncCollection.find().skip(pages.skips).limit(pages.limit).forEach({
            details.add(it.detail)
        }, { _, t ->
            if (t != null) {
                logger.error("Error getting count", t);
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
                logger.error("Error getting count", t);
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
                logger.error("Error getting one", t);
            }

            if (result != null) {
                response.end(Gson().toJson(result))
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
        synchronized(this, {
            var dbAccount: Account?
            if (pendingCreation.contains(account)) {
                val startTime = LocalDateTime.now()
                do {
                    dbAccount = syncCollection.findOne("{_id:${account.json}}")
                } while (dbAccount == null)
                val total = Duration.between(startTime, LocalDateTime.now())
                if (logger.isDebugEnabled) {
                    logger.debug("Blocked waiting for $account! Total time: $total")
                }
                pendingCreation.remove(account)
            } else {
                dbAccount = syncCollection.findOne("{_id:${account.json}}")
            }

            // Find accountDto, add block mined and update balance
            val ethGetBalance = web3Client.ethGetBalance(account, DefaultBlockParameterName.LATEST).sendAsync().get()
            val accountDto: AccountDto?
            if (dbAccount != null) {
                accountDto = dbAccount.detail
                // We received a contract or already is a contract
                if ((type == "contract" && accountDto.type == "account") || (type == "account" && accountDto.type == "contract")) {
                    logger.warn("collision with account $account")
                }
                if (blockNumber != null && !accountDto.blocksMined.contains(blockNumber)) {
                    accountDto.blocksMined.add(blockNumber)
                }
                if (transaction != null && !accountDto.transactions.contains(transaction)) {
                    accountDto.transactions.add(transaction)
                }
                accountDto.amount = ethGetBalance.balance.toString()
                asyncCollection.updateOneById(accountDto.address, Account(accountDto.address, accountDto), { result, t ->
                    if (t != null) {
                        logger.error("Error update account", t);
                    }
                    if (logger.isDebugEnabled) {
                        logger.debug("Updated account ${accountDto.address}: $result")
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
                        ethGetBalance.balance.toString(),
                        blocks,
                        transactions,
                        null,
                        type,
                        firstSeen)
                // Add to page only the first time
                pendingCreation.add(accountDto.address)
                asyncCollection.insertOne(Account(accountDto.address, accountDto), { result, t ->
                    if (t != null) {
                        logger.error("Error new account", t);
                    }
                    if (logger.isDebugEnabled) {
                        logger.debug("New account ${accountDto.address}: $result")
                    }
                })
            }
        })
    }

    private fun logTotalTime(startTime: LocalDateTime, blockNumber: BigInteger, lastBlockNumber: BigInteger, msg: String) {
        if (blockNumber.toInt() % 10000 == 0) {
            val total = Duration.between(startTime, LocalDateTime.now())
            logger.info("Partial elapsed $blockNumber! Total time: $total $msg")
        }
        if (blockNumber.compareTo(lastBlockNumber) == 0) {
            val total = Duration.between(startTime, LocalDateTime.now())
            logger.info("Account finder arrive to the last block, great! Total time: $total $msg")
        }
    }
}