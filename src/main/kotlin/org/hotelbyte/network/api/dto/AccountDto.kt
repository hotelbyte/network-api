package org.hotelbyte.network.api.dto

import java.math.BigInteger

/**
 * Account data class
 */
data class AccountDto(val address: String,
                      var amount: String,
                      val blocksMined: ArrayList<BigInteger>,
                      val transactions: ArrayList<TransactionDto>,
                      var tag:String?,
                      var type:String,
                      var firstSeen:BigInteger)