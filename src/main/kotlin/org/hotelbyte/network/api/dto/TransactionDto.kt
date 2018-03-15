package org.hotelbyte.network.api.dto

/**
 * Transaction data class
 */
data class TransactionDto(val hash: String, val sender:Boolean) {
    override fun equals(other: Any?): Boolean {
        return super.equals(other)
    }

    override fun hashCode(): Int {
        return super.hashCode()
    }
}