package org.hotelbyte.network.api.dto

import java.math.BigInteger

data class BlockDto(val number:BigInteger, val miner:String, val timestamp:BigInteger)