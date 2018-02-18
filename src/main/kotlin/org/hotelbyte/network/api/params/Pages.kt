package org.hotelbyte.network.api.params

/**
 * Pages to keep to and from values
 */
data class Pages(val from:Long, val to:Long) {

    /**
     * Page handler to help the page calculation from QueryParam
     */
    object Handler {

        /**
         * Calculates from and to
         */
        fun pagination(queryParameters:Map<String,List<String>>):Pages {

            val pagNumber = getPageParam(queryParameters["page"])
            val pageSize = getPageParam(queryParameters["size"])

            // Calculate page values
            val to = pagNumber * pageSize
            val from = to.minus(pageSize)

            return Pages(from,to.dec())
        }

        private fun getPageParam(queryParam:List<String>?):Long {
            var number = 0L
            if (queryParam != null) {
                number = queryParam[0].toLong()
            }
            return number
        }
    }
}