package org.hotelbyte.network.api.params

/**
 * Pages to keep to and from values
 */
data class Pages(val skips: Int, val limit: Int) {

    /**
     * Page handler to help the page calculation from QueryParam
     */
    object Handler {

        /**
         * Calculates from and to
         */
        fun pagination(queryParameters: Map<String, List<String>>): Pages {

            val pagNumber = getPageParam(queryParameters["page"])
            val pageSize = getPageParam(queryParameters["size"])

            // Calculate page values
            val skips = pageSize * (pagNumber - 1)
            return Pages(skips, pageSize)
        }

        private fun getPageParam(queryParam: List<String>?): Int {
            var number = 0
            if (queryParam != null) {
                number = queryParam[0].toInt()
            }
            return number
        }
    }
}