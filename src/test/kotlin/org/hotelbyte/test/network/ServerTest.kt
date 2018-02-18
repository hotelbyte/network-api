package org.hotelbyte.test.network

import io.vertx.ext.unit.TestSuite
import io.vertx.kotlin.ext.unit.TestOptions
import io.vertx.kotlin.ext.unit.report.ReportOptions
import org.junit.Test

/**
 * Sample test TODO please implement some test!!!!
 */
class ServerTest {

    var suite = TestSuite.create("the_test_suite")

    @Test
    fun s() {
        suite.test("my_test_case", { context ->
            var s = "value"
            context.assertEquals("value", s)
        })
        suite.run(TestOptions(
                reporters = listOf(ReportOptions(
                        to = "console"))))
    }

}