package github.etx.neo4j

import org.neo4j.driver.v1.Logger
import org.neo4j.driver.v1.Logging

class NeoLogging(private val logger: org.slf4j.Logger) : Logging {

    override fun getLog(name: String): Logger {
        return neoLogger
    }

    private val neoLogger = object : Logger {
        override fun warn(message: String?, cause: Throwable?) {
            logger.warn(message, cause)
        }

        override fun trace(message: String?, vararg params: Any?) {
            logger.trace(message, params)
        }

        override fun debug(message: String?, vararg params: Any?) {
            logger.debug(message, *params)
        }

        override fun info(message: String?, vararg params: Any?) {
            logger.info(message, *params)
        }

        override fun warn(message: String?, vararg params: Any?) {
            logger.warn(message, *params)
        }

        override fun error(message: String?, cause: Throwable?) {
            logger.error(message, cause)
        }

        override fun isTraceEnabled(): Boolean {
            return logger.isTraceEnabled
        }

        override fun isDebugEnabled(): Boolean {
            return logger.isDebugEnabled
        }
    }
}