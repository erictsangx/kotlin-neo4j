package github.etx.neo4j

import org.neo4j.driver.v1.Record
import org.neo4j.driver.v1.StatementResult

class CursorWrapper(private val record: Record, private val sr: StatementResult) : Sequence<CursorWrapper> {

    private class CursorIterator(val sr: StatementResult) : Iterator<CursorWrapper> {
        override fun next(): CursorWrapper {
            return CursorWrapper(sr.next(), sr)
        }

        override fun hasNext(): Boolean {
            return sr.hasNext()
        }
    }

    override fun iterator(): Iterator<CursorWrapper> {
        return CursorIterator(sr)
    }

    fun unwrap(key: String): Cursor {
        return Cursor(record[key])
    }

}
