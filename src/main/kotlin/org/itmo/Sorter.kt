package org.itmo

import java.util.stream.StreamSupport
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer

class Sorter {

    class RecordHandler : Mapper<Any, Text, SalesRecord, Text>() {

        private val record = SalesRecord()
        private val text = Text()

        override fun map(key: Any, value: Text, context: Context) {
            val rec = value.toString().split(",").map { it.trim() }

            val category = rec[0]
            val revenue = rec[1].toDoubleOrNull() ?: return
            val quantity = rec[2].toLongOrNull() ?: return

            record.category.set(category)
            record.quantity.set(quantity)
            record.revenue.set(revenue)
            context.write(record, text)
        }
    }

    class RevenueReducer : Reducer<SalesRecord, Text, SalesRecord, Text>() {
        override fun reduce(key: SalesRecord, values: MutableIterable<Text>, context: Context) {
            StreamSupport.stream(values.spliterator(), true).forEach { context.write(key, it) }
        }
    }
}
