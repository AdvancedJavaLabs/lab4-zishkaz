package org.itmo

import java.util.stream.StreamSupport
import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer

class Sorter {

    class RecordHandler : Mapper<Any, Text, DoubleWritable, SalesRecord>() {

        private val record = SalesRecord()
        private val outKey = DoubleWritable()

        override fun map(key: Any, value: Text, context: Context) {
            val rec = value.toString().split(",").map { it.trim() }

            val category = rec[0]
            val revenue = rec[1].toDoubleOrNull() ?: return
            val quantity = rec[2].toLongOrNull() ?: return

            outKey.set(-1 * revenue)

            record.category.set(category)
            record.quantity.set(quantity)
            record.revenue.set(revenue)
            context.write(outKey, record)
        }
    }

    class RevenueReducer : Reducer<DoubleWritable, SalesRecord, Text, Text>() {
        override fun reduce(key: DoubleWritable, values: MutableIterable<SalesRecord>, context: Context) {
            StreamSupport.stream(values.spliterator(), true)
                .forEach { context.write(Text(it.category), Text(it.toString())) }
        }
    }
}
