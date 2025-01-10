package org.itmo

import com.google.common.util.concurrent.AtomicDouble
import java.util.concurrent.atomic.AtomicLong
import java.util.stream.StreamSupport
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer


class LineProcessor : Mapper<Any, Text, Text, DataRecord>() {

    private val categoryKey = Text()
    private val dataRecord = DataRecord()

    override fun map(key: Any, value: Text, context: Context) {
        val record = value.toString().split(",").map { it.trim() }

        if (record.size != 5 && record[0] == "transaction_id") return

        val category = record[2]
        val price = record[3].toDoubleOrNull() ?: return
        val quantity = record[4].toLongOrNull() ?: return

        categoryKey.set(category)

        dataRecord.apply {
            this.category.set(category)
            this.totalQuantity.set(quantity)
            this.totalRevenue.set(price * quantity)
        }

        println(dataRecord)

        context.write(categoryKey, dataRecord)
    }
}

class DataAggregator : Reducer<Text, DataRecord, Text, DataRecord>() {

    private val aggregated = DataRecord()

    override fun reduce(categoryKey: Text, records: Iterable<DataRecord>, context: Context) {
        val totalQuantity = AtomicLong(0)
        val totalRevenue = AtomicDouble(0.0)

        StreamSupport.stream(records.spliterator(), true).forEach { record ->
            totalQuantity.addAndGet(record.totalQuantity.get())
            totalRevenue.addAndGet(record.totalRevenue.get())
        }

        aggregated.apply {
            this.category.set(categoryKey)
            this.totalQuantity.set(totalQuantity.get())
            this.totalRevenue.set(totalRevenue.get())
        }

        context.write(categoryKey, aggregated)
    }
}
