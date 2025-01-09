package org.itmo

import java.io.DataInput
import java.io.DataOutput
import java.util.*
import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.WritableComparable

class SalesRecord(
    val category: Text = Text(),
    val revenue: DoubleWritable = DoubleWritable(),
    val quantity: LongWritable = LongWritable()
) : WritableComparable<SalesRecord> {

    override fun write(dest: DataOutput) {
        category.write(dest)
        revenue.write(dest)
        quantity.write(dest)
    }

    override fun readFields(src: DataInput) {
        category.readFields(src)
        revenue.readFields(src)
        quantity.readFields(src)
    }

    override fun compareTo(other: SalesRecord): Int {
        return other.revenue.compareTo(revenue)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is SalesRecord) return false
        return category == other.category &&
                revenue == other.revenue &&
                quantity == other.quantity
    }

    override fun hashCode(): Int {
        return Objects.hash(category, revenue, quantity)
    }

    override fun toString(): String {
        return "$category\t${revenue.get()}\t$quantity"
    }
}