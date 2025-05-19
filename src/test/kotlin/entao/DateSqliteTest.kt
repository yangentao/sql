package entao

import io.github.yangentao.anno.ModelField
import io.github.yangentao.sql.TableMigrater
import io.github.yangentao.sql.TableModel
import io.github.yangentao.sql.TableModelClass
import io.github.yangentao.sql.pool.HarePool
import io.github.yangentao.sql.pool.LiteSources
import io.github.yangentao.sql.toJson
import io.github.yangentao.types.DateTime
import io.github.yangentao.xlog.logd
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import kotlin.test.Test

class ADate : TableModel() {
    @ModelField(primaryKey = true, autoInc = 1)
    var id: Int by model

    @ModelField
    var localDateTime: LocalDateTime? by model

    @ModelField
    var localDate: LocalDate? by model

    @ModelField
    var localTime: LocalTime? by model

    @ModelField
    var sqlDate: java.sql.Date? by model

    @ModelField
    var sqlTime: java.sql.Time? by model

    @ModelField
    var stamp: java.sql.Timestamp? by model

    override fun toString(): String {
        return this.toJson().toString()
    }

    companion object : TableModelClass<ADate>()
}

class DateTest {

    @Test
    fun date() {
        HarePool.pushSource(LiteSources.sqliteMemory())
        val c = HarePool.pick()
        TableMigrater(c, ADate::class)

        val now = DateTime()
        val a = ADate()
        a.sqlTime = now.time
        a.sqlDate = now.dateSQL
        a.stamp = now.timestamp
        a.localDate = now.localDate
        a.localTime = now.localTime
        a.localDateTime = now.localDateTime
        a.insert()
        ADate.dumpTable()
        // id=1, ldt=2025-05-19T16:30:20.529, ld=2025-05-19, lt=16:30:20.529, dt=1747643420529, tm=1747643420529, stamp=1747643420529,

        val b = ADate.oneByKey(1) ?: return
        for (e in b.model.entries) {
            logd(e.key, e.value, e.value!!::class.simpleName)
        }
        logd(b.localTime)
        println(b.toString())

    }

}