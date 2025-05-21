package entao

import io.github.yangentao.anno.ModelField
import io.github.yangentao.sql.TableModel
import io.github.yangentao.sql.TableModelClass
import io.github.yangentao.sql.pool.HarePool
import io.github.yangentao.sql.pool.LiteSources
import io.github.yangentao.sql.toJson
import io.github.yangentao.xlog.logd
import kotlin.test.Test
import kotlin.test.assertEquals

class Ai : TableModel() {
    @ModelField(primaryKey = true, autoInc = 10)
    var id: Int by model

    @ModelField
    var name: String by model

    companion object : TableModelClass<Ai>()
}

class AutoIncTest {
    @Test
    fun aiTest() {
        HarePool.pushSource(LiteSources.sqliteMemory())
        val a = Ai()
        a.name = "entao"
        val r = a.insert()
        assertEquals(11, a.id)
//        logd("count: ", r.count, "keys:", r.returnkeys)
        val jo = a.toJson()
        logd(jo.toString())

        logd("dump:")
        Ai.dumpTable()
    }
}