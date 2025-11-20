package entao

import io.github.yangentao.anno.ModelField
import io.github.yangentao.sql.TableModel
import io.github.yangentao.sql.TableModelClass
import io.github.yangentao.sql.clause.*
import io.github.yangentao.sql.pool.HarePool
import io.github.yangentao.sql.pool.LiteSources
import kotlin.test.Test

class OnClauseTest {

    @Test
    fun f1() {
        HarePool.pushSource(LiteSources.sqliteMemory())
        val a = Per::id EQ "per.name"
        println(a)
        println(a.arguments)
        val node = SELECT(Per.ALL).FROM(Per JOIN Stu ON (Per::id EQ "stu.id" AND (Stu::id GE 100)))
        println(node.toString())
        println(node.arguments)
    }
}

class Per : TableModel() {
    @ModelField(primaryKey = true, autoInc = 10)
    var id: Int by model

    @ModelField
    var name: String by model

    companion object : TableModelClass<Ai>()
}

class Stu : TableModel() {
    @ModelField(primaryKey = true, autoInc = 10)
    var id: Int by model

    @ModelField
    var name: String by model

    companion object : TableModelClass<Ai>()
}
