package entao

import io.github.yangentao.anno.ModelField
import io.github.yangentao.sql.*
import io.github.yangentao.sql.pool.HarePool
import io.github.yangentao.sql.pool.LiteSources
import io.github.yangentao.xlog.logd
import kotlin.test.Test

class Person : TableModel() {
    @ModelField(primaryKey = true)
    var name: String by model

    @ModelField(defaultValue = "")
    var addr: String by model

    @ModelField(defaultValue = "0")
    var age: Int by model

    override fun toString(): String {
        return "Person(name=$name, addr=$addr, age=$age )"
    }

    companion object : TableModelClass<Person>()
}

class OrmTest {
    @Test
    fun indexList() {
        HarePool.pushSource(LiteSources.sqliteMemory())
        val c = HarePool.pick()
        TableMigrater(c, Person::class)
        val ls = c.tableIndexList(Person::class.nameSQL)
        for (a in ls) {
            println(a)
        }

    }

    @Test
    fun orm() {
        HarePool.pushSqliteMemory()
        val p = Person()
        p.name = "entao"
        p.addr = "Shandong"
        p.age = 99
        p.upsert()
        val ls = Person.list()
        for (p in ls) {
            logd(p)
        }
    }
}