package entao

import io.github.yangentao.anno.ModelField
import io.github.yangentao.sql.TableModel
import io.github.yangentao.sql.exec
import io.github.yangentao.sql.pool.HarePool
import io.github.yangentao.sql.pool.PostgresBuilder

fun main() {
    HarePool.push(PostgresBuilder(user = "test", password = "test", dbname = "test", host = "127.0.0.1"))
    val conn = HarePool.pick();
    conn.exec("CREATE TABLE IF NOT EXISTS test(id serial PRIMARY KEY, name text)")

    val h = Test()
    h.name = "yang"
    val ok = h.upsertReturning(emptyList())
    println(ok)
    println(h)
    h.name = "entaoyang"
    h.upsertReturning()
    println(h)
}

class Test : TableModel() {
    @ModelField(primaryKey = true, autoInc = 1)
    var id: Long by model

    @ModelField
    var name: String? by model
}