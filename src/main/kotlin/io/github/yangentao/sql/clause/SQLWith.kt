@file:Suppress("FunctionName", "unused")

package io.github.yangentao.sql.clause

class WithNode(clause: Any? = null) : SQLNode(clause)

private fun testWithClause() {
    WITH("for_update_alias").AS {
        SELECT("id", "name").FROM("person").WHERE("score".GE(60)).FOR_UPDATE.LIMIT(10)
    }.UPDATE("person").WHERE("id".EQ(1))

    WITH(RECURSIVE("search_tree", listOf("id, link"))).AS {
        SELECT("id", "link").FROM("tree").WHERE("score".GE(60)).FOR_UPDATE.LIMIT(10)
    }.SELECT("a", "b")
}

fun WITH(exp: Any): WithNode {
    return WithNode("WITH")..exp
}

fun RECURSIVE(name: String, columns: List<Any> = emptyList()): SQLExpress {
    val node = SQLExpress("RECURSIVE")..name
    if (columns.isNotEmpty()) {
        node.brace(columns)
    }
    return node
}

fun WithNode.SELECT(vararg exps: Any): SQLNode {
    return this.._SELECT_LIST(exps.toList())
}

fun WithNode.SELECT_LIST(exps: List<Any>): SQLNode {
    return this.._SELECT_LIST(exps)
}

fun WithNode.INSERT_INTO(table: Any, vararg keyValues: Pair<Any, Any?>): SQLNode {
    return this..INSERT_INTO_VALUES(table, keyValues.map { it.first }, listOf(keyValues.map { it.second }))
}

fun WithNode.UPDATE(table: Any): SQLNode {
    return this..SQLNode("UPDATE")..table
}

fun WithNode.DELETE_FROM(table: Any): SQLNode {
    return this..SQLNode("DELETE FROM")..table
}

//var sql = """
//       WITH RECURSIVE cte(id, pid, name) AS (
//           SELECT id, pid ,name
//           FROM dept
//           WHERE  name = 'dev'
//           UNION
//           SELECT a.id, a.pid , a.name
//           FROM dept as a
//           JOIN cte ON a.id = cte.pid
//       )
//       SELECT id, pid, name FROM cte ;
//   """.trimIndent()


fun WITH_RECURSIVE_SELECT(name: String, vararg cols: Any, block: () -> SQLNode): SQLNode {
    return WITH(RECURSIVE(name, cols.toList())).AS(block).."SELECT"..cols.ifEmpty { "*" }.."FROM"..name
}
