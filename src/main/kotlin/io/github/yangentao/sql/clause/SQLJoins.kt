@file:Suppress("FunctionName", "unused")

package io.github.yangentao.sql.clause

import io.github.yangentao.sql.BaseModelClass
import kotlin.reflect.KClass

class JoinNode(clause: String? = null) : SQLExpress(clause)

private val newJoin: JoinNode get() = JoinNode()

//==
infix fun KClass<*>.JOIN(exp: Any): JoinNode {
    return newJoin..this.."JOIN"..exp.asExpress
}

infix fun BaseModelClass<*>.JOIN(exp: Any): JoinNode {
    return newJoin..this.."JOIN"..exp.asExpress
}

infix fun String.JOIN(exp: Any): JoinNode {
    return newJoin..this.asExpress.."JOIN"..exp.asExpress
}

infix fun SQLExpress.JOIN(exp: Any): JoinNode {
    return newJoin..this.."JOIN"..exp.asExpress
}

//==
infix fun KClass<*>.INNER_JOIN(exp: Any): JoinNode {
    return newJoin..this.."INNER JOIN"..exp.asExpress
}

infix fun BaseModelClass<*>.INNER_JOIN(exp: Any): JoinNode {
    return newJoin..this.."INNER JOIN"..exp.asExpress
}

infix fun String.INNER_JOIN(exp: Any): JoinNode {
    return newJoin..this.asExpress.."INNER JOIN"..exp.asExpress
}

infix fun SQLExpress.INNER_JOIN(exp: Any): JoinNode {
    return newJoin..this.."INNER JOIN"..exp.asExpress
}

//==
infix fun KClass<*>.LEFT_JOIN(exp: Any): JoinNode {
    return newJoin..this.."LEFT JOIN"..exp.asExpress
}

infix fun BaseModelClass<*>.LEFT_JOIN(exp: Any): JoinNode {
    return newJoin..this.."LEFT JOIN"..exp.asExpress
}

infix fun String.LEFT_JOIN(exp: Any): JoinNode {
    return newJoin..this.asExpress.."LEFT JOIN"..exp.asExpress
}

infix fun SQLExpress.LEFT_JOIN(exp: Any): JoinNode {
    return newJoin..this.."LEFT JOIN"..exp.asExpress
}

//==
infix fun KClass<*>.RIGHT_JOIN(exp: Any): JoinNode {
    return newJoin..this.."RIGHT JOIN"..exp.asExpress
}

infix fun BaseModelClass<*>.RIGHT_JOIN(exp: Any): JoinNode {
    return newJoin..this.."RIGHT JOIN"..exp.asExpress
}

infix fun String.RIGHT_JOIN(exp: Any): JoinNode {
    return newJoin..this.asExpress.."RIGHT JOIN"..exp.asExpress
}

infix fun SQLExpress.RIGHT_JOIN(exp: Any): JoinNode {
    return newJoin..this.."RIGHT JOIN"..exp.asExpress
}

//==
infix fun KClass<*>.FULL_JOIN(exp: Any): JoinNode {
    return newJoin..this.."FULL OUTER JOIN"..exp.asExpress
}

infix fun BaseModelClass<*>.FULL_JOIN(exp: Any): JoinNode {
    return newJoin..this.."FULL OUTER JOIN"..exp.asExpress
}

infix fun String.FULL_JOIN(exp: Any): JoinNode {
    return newJoin..this.asExpress.."FULL OUTER JOIN"..exp.asExpress
}

infix fun SQLExpress.FULL_JOIN(exp: Any): JoinNode {
    return newJoin..this.."FULL OUTER JOIN"..exp.asExpress
}

//==
infix fun KClass<*>.CROSS_JOIN(exp: Any): JoinNode {
    return newJoin..this.."CROSS JOIN"..exp.asExpress
}

infix fun BaseModelClass<*>.CROSS_JOIN(exp: Any): JoinNode {
    return newJoin..this.."CROSS JOIN"..exp.asExpress
}

infix fun String.CROSS_JOIN(exp: Any): JoinNode {
    return newJoin..this.asExpress.."CROSS JOIN"..exp.asExpress
}

infix fun SQLExpress.CROSS_JOIN(exp: Any): JoinNode {
    return newJoin..this.."CROSS JOIN"..exp.asExpress
}

infix fun JoinNode.USING(clause: PropSQL): JoinNode {
    return this.."USING("..clause.asColumn..")"
}

infix fun JoinNode.USING(clause: String): JoinNode {
    return this.."USING("..clause.asColumn..")"
}

infix fun JoinNode.USING(clauses: List<Any>): JoinNode {
    this.."USING("
    this.addList(clauses)
    this..")"
    return this
}

infix fun JoinNode.ON(condition: Where): JoinNode {
    return this.."ON"..condition.braced
}

infix fun JoinNode.ON(clause: String): JoinNode {
    return this.."ON("..clause..")"
}

infix fun PropSQL.EQUAL(exp: Any): Where {
    return this.EQ(exp)
}

infix fun String.EQUAL(exp: Any): Where {
    return this.EQ(exp)
}
