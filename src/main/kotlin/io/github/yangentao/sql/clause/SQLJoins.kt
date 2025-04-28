@file:Suppress("FunctionName", "unused")

package io.github.yangentao.sql.clause

import io.github.yangentao.reflect.KotClass
import io.github.yangentao.sql.BaseModelClass

class JoinNode(clause: String? = null) : SQLExpress(clause)
class OnCondition(clause: String? = null) : SQLExpress(clause)

private val newJoin: JoinNode get() = JoinNode()

//==
infix fun KotClass.JOIN(exp: Any): JoinNode {
    return newJoin..this.."JOIN"..exp.asKey
}

infix fun BaseModelClass<*>.JOIN(exp: Any): JoinNode {
    return newJoin..this.."JOIN"..exp.asKey
}

infix fun String.JOIN(exp: Any): JoinNode {
    return newJoin..this.asKey.."JOIN"..exp.asKey
}

infix fun SQLExpress.JOIN(exp: Any): JoinNode {
    return newJoin..this.."JOIN"..exp.asKey
}

//==
infix fun KotClass.INNER_JOIN(exp: Any): JoinNode {
    return newJoin..this.."INNER JOIN"..exp.asKey
}

infix fun BaseModelClass<*>.INNER_JOIN(exp: Any): JoinNode {
    return newJoin..this.."INNER JOIN"..exp.asKey
}

infix fun String.INNER_JOIN(exp: Any): JoinNode {
    return newJoin..this.asKey.."INNER JOIN"..exp.asKey
}

infix fun SQLExpress.INNER_JOIN(exp: Any): JoinNode {
    return newJoin..this.."INNER JOIN"..exp.asKey
}

//==
infix fun KotClass.LEFT_JOIN(exp: Any): JoinNode {
    return newJoin..this.."LEFT JOIN"..exp.asKey
}

infix fun BaseModelClass<*>.LEFT_JOIN(exp: Any): JoinNode {
    return newJoin..this.."LEFT JOIN"..exp.asKey
}

infix fun String.LEFT_JOIN(exp: Any): JoinNode {
    return newJoin..this.asKey.."LEFT JOIN"..exp.asKey
}

infix fun SQLExpress.LEFT_JOIN(exp: Any): JoinNode {
    return newJoin..this.."LEFT JOIN"..exp.asKey
}

//==
infix fun KotClass.RIGHT_JOIN(exp: Any): JoinNode {
    return newJoin..this.."RIGHT JOIN"..exp.asKey
}

infix fun BaseModelClass<*>.RIGHT_JOIN(exp: Any): JoinNode {
    return newJoin..this.."RIGHT JOIN"..exp.asKey
}

infix fun String.RIGHT_JOIN(exp: Any): JoinNode {
    return newJoin..this.asKey.."RIGHT JOIN"..exp.asKey
}

infix fun SQLExpress.RIGHT_JOIN(exp: Any): JoinNode {
    return newJoin..this.."RIGHT JOIN"..exp.asKey
}

//==
infix fun KotClass.FULL_JOIN(exp: Any): JoinNode {
    return newJoin..this.."FULL OUTER JOIN"..exp.asKey
}

infix fun BaseModelClass<*>.FULL_JOIN(exp: Any): JoinNode {
    return newJoin..this.."FULL OUTER JOIN"..exp.asKey
}

infix fun String.FULL_JOIN(exp: Any): JoinNode {
    return newJoin..this.asKey.."FULL OUTER JOIN"..exp.asKey
}

infix fun SQLExpress.FULL_JOIN(exp: Any): JoinNode {
    return newJoin..this.."FULL OUTER JOIN"..exp.asKey
}

//==
infix fun KotClass.CROSS_JOIN(exp: Any): JoinNode {
    return newJoin..this.."CROSS JOIN"..exp.asKey
}

infix fun BaseModelClass<*>.CROSS_JOIN(exp: Any): JoinNode {
    return newJoin..this.."CROSS JOIN"..exp.asKey
}

infix fun String.CROSS_JOIN(exp: Any): JoinNode {
    return newJoin..this.asKey.."CROSS JOIN"..exp.asKey
}

infix fun SQLExpress.CROSS_JOIN(exp: Any): JoinNode {
    return newJoin..this.."CROSS JOIN"..exp.asKey
}

infix fun JoinNode.ON(clause: OnCondition): JoinNode {
    return this.."ON"..clause.braced
}

infix fun JoinNode.ON(clause: String): JoinNode {
    return this.."ON("..clause..")"
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

infix fun PropSQL.EQUAL(exp: Any): OnCondition {
    return OnCondition()..this.."="..exp
}

infix fun String.EQUAL(exp: Any): OnCondition {
    return OnCondition()..this.."="..exp
}

infix fun OnCondition.AND(other: OnCondition): OnCondition {
    return this.."AND"..other
}

infix fun OnCondition.OR(other: OnCondition): OnCondition {
    return this.."OR"..other
}