@file:Suppress("FunctionName", "unused")

package io.github.yangentao.sql.clause

import io.github.yangentao.sql.BaseModelClass
import kotlin.reflect.KClass

class JoinNode(left: Any, join: String, right: Any) : SQLExpress() {
    init {
        this..left..join..right
    }
}

infix fun JoinNode.ON(express: Any): SQLExpress {
    return this.."ON"..express
}

infix fun JoinNode.USING(express: Any): SQLExpress {
    return this.."USING("..express..")"
}

//==
infix fun KClass<*>.JOIN(exp: Any): JoinNode {
    return JoinNode(this, "JOIN", exp)
}

infix fun BaseModelClass<*>.JOIN(exp: Any): JoinNode {
    return JoinNode(this, "JOIN", exp)
}

infix fun String.JOIN(exp: Any): JoinNode {
    return JoinNode(this, "JOIN", exp)
}

infix fun SQLExpress.JOIN(exp: Any): JoinNode {
    return JoinNode(this, "JOIN", exp)
}

//==
infix fun KClass<*>.INNER_JOIN(exp: Any): JoinNode {
    return JoinNode(this, "INNER JOIN", exp)
}

infix fun BaseModelClass<*>.INNER_JOIN(exp: Any): JoinNode {
    return JoinNode(this, "INNER JOIN", exp)
}

infix fun String.INNER_JOIN(exp: Any): JoinNode {
    return JoinNode(this, "INNER JOIN", exp)
}

infix fun SQLExpress.INNER_JOIN(exp: Any): JoinNode {
    return JoinNode(this, "INNER JOIN", exp)
}

//==
infix fun KClass<*>.LEFT_JOIN(exp: Any): JoinNode {
    return JoinNode(this, "LEFT JOIN", exp)
}

infix fun BaseModelClass<*>.LEFT_JOIN(exp: Any): JoinNode {
    return JoinNode(this, "LEFT JOIN", exp)
}

infix fun String.LEFT_JOIN(exp: Any): JoinNode {
    return JoinNode(this, "LEFT JOIN", exp)
}

infix fun SQLExpress.LEFT_JOIN(exp: Any): JoinNode {
    return JoinNode(this, "LEFT JOIN", exp)
}

//==
infix fun KClass<*>.RIGHT_JOIN(exp: Any): JoinNode {
    return JoinNode(this, "RIGHT JOIN", exp)
}

infix fun BaseModelClass<*>.RIGHT_JOIN(exp: Any): JoinNode {
    return JoinNode(this, "RIGHT JOIN", exp)
}

infix fun String.RIGHT_JOIN(exp: Any): JoinNode {
    return JoinNode(this, "RIGHT JOIN", exp)
}

infix fun SQLExpress.RIGHT_JOIN(exp: Any): JoinNode {
    return JoinNode(this, "RIGHT JOIN", exp)
}

//==
infix fun KClass<*>.FULL_JOIN(exp: Any): JoinNode {
    return JoinNode(this, "FULL OUTER JOIN", exp)
}

infix fun BaseModelClass<*>.FULL_JOIN(exp: Any): JoinNode {
    return JoinNode(this, "FULL OUTER JOIN", exp)
}

infix fun String.FULL_JOIN(exp: Any): JoinNode {
    return JoinNode(this, "FULL OUTER JOIN", exp)
}

infix fun SQLExpress.FULL_JOIN(exp: Any): JoinNode {
    return JoinNode(this, "FULL OUTER JOIN", exp)
}

//==
infix fun KClass<*>.CROSS_JOIN(exp: Any): JoinNode {
    return JoinNode(this, "CROSS JOIN", exp)
}

infix fun BaseModelClass<*>.CROSS_JOIN(exp: Any): JoinNode {
    return JoinNode(this, "CROSS JOIN", exp)
}

infix fun String.CROSS_JOIN(exp: Any): JoinNode {
    return JoinNode(this, "CROSS JOIN", exp)
}

infix fun SQLExpress.CROSS_JOIN(exp: Any): JoinNode {
    return JoinNode(this, "CROSS JOIN", exp)
}



