@file:Suppress("unused")

package io.github.yangentao.sql

import io.github.yangentao.anno.userName
import io.github.yangentao.reflect.ownerClass
import io.github.yangentao.reflect.valueParams
import io.github.yangentao.sql.pool.namedConnection
import java.math.BigDecimal
import java.sql.CallableStatement
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Types
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.KParameter
import kotlin.reflect.KProperty0
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.hasAnnotation

typealias SQLProc = KFunction<*>

fun Connection.procedureDefine(proc: SQLProc): Int {
    val argStr = proc.valueParams.joinToString(",") { p ->
        defineProcParam(p)
    }
    val bd = proc.findAnnotation<SQLProcedure>()?.value ?: return 0
    var bodyStr = bd.trimIndent().trim()
    if (bodyStr.isNotEmpty() && !bodyStr.endsWith(';')) {
        bodyStr += ";"
    }
    val decl = """
				CREATE PROCEDURE ${proc.userName}($argStr)
				BEGIN
					$bodyStr
				END;
			""".trimIndent()
    return this.update(decl)
}

@Suppress("SqlSourceToSinkFlow", "SqlNoDataSourceInspection")
private fun Connection.procedureStatement(proc: SQLProc, args: List<Any?>): CallableStatement {
    val procName = proc.userName
    if (!procedureExist(procName)) {
        procedureDefine(proc)
    }
    val params = args.joinToString(",") { "?" }
    val st: CallableStatement = this.prepareCall("call $procName($params)")
    st.setParams(proc, args)
    return st
}

private fun CallableStatement.setParams(proc: SQLProc, args: List<Any?>) {
    for ((n, p) in proc.valueParams.withIndex()) {
        this.setObject(p.userName, args[n])
    }
}

fun Connection.procedureCall(proc: SQLProc, args: List<Any?>): LinkedHashMap<String, Any?> {
    val st = procedureStatement(proc, args)
    for (p in proc.valueParams.filter { it.hasAnnotation<ParamOut>() || it.hasAnnotation<ParamInOut>() }) {
        val t = classToSQLType(p).first
        st.registerOutParameter(p.userName, t)
    }
    try {
        st.use {
            st.execute()
            val map = LinkedHashMap<String, Any?>()
            for (p in proc.valueParams) {
                if (p.hasAnnotation<ParamOut>() || p.hasAnnotation<ParamInOut>()) {
                    val paramName = p.userName
                    map[paramName] = st.getObject(paramName)
                }
            }
            return map
        }
    } catch (ex: Exception) {
        println("Error Proc: ${proc.userName}")
        println("Args: $args ")
        ex.printStackTrace()
        throw ex
    }
}

fun Connection.procedureQuery(proc: SQLProc, args: List<Any?>): ResultSet {
    val st = procedureStatement(proc, args)
    return st.executeQuery()
}

private val SQLProc.connection: Connection
    get() {
        val cls: KClass<*> = this.ownerClass ?: error("No owner class")
        return cls.namedConnection
    }

fun SQLProc.procQuery(vararg args: Any): ResultSet {
    return this.connection.procedureQuery(this, args.toList())
}

fun SQLProc.procCall(vararg args: Any): LinkedHashMap<String, Any?> {
    return this.connection.procedureCall(this, args.toList())
}

fun Connection.functionCreate(funText: String) {
    val s = funText.trimIndent().trim()
    val st = this.prepareStatement(s)
    try {
        st.use {
            st.execute()
        }
    } catch (ex: Exception) {
        println("Create Function Error: $funText")
        ex.printStackTrace()
    }

}

fun Connection.functionNeeded(prop: KProperty0<String>) {
    if (functionExist(prop.userName)) return
    functionCreate(prop.get())
}

private fun defineProcParam(p: KParameter): String {
    val inoutStr = when {
        p.hasAnnotation<ParamIn>() -> {
            "IN "
        }

        p.hasAnnotation<ParamOut>() -> {
            "OUT "
        }

        p.hasAnnotation<ParamInOut>() -> {
            "INOUT "
        }

        else -> ""
    }
    val pt = classToSQLType(p).second
    return inoutStr + p.userName.escapeSQL + " " + pt
}

private fun classToSQLType(p: KParameter): Pair<Int, String> {
    val dc = p.findAnnotation<Decimal>()
    if (dc != null) {
        return Types.DECIMAL to "DECIMAL(${dc.precision},${dc.scale})"
    }
    return when (p.type.classifier) {
        String::class -> Types.VARCHAR to "VARCHAR"
        Byte::class -> Types.TINYINT to "TINYINT"
        Short::class -> Types.SMALLINT to "SMALLINT"
        Int::class -> Types.INTEGER to "INT"
        Long::class -> Types.BIGINT to "BIGINT"
        Float::class -> Types.FLOAT to "REAL"
        Double::class -> Types.DOUBLE to "DOUBLE"
        BigDecimal::class -> Types.DECIMAL to "DECIMAL"
        else -> error("unknown procedure out parameter")
    }
}