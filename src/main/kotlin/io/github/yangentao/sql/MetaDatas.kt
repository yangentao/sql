@file:Suppress("PropertyName", "unused")

package io.github.yangentao.sql

import java.sql.Connection

fun Connection.viewExists(viewName: String): Boolean {
    val tname = viewName.unescapeSQL.lowercase()
    val rs = this.metaData.getTables(this.catalog, this.schema, tname, arrayOf("VIEW"))
    return rs.one {
        tname == this.stringValue("TABLE_NAME")?.lowercase() || tname == this.stringValue("table_name")?.lowercase()
    } == true
}

fun Connection.tableExists(tableName: String): Boolean {
    val tname = tableName.unescapeSQL.lowercase()
    val rs = this.metaData.getTables(this.catalog, this.schema, tname, arrayOf("TABLE"))
    return rs.one {
        tname == this.stringValue("TABLE_NAME")?.lowercase() || tname == this.stringValue("table_name")?.lowercase()
    } == true
}

fun Connection.tableDesc(tableName: String): List<COLUMN_INFO> {
    val rs = this.metaData.getColumns(this.catalog, this.schema, tableName.unescapeSQL, "%")
    return rs.list { this.model() }
}

fun Connection.tableIndexList(tableName: String): List<INDEX_INFO> {
    val rs = this.metaData.getIndexInfo(this.catalog, this.schema, tableName.unescapeSQL, false, false)
    return rs.list { this.model() }
}

//PROCEDURE_CAT=manghe, PROCEDURE_SCHEM=null, PROCEDURE_NAME=queryAll, reserved1=null, reserved2=null, reserved3=null, REMARKS=, PROCEDURE_TYPE=1, SPECIFIC_NAME=queryAll,
fun Connection.procedureExist(procName: String): Boolean {
    val rs = this.metaData.getProcedures(this.catalog, this.schema, procName.unescapeSQL)
    return procName.unescapeSQL == rs.one { this.stringValue("PROCEDURE_NAME") }
}

fun Connection.functionExist(funName: String): Boolean {
    val rs = this.metaData.getFunctions(this.catalog, this.schema, funName.unescapeSQL)
    return funName.unescapeSQL == rs.one { this.stringValue("FUNCTION_NAME") }
}

fun Connection.dumpIndex(tableName: String) {
    val rs = this.metaData.getIndexInfo(this.catalog, this.schema, tableName.unescapeSQL, false, false)
    rs.dump()
}

//TABLE_CAT=apps, TABLE_SCHEM=null, TABLE_NAME=person4, COLUMN_NAME=id, DATA_TYPE=4, TYPE_NAME=INT, COLUMN_SIZE=10, BUFFER_LENGTH=65535, DECIMAL_DIGITS=null, NUM_PREC_RADIX=10, NULLABLE=0, REMARKS=, COLUMN_DEF=null, SQL_DATA_TYPE=0, SQL_DATETIME_SUB=0, CHAR_OCTET_LENGTH=null, ORDINAL_POSITION=1, IS_NULLABLE=NO, SCOPE_CATALOG=null, SCOPE_SCHEMA=null, SCOPE_TABLE=null, SOURCE_DATA_TYPE=null, IS_AUTOINCREMENT=YES, IS_GENERATEDCOLUMN=NO,
//TABLE_CAT=apps, TABLE_SCHEM=null, TABLE_NAME=person4, COLUMN_NAME=person, DATA_TYPE=-1, TYPE_NAME=JSON, COLUMN_SIZE=1073741824, BUFFER_LENGTH=65535, DECIMAL_DIGITS=null, NUM_PREC_RADIX=10, NULLABLE=0, REMARKS=, COLUMN_DEF=null, SQL_DATA_TYPE=0, SQL_DATETIME_SUB=0, CHAR_OCTET_LENGTH=null, ORDINAL_POSITION=2, IS_NULLABLE=NO, SCOPE_CATALOG=null, SCOPE_SCHEMA=null, SCOPE_TABLE=null, SOURCE_DATA_TYPE=null, IS_AUTOINCREMENT=NO, IS_GENERATEDCOLUMN=NO,
class COLUMN_INFO {
    var TABLE_CAT: String? = null
    var TABLE_SCHEM: String? = null
    var TABLE_NAME: String = ""

    var COLUMN_NAME: String = ""
    var DATA_TYPE: Int = 0
    var TYPE_NAME: String = ""
    var COLUMN_SIZE: Int? = null
    var NULLABLE: Int = 0
    var IS_NULLABLE: String = ""
    var IS_AUTOINCREMENT: String = ""
    var IS_GENERATEDCOLUMN: String = ""

    val tableName: String get() = TABLE_NAME
    val columnName: String get() = COLUMN_NAME
    val typeName: String get() = TYPE_NAME
    val autoInc: Boolean get() = IS_AUTOINCREMENT == "YES"
    val nullable: Boolean get() = IS_NULLABLE == "YES"

}

//table_cat=null, table_schem=public, table_name=logtable, non_unique=false, index_qualifier=null, index_name=logtable_pkey, type=3, ordinal_position=1, column_name=id, asc_or_desc=A, cardinality=0.0, pages=1, filter_condition=null,

//TABLE_CAT=campus, TABLE_SCHEM=null, TABLE_NAME=ip, NON_UNIQUE=false, INDEX_QUALIFIER=,
// INDEX_NAME=PRIMARY, TYPE=3, ORDINAL_POSITION=1, COLUMN_NAME=id, ASC_OR_DESC=A, CARDINALITY=6,
// PAGES=0, FILTER_CONDITION=null,
class INDEX_INFO {
    var TABLE_CAT: String? = ""
    var TABLE_NAME: String = ""
    var INDEX_NAME: String = ""
    var COLUMN_NAME: String = ""
    var TYPE: Int = 0

    override fun toString(): String {
        return "INDEX_INFO(cat: $TABLE_CAT, tableName: $TABLE_NAME, indexName:$INDEX_NAME, column:$COLUMN_NAME, type: $TYPE)"
    }

}