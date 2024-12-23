package lb.crazy.producer.scripting

import lb.crazy.model.*
import lb.crazy.producer.coding.CodeFile
import lb.crazy.producer.dialects.SqlDialect


open class SqlProducer (val dialect: SqlDialect) {

    val codeFiles = ArrayList<CodeFile>()


    fun produceCreateScriptForModel(model: Model) {
        for (schema in model.schemas) {
            produceCreateScriptForSchema(model, schema)
        }
    }

    private fun produceCreateScriptForSchema(model: Model, schema: Schema) {
        val schemaNameStr = schema.name?.replace('_', '-') ?: "schema"
        val schemaCodeFileName = "create-$schemaNameStr.sql"
        val schemaCodeFile = newCodeFile(schemaCodeFileName)

        makeSchemaHeader(schemaCodeFile, schemaNameStr)

        for (subjectArea in schema.areas) {
            val areaCode = subjectArea.prefix ?: "0"
            val areaCodeFileName = "create-$schemaNameStr-$areaCode.sql"
            val areaCodeFile = newCodeFile(areaCodeFileName)
            produceCreateScriptForSubjectArea(areaCodeFile, model, subjectArea)

            schemaCodeFile.coding {
                line(areaCodeFile.fileName)
            }
        }
    }

    protected fun makeSchemaHeader(file: CodeFile, schemaName: String?) =
        file.coding {
            phrase("-- Creating schema:", schemaName)
            emptyLine()
        }


    private fun produceCreateScriptForSubjectArea(file: CodeFile, model: Model, area: SubjectArea) {
        val schema: Schema = area.schema

        produceCreateTables(file, area)

    }

    protected fun produceCreateTables(file: CodeFile, area: SubjectArea) {
        for (table in area.tables) {
            produceCreateTable(file, table)
        }
    }

    protected fun produceCreateTable(file: CodeFile, table: Table) {
        file.coding {
            phrase("create table", table.name)
            line("(")
            frame {
                for (column in table.columns) {
                    val m = if (column.mandatory) "not null" else null
                    phrase(column.name, column.type.script(), m, ",")
                }
                for (index in table.indices) {
                    if (index.unique) {
                        if (index.name != null) phrase("constraint", index.name, eoln = false)
                        val w = index.primary.choose("primary key", "unique")
                        phrase(w, "(", index.columnNames, "),")
                    }
                }
                for (fk in table.foreignKeys) {
                    if (fk.name != null) phrase("constraint", fk.name, eoln = false)
                    val cascade = if (fk.cascadeDelete) "on delete cascade" else null
                    val refKey = fk.refKey
                    val refColumns = if (refKey.primary) null else refKey.columnNames
                    phrase("foreign key (", fk.domColumnNames, ") references", refKey.name, refColumns, cascade, ",")
                }
                removeLastChar(',')
            }
            line(")")
            line(dialect.commandDelimiter)
            emptyLine()
        }
    }


    fun Type.script() =
        when (this) {
            is IntType -> dialect.typeForInt(this.bytes)
            is DecimalIntType -> dialect.typeForDecimalInt(this.digits)
            is RangeIntType -> dialect.typeForIntRange(this.min, this.max)
            is BoolType -> dialect.typeForBoolean()
            is StrType -> dialect.typeForString(this.size)
        }


    private fun newCodeFile(fileName: String): CodeFile {
        val codeFile = CodeFile(fileName)
        codeFiles += codeFile
        return codeFile
    }

}