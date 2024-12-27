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
        produceCreateViews(file, area)
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
                    val name = column.name
                    val type = column.type
                    val m = if (column.mandatory) "not null" else null
                    val defaultExpression = column.defaultExpression?.let { "default ($it)" }
                    val innerCheck = if (type is RangeIntType) "check ($name between ${type.min} and ${type.max})" else null
                    phrase(name, type.script(), m, defaultExpression, innerCheck, ",")
                }
                for (index in table.indices) {
                    if (index.unique) {
                        phrase("constraint", index.name, eoln = false)
                        val w = index.primary.choose("primary key", "unique")
                        phrase(w, "(", index.columnNames, "),")
                    }
                }
                for (fk in table.foreignKeys) {
                    phrase("constraint", fk.name, eoln = false)
                    val cascade = if (fk.cascadeDelete) "on delete cascade" else null
                    val refKey = fk.refKey
                    val refColumns = if (!refKey.primary || dialect.foreignKeyAlwaysRequiresColumnList) refKey.columnNames.inParens else null
                    phrase("foreign key", fk.domColumnNames.inParens, "references", refKey.table.name, refColumns, cascade, ",")
                }
                removeLastChar(',')
            }
            line(")")
            line(dialect.commandDelimiter)
            emptyLine()

            for (index in table.indices) {
                if (index.unique) continue
                phrase("create index", index.name, "on", table.name, index.columnNames.inParens)
                line(dialect.commandDelimiter)
                emptyLine()
            }
        }
    }


    fun produceCreateViews(file: CodeFile, area: SubjectArea) {
        for (view in area.views) {
            produceCreateView(file, view)
        }
    }

    fun produceCreateView(file: CodeFile, view: View) {
        file.coding {
            phrase("create view", view.name, "as")

            var b = true
            var left: String
            for (column in view.columns) {
                left = b.choose("select ", "     , ")
                val item = column.nameWithAlias
                line(left + item)
                b = false
            }

            val conditions = ArrayList<String>()
            b = true
            for (section in view.sections) {
                left = b.choose("from ", "   , ")
                val domTableName = section.table.name
                val domTableQ = section.alias?.toString() ?: domTableName
                val domAlias = section.alias?.toString()
                val item = domTableName + (if (domAlias != null) " $domAlias" else "")
                line(left + item)
                for (bind in section.binds) {
                    val refTable = bind.refKey.table
                    val refSection = view.sections.first { it.table === refTable }
                    val refTableQ = refSection.alias?.toString() ?: refTable.name
                    val n = bind.domColumns.size
                    for (i in 0 until n) {
                        val domColumnName = bind.domColumns[i].name
                        val refColumnName = bind.refKey.columns[i].name
                        conditions += "$domTableQ.$domColumnName = $refTableQ.$refColumnName"
                    }
                }
                b = false
            }

            b = true
            for (condition in conditions) {
                left = b.choose("where ", "  and ")
                line(left + condition)
                b = false
            }

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