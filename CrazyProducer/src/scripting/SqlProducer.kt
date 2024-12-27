package lb.crazy.producer.scripting

import lb.crazy.model.*
import lb.crazy.producer.coding.CodeFile
import lb.crazy.producer.coding.CodeFrame
import lb.crazy.producer.dialects.SqlDialect


open class SqlProducer (val dialect: SqlDialect, val settings: SqlGeneratingSettings) {

    val codeFiles = ArrayList<CodeFile>()

    fun produceCreateScriptForModel(model: Model) {
        for (schema in model.schemas) {
            val schemaScriptProducer = SchemaScriptProducer(model, schema)
            schemaScriptProducer.produceCreateScriptForSchema()
        }
    }


    private inner class SchemaScriptProducer(val model: Model, val schema: Schema) {

        val schemaQualifier: String? = if (settings.qualifyNames) schema.name + '.' else null


        fun produceCreateScriptForSchema() {
            val schemaCodeFileName = "create-${schema.fileLemma}.sql"
            val schemaCodeFile = newCodeFile(schemaCodeFileName)

            makeSchemaHeader(schemaCodeFile)

            for (subjectArea in schema.areas) {
                val areaCode = subjectArea.prefix ?: "0"
                val areaFileName = "create-${schema.fileLemma}-$areaCode.sql"
                val areaFile = newCodeFile(areaFileName)
                produceCreateScriptForSubjectArea(areaFile, subjectArea)

                schemaCodeFile.coding {
                    val includeTemplate = dialect.commandIncludeFile
                    if (includeTemplate != null) line(includeTemplate.replace("FILEPATH", areaFileName))
                    else phrase("-- perform script from the file", areaFileName)
                }
            }
        }

        protected fun makeSchemaHeader(file: CodeFile) =
            file.coding {
                val schemaName = schema.name
                val password = schemaName.first().uppercaseChar().toString()
                phrase("-- Creating schema:", schemaName)

                dialect.commandCreateSchema?.also {
                    val cmd = it.replace("SCHEMA", schemaName).replace("PASSWORD", password)
                    command(cmd)
                }
                dialect.commandSetCurrentSchema?.also {
                    val cmd = it.replace("SCHEMA", schemaName)
                    command(cmd)
                }

                emptyLine()
            }

        private fun produceCreateScriptForSubjectArea(areaFile: CodeFile, area: SubjectArea) {
            produceCreateTables(areaFile, area)
            produceCreateViews(areaFile, area)
        }

        protected fun produceCreateTables(file: CodeFile, area: SubjectArea) {
            for (table in area.tables) {
                produceCreateTable(file, table)
            }
        }

        protected fun produceCreateTable(file: CodeFile, table: Table) {
            file.coding {
                val constraintQualifier = if (settings.qualifyNames && dialect.constraintNameIsGlobal) schemaQualifier else null
                phrase("create table", schemaQualifier, table.name)
                line("(")
                frame {
                    for (column in table.columns) {
                        val name = column.name
                        val type = column.type
                        val m = if (column.mandatory) "not null" else null
                        val defaultExpression = column.defaultExpression?.let { "default ($it)" }
                        val innerCheck =
                            if (type is RangeIntType) "check ($name between ${type.min} and ${type.max})" else null
                        phrase(name, type.script(), m, defaultExpression, innerCheck, ",")
                    }
                    for (index in table.indices) {
                        if (index.unique) {
                            phrase("constraint", constraintQualifier, index.name, eoln = false)
                            val w = index.primary.choose("primary key", "unique")
                            phrase(w, "(", index.columnNames, "),")
                        }
                    }
                    for (fk in table.foreignKeys) {
                        phrase("constraint", constraintQualifier, fk.name, eoln = false)
                        val cascade = if (fk.cascadeDelete) "on delete cascade" else null
                        val refKey = fk.refKey
                        val refColumns =
                            if (!refKey.primary || dialect.foreignKeyAlwaysRequiresColumnList) refKey.columnNames.inParens else null
                        phrase("foreign key",
                               fk.domColumnNames.inParens,
                               "references",
                               refKey.table.name,
                               refColumns,
                               cascade,
                               ",")
                    }
                    removeLastChar(',')
                }
                line(")")
                endCommand()
                emptyLine()

                for (index in table.indices) {
                    if (index.unique) continue
                    phrase("create index", schemaQualifier, index.name, "on", table.name, index.columnNames.inParens)
                    endCommand()
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
                phrase("create view", schemaQualifier, view.name, "as")

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
                    val domTableNameQ = schemaQualifier?.let { it + domTableName } ?: domTableName
                    val domTableQ = section.alias?.toString() ?: domTableName
                    val domAlias = section.alias?.toString()
                    val item = domTableNameQ + (if (domAlias != null) " $domAlias" else "")
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

                endCommand()
                emptyLine()
            }
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

    private fun CodeFrame.command(commandText: CharSequence?) {
        if (commandText != null) {
            line(commandText)
            endCommand()
        }
    }

    private fun CodeFrame.endCommand() {
        line(dialect.commandDelimiter)
    }

}