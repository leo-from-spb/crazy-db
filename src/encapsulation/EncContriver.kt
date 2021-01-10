package lb.crazydb.encapsulation

import lb.crazydb.*
import lb.crazydb.Direction.dir_In
import lb.crazydb.TableRole.roleAlone
import lb.crazydb.gears.*
import lb.crazydb.gears.Dictionary
import java.util.*
import kotlin.collections.HashSet


class EncContriver (val model: Model, val dict: Dictionary, val areaPrefix: String) {


    private val usedNouns = HashSet<String>()

    val rnd = Random(System.nanoTime() * 19L + areaPrefix.hashCode())

    fun inventSchema(filesNumber: Int, tablesPerFile: Int, dataColumnsLim: Int) {
        for (f in 1..filesNumber) inventFile(f, tablesPerFile, dataColumnsLim)
    }


    private fun inventFile(fileNr: Int, tablesNumber: Int, dataColumnsLim: Int) {
        for (i in 1..tablesNumber) inventTable(fileNr, dataColumnsLim)
    }


    private fun inventTable(fileNr: Int, dataColumnsLim: Int) {
        val ctx = ModelFileContext(model, areaPrefix, fileNr)
        val mainName = dict.guessNoun(8, usedNouns, ReservedWords.words)

        val sequence = ctx.newSequence(mainName, "seq")

        val table = ctx.newTable(roleAlone, mainName)
        table.associatedSequence = sequence
        val keyNameSpec = inventKeyColumn()
        val keyColumn = table.newColumn(keyNameSpec)

        val usedNames = TreeSet<String>()
        usedNames += mainName
        usedNames += keyColumn.nameWords

        val dataColumnNumber = rnd.nextInt(3, dataColumnsLim)
        val dataColumns = Array<TableColumn>(dataColumnNumber) {
            val name = dict.guessNoun(3, usedNames, ReservedWords.words)
            val spec = inventDataColumnSpec()
            usedNames += name
            table.newColumn(NameSpec(name, spec))
        }

        inventPackage(ctx, table, keyColumn, dataColumns)
    }


    private fun inventKeyColumn(): NameSpec {
         return when (val x = rnd.nextInt(4)) {
             0 -> NameSpec("id", "number(${rnd.nextInt(6, 12)})")
             1 -> NameSpec("id", "decimal(${rnd.nextInt(4, 8)})")
             2 -> NameSpec("code", "char")
             3 -> NameSpec("code", "varchar(${rnd.nextInt(2, 8)})")
             else -> throw RuntimeException("Impossible fork: $x")
         }
    }

    private fun inventDataColumnSpec(): String {
        return when (val x = rnd.nextInt(7)) {
            0 -> "date -> sysdate"
            1 -> "char"
            2 -> "char(2)"
            3 -> "decimal(${rnd.nextInt(1,8)})"
            4 -> "number(${rnd.nextInt(3,18)})"
            5 -> "char(${rnd.nextInt(3,18)})"
            6 -> "varchar(${rnd.nextInt(1,16)*5})"
            else -> throw RuntimeException("Impossible fork: $x")
        }
    }


    private fun inventPackage(ctx: ModelFileContext, table: Table, keyColumn: TableColumn, dataColumns: Array<TableColumn>) {
        val pack = ctx.newPackage(*(table.nameWords + "man"))
        val tableName = table.name
        val keyColumnName = keyColumn.name

        // insert
        val instVerbs: Couple<String> = when (rnd.nextInt(4)) {
            0 -> "new" to "delete"
            1 -> "add" to "remove"
            2 -> "register" to "deregister"
            3 -> "create" to "delete"
            else -> "xxx" to "yyy"
        }
        val p1 = pack.newRoutine(arrayOf(instVerbs.first) + table.nameWords)
        for (column in table.columns) {
            val arg = Argument(column.name + "_", dir_In, formalType(column.dataType))
            p1.arguments += arg
        }
        p1.bodyText = """|insert 
                         |¬into $tableName ($keyColumnName, ${dataColumns.joinToString{it.name}})
                         |¬values (${keyColumnName}_, ${dataColumns.joinToString{it.name+'_'}});
                      """.trimMargin().tabs()

        // update
        val updateVerb: String = when (rnd.nextInt(3)) {
            0 -> "update"
            1 -> "change"
            2 -> "modify"
            else -> "zzz"
        }
        for (column in dataColumns) {
            val p = pack.newRoutine(arrayOf(updateVerb) + column.nameWords)
            val keyArg = Argument(keyColumnName + "_", dir_In, formalType(keyColumn.dataType))
            p.arguments += keyArg
            val columnName = column.name
            val arg = Argument(columnName + "_", dir_In, formalType(column.dataType))
            p.arguments += arg
            p.bodyText = """|update $tableName
                            |¬set $columnName = ${columnName}_ 
                            |¬where $keyColumnName = ${keyColumnName}_;
                         """.trimMargin().tabs()
        }

        // delete
        val p9 = pack.newRoutine(arrayOf(instVerbs.second) + table.nameWords)
        val arg9 = Argument(keyColumn.name + "_", dir_In, formalType(keyColumn.dataType))
        p9.arguments += arg9
        p9.bodyText = """|delete $tableName
                         |¬where $keyColumnName = ${keyColumnName}_;
                      """.trimMargin().tabs()
    }

}
