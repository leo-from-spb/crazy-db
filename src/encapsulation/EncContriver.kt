package lb.crazydb.encapsulation

import lb.crazydb.Model
import lb.crazydb.ModelFileContext
import lb.crazydb.NameSpec
import lb.crazydb.TableColumn
import lb.crazydb.TableRole.roleAlone
import lb.crazydb.gears.Dictionary
import lb.crazydb.gears.ReservedWords
import lb.crazydb.gears.nextInt
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

        // TODO
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

}
