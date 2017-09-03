package eu.dragulescu.mysql

import eu.dragulescu.base.Node
import eu.dragulescu.base.ServerConfig
import eu.dragulescu.base.ServerOperator
import eu.dragulescu.base.operator
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction
import java.net.URL
import java.util.*
import java.time.LocalDateTime.now

data class MysqlNode(override val key:String?="/", override val dir:Boolean=false, override val value:String, override val nodes:Collection<MysqlNode>, override var source: URL?) : Node {
    override fun getConfig(from: ServerConfig): ServerConfig {
        return from
    }
}


class MySqlServerOperator(val config: MySqlServerConfig) : ServerOperator {
    val ct : kvtable
    init {
        Database.connect(config.serverURL, "com.mysql.cj.jdbc.Driver")
        ct = kvtable(config.kvTableName,config.kName,config.kSize,config.vName,config.vSize)
    }

    class kvtable(name:String, kName:String, kSize:Int, vName:String, vSize:Int) : Table(name) {
        val key=varchar(kName, kSize)
        val value=varchar(vName, vSize)
    }


    override fun get() : MysqlNode {
        println(now().toString() + ": GET FROM ${config.getFullUrl()}")

        val source = URL(config.getFullUrl())
        val entries = LinkedHashSet<MysqlNode>()

        transaction {
            ct.select { ct.key.isNotNull() }
                    .forEach { entry ->
                        entries.add(MysqlNode(entry[ct.key],false,entry[ct.value],Collections.emptyList(),source))
                    }
        }

        return MysqlNode(config.kvTableName, true, "", entries, source)
    }

    override fun put(what:Node) {
        println(now().toString() + ": PUT ${what.key}")
        println(now().toString() + ": TO ${config.getFullUrl()}")

        val processingQueue = LinkedList<Node>()
        val current_batch = LinkedList<Node>()
        processingQueue.push(what)

        val auto_batch_size = 100

        if (what.dir) {
            println(now().toString() + ": TODOs list is ${what.nodes.size} long, batch size of $auto_batch_size")
        }

        while(processingQueue.isNotEmpty()) {
            var current = processingQueue.pop()
            if (current.dir) {
                // retrieve dirs if no nodes are already retrieved
                if (current.source != null)
                    current = operator(current.getConfig(config)).get()

                current.nodes.forEach { node -> processingQueue.push(node) }
            } else {
                // atomically execute each entry (vs having a single or batch of transactions)
               current_batch.push(current)
            }

            if (current_batch.size == auto_batch_size || processingQueue.isEmpty()) {
                try {
                    transaction {
                        while(current_batch.isNotEmpty()) {
                            val cn = current_batch.pop()
                            ct.insertIgnore {
                                it[key] = cn.key!!.split('/').last()
                                it[value] = cn.value
                            }
                        }
                    }
                } finally {
                    current_batch.clear()
                }
            }
        }
    }
}

data class  MySqlServerConfig(val serverURL: String, val kvTableName:String, val kName:String, val kSize:Int, val vName:String, val vSize:Int) : ServerConfig {
    override fun getFullUrl() = serverURL
}
