/*
 * Copyright (c) 2017. Alex Dragulescu alex@dragulescu.eu
 */
package eu.dragulescu.etcd2migrate

import eu.dragulescu.base.ServerConfig
import eu.dragulescu.base.operator
import eu.dragulescu.etcd2.Etcd2ServerConfig
import eu.dragulescu.mysql.MySqlServerConfig
import java.lang.String.join
import java.net.URL
import java.time.LocalDateTime.now

// dir to dir http://127.0.0.1:2379 /dir1/ http://127.0.0.1:2379 /backup/dir1backup/ "u1" "p1" "u2" "p2" etcd2 etcd2
// dir to dir http://127.0.0.1:2379 / http://127.0.0.1:2379 /backup/dir1backup/ "" "" "u2" "p2" etcd2 etcd2
// key to key http://127.0.0.1:2379 /k1 http://127.0.0.1:2379 /backup/k22 "" "" "" "" etcd2 etcd2
// key to key http://127.0.0.1:2379 /k1 http://127.0.0.1:2379 /k22 "" "" "" "" etcd2 etcd2
// dir to mysql table http://127.0.0.1:2379 /source/directory/ "jdbc:mysql://user:pass@test.mysql:3306/database?useTimezone=true&serverTimezone=UTC" "mysql_table", "etcd2user", "etcd2pass", "mysql.string.key.column.name,mysql.string.key.column.size", "mysql.string.value.column.name,mysql.string.value.column.size" , "etcd2", "mysql"

fun main(args : Array<String>) {
    val config = Etcd2MigrateConfig(args)
    println(now().toString() + ": Migration started.")

    operator(config.destinationServer).put(operator(config.sourceServer).get())

    println(now().toString() + ": Migration completed.")
}

class Etcd2MigrateConfig(args: Array<String>) {
    private val ETCD2_PREFIX = "v2/keys"
    private val URL_SEPARATOR = "/"

    val sourceServer: ServerConfig
    val destinationServer: ServerConfig

    init {
        if (args.size != 10) throw IllegalArgumentException("Required arguments: sourceServer sourceNode destinationServer destinationNode")
        if (args[1].isNullOrBlank() || args[3].isNullOrBlank()) throw IllegalArgumentException("A valid node name must be provided, full path from root required.")
        if ((args[8] == "etcd2" && !args[1].startsWith('/')) || (args[9] == "etcd2" && !args[3].startsWith('/'))) throw IllegalArgumentException("All node names must start in root")

        if (args[8] != "etcd2" && args[8] != "mysql") throw IllegalArgumentException("Unsupported server type")
        if (args[9] != "etcd2" && args[9] != "mysql") throw IllegalArgumentException("Unsupported server type")

        if (args[8] == "mysql")
        {
            val ksettings = args[4].split(',')
            val vsettings = args[5].split(',')
            sourceServer = MySqlServerConfig(args[0], args[1], ksettings.first(), ksettings.last().toInt(), vsettings.first(), vsettings.last().toInt())
        }
        else
            sourceServer = Etcd2ServerConfig(URL(join(URL_SEPARATOR, args[0],ETCD2_PREFIX)), args[1], args[4], args[5])

        if (args[9] == "mysql")
        {
            val ksettings = args[6].split(',')
            val vsettings = args[7].split(',')
            destinationServer = MySqlServerConfig(args[2], args[3], ksettings.first(), ksettings.last().toInt(), vsettings.first(), vsettings.last().toInt())
        }
        else
            destinationServer = Etcd2ServerConfig(URL(join(URL_SEPARATOR, args[2],ETCD2_PREFIX)), args[3], args[6], args[7])
    }
}
