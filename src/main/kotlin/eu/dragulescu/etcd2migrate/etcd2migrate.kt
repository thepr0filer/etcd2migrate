/*
 * Copyright (c) 2017. Alex Dragulescu alex@dragulescu.eu
 */
package eu.dragulescu.etcd2migrate

import com.google.gson.Gson
import eu.dragulescu.etcd2.Etcd2Node
import eu.dragulescu.etcd2.Etcd2Reply
import khttp.structures.authorization.BasicAuthorization
import java.lang.String.join
import java.net.URL
import java.time.LocalDateTime.now

import java.lang.reflect.Type
import java.util.*

// dir to dir http://127.0.0.1:2379 /dir1/ http://127.0.0.1:2379 /backup/dir1backup/ "u1" "p1" "u2" "p2"
// dir to dir http://127.0.0.1:2379 / http://127.0.0.1:2379 /backup/dir1backup/ "" "" "u2" "p2"
// key to key http://127.0.0.1:2379 /k1 http://127.0.0.1:2379 /backup/k22 "" "" "" ""
// key to key http://127.0.0.1:2379 /k1 http://127.0.0.1:2379 /k22 "" "" "" ""


fun main(args : Array<String>) {
    val config = Etcd2MigrateConfig(args)
    println(now().toString() + ": Migration started.")

    Etcd2ServerOperator(config.destinationServer).put(Etcd2ServerOperator(config.sourceServer).get())

    println(now().toString() + ": Migration completed.")
}

class Etcd2ServerOperator(val config: Etcd2ServerConfig) {
    fun get() : Etcd2Node {
        println(now().toString() + ": GET FROM ${config.getFullUrl()}")
        if (config.userName.isNotEmpty())
            return khttp.get(config.getFullUrl(), auth = BasicAuthorization(config.userName, config.userPass))
                .text
                .deserializeTo<Etcd2Reply>(Etcd2Reply::class.java)
                .node
                .updateSourceIfRequired(config.serverURL)
        else
            return khttp.get(config.getFullUrl())
                .text
                .deserializeTo<Etcd2Reply>(Etcd2Reply::class.java)
                .node
                .updateSourceIfRequired(config.serverURL)
    }

    fun put(what:Etcd2Node) {
        println(now().toString() + ": PUT $what")
        println(now().toString() + ": TO ${config.getFullUrl()}")

        val processingQueue = LinkedList<Etcd2Node>()
        processingQueue.push(what)

        // path to target
        val obase = what.key ?: "/"
        val nbase = config.nodeName

        while(processingQueue.isNotEmpty()) {
            var current = processingQueue.pop()
            if (current.dir) {
                // retrieve dirs if no nodes are already retrieved
                if (current.source != null)
                    current = Etcd2ServerOperator(Etcd2ServerConfig(current.source!!, current.key!!, config.userName, config.userPass)).get()

                current.nodes?.forEach { node -> processingQueue.push(node) }
            }

            val ckey = current.key ?: "/"
            val cpath = nbase
                    .plus(ckey.replaceFirst(obase, ""))
                    .bindTo(config.serverURL.toString())

            var currentData = "value=${current.value}"
            if (current.dir) currentData = "dir=true"

            if (config.userName.isNotEmpty())
                khttp.put(cpath, headers = mapOf("content-type" to "application/x-www-form-urlencoded"), data = currentData, auth = BasicAuthorization(config.userName, config.userPass))
            else
                khttp.put(cpath, headers = mapOf("content-type" to "application/x-www-form-urlencoded"), data = currentData)
        }
    }
}

fun Etcd2Node.updateSourceIfRequired(source: URL) : Etcd2Node {
    if (this.dir) {
        if (this.nodes == null)
            this.source = source
        else
            this.nodes.forEach { node -> node.updateSourceIfRequired(source) }
    }

    return this
}

fun <T> String.deserializeTo(t: Type) : T {
    return Gson().fromJson<T>(this, t)
}

fun String.bindTo(server:String) = server.plus(this)

data class Etcd2ServerConfig(val serverURL: URL, val nodeName:String, val userName:String, val userPass:String) {
    fun getFullUrl() = serverURL.toString().plus(nodeName)
}

class Etcd2MigrateConfig(args: Array<String>) {
    private val ETCD2_PREFIX = "v2/keys"
    private val URL_SEPARATOR = "/"

    val sourceServer:Etcd2ServerConfig
    val destinationServer:Etcd2ServerConfig

    init {
        if (args.size != 8) throw IllegalArgumentException("Required arguments: sourceServer sourceNode destinationServer destinationNode")
        if (args[1].isNullOrBlank() || args[3].isNullOrBlank()) throw IllegalArgumentException("A valid node name must be provided, full path from root required.")
        if (!args[1].startsWith('/') || !args[3].startsWith('/')) throw IllegalArgumentException("All node names must start in root")

        sourceServer = Etcd2ServerConfig(URL(join(URL_SEPARATOR, args[0],ETCD2_PREFIX)), args[1], args[4], args[5])
        destinationServer = Etcd2ServerConfig(URL(join(URL_SEPARATOR, args[2],ETCD2_PREFIX)), args[3], args[6], args[7])
    }
}
