/*
 * Copyright (c) 2017. Alex Dragulescu alex@dragulescu.eu
 */
package eu.dragulescu.etcd2

import eu.dragulescu.base.*
import khttp.structures.authorization.BasicAuthorization
import java.net.URL
import java.util.*
import java.time.LocalDateTime.now

data class Etcd2Node(override val key:String?="/", override val dir:Boolean=false, override val value:String, override val nodes:Collection<Etcd2Node>, val modifiedIndex:Long, val createdIndex:Long, override var source:URL?) : Node {
    override fun getConfig(from:ServerConfig): Etcd2ServerConfig {
        if (from !is Etcd2ServerConfig) throw IllegalArgumentException("")
        return Etcd2ServerConfig(source!!, key!!, from.userName, from.userPass)
    }
}

data class Etcd2Reply(val action:String, val node:Etcd2Node)

class Etcd2ServerOperator(val config: Etcd2ServerConfig) : ServerOperator {
    override fun get() : Etcd2Node {
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

    override fun put(what:Node) {
        println(now().toString() + ": PUT ${what.key}")
        println(now().toString() + ": TO ${config.getFullUrl()}")

        val processingQueue = LinkedList<Node>()
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

data class Etcd2ServerConfig(val serverURL: URL, val nodeName:String, val userName:String, val userPass:String) : ServerConfig {
    override fun getFullUrl() = serverURL.toString().plus(nodeName)
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
