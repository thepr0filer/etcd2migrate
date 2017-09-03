package eu.dragulescu.base

import com.google.gson.Gson
import eu.dragulescu.etcd2.Etcd2ServerConfig
import eu.dragulescu.etcd2.Etcd2ServerOperator
import eu.dragulescu.mysql.MySqlServerConfig
import eu.dragulescu.mysql.MySqlServerOperator
import java.lang.reflect.Type
import java.net.URL

fun operator(of: ServerConfig) : ServerOperator {
    if (of is MySqlServerConfig) return MySqlServerOperator(of)

    return Etcd2ServerOperator(of as Etcd2ServerConfig)
}

interface Node {
    val key:String?
    val dir:Boolean
    val value:String
    val nodes:Collection<Node>
    var source: URL?

    fun getConfig(from:ServerConfig) : ServerConfig
}

interface ServerOperator {
    fun get() :Node
    fun put(what: Node)
}

fun <T> String.deserializeTo(t: Type) : T {
    return Gson().fromJson<T>(this, t)
}

fun String.bindTo(server:String) = server.plus(this)

interface ServerConfig {
    fun getFullUrl():String
}
