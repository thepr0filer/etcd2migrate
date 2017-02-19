/*
 * Copyright (c) 2017. Alex Dragulescu alex@dragulescu.eu
 */
package eu.dragulescu.etcd2

import java.net.URL

data class Etcd2Node(val key:String?="/", val dir:Boolean=false, val value:String, val nodes:Collection<Etcd2Node>, val modifiedIndex:Long, val createdIndex:Long, var source:URL?)

data class Etcd2Reply(val action:String, val node:Etcd2Node)
