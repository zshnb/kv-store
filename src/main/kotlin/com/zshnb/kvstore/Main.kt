package com.zshnb.kvstore

fun main() {
    val store = Store()
    while (true) {
        val line = readLine()
        if (line == null || line.isEmpty()) {
            println("empty!")
        } else {
            store.executeCommand(line)
        }
    }
}