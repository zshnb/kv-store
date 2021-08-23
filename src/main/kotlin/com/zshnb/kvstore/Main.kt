package com.zshnb.kvstore

fun main() {
    val store = Store("data.txt")
    while (true) {
        val line = readLine()
        if (line == null || line.isEmpty()) {
            println("empty!")
        } else {
            store.executeCommand(line)
        }
    }
}