package com.zshnb.kvstore

import com.zshnb.kvstore.Command.*
import org.apache.commons.io.input.ReversedLinesFileReader
import java.io.*
import java.lang.RuntimeException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.*
import kotlin.concurrent.thread

class Store {
    private val map: MutableMap<String, Any> = mutableMapOf()
    private var reader: BufferedReader
    private var inTransaction: Boolean = false
    private val transactionId: Int = 0
    private val unDoLogs = mutableListOf<UnDoLog>()

    init {
        val file = File("data.txt")
        if (!file.exists()) {
            file.createNewFile()
        }
        reader = BufferedReader(InputStreamReader(FileInputStream("data.txt")))
        reader.readLines().forEach {
            val strings = it.split(" ")
            when (Command.valueOf(strings[0])) {
                SET -> {
                    val key = strings[1]
                    val value = strings[2]
                    executeSet(key, value)
                }
                DEL -> {
                    val key = strings[1]
                    executeDel(key)
                }
                else -> {
                }
            }
        }
        reader.close()
        println("load data.")
        thread {
            val watchService = FileSystems.getDefault().newWatchService()
            val path = Paths.get("C:\\Users\\85768\\workbench\\kv-store")
            path.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY)
            while (true) {
                val watchKey = watchService.take()
                watchKey.pollEvents().forEach {
                    if (it.context().toString().startsWith("data.txt")) {
                        val line = readEndLine()
                        val strings = line.split(" ")
                        when (Command.valueOf(strings[0])) {
                            SET -> {
                                val key = strings[1]
                                val value = strings[2]
                                executeSet(key, value)
                            }
                            DEL -> {
                                val key = strings[1]
                                executeDel(key)
                            }
                            else -> {
                            }
                        }
                    }
                }
                watchKey.reset()
            }
        }
    }

    fun executeCommand(line: String) {
        val strings = line.split(" ")
        val command = Command.valueOf(strings[0])
        when (command) {
            GET -> {
                if (strings.size != 2) {
                    println("invalid command")
                } else {
                    val key = strings[1]
                    executeGet(key)
                }
            }
            SET -> {
                if (strings.size != 3) {
                    println("invalid command")
                } else {
                    val key = strings[1]
                    val value = strings[2]
                    if (inTransaction) {
                        val undoLog = generateUnDoLog(command, key, value)
                        unDoLogs.add(undoLog)
                    } else {
                        executeSet(key, value)
                        writeCommand(line)
                        println("OK!")
                    }
                }
            }
            DEL -> {
                if (strings.size != 2) {
                    println("invalid command")
                } else {
                    val key = strings[1]
                    executeDel(key)
                    writeCommand(line)
                    println("OK!")
                }
            }
            BEGIN -> {
                inTransaction = true
            }
        }
    }

    private fun executeGet(key: String) {
        if (!map.containsKey(key)) {
            println("key: $key not exist")
        } else {
            val value = map.getValue(key)
            println("value: $value")
        }
    }

    private fun executeSet(key: String, value: String) {
        map[key] = value
    }

    private fun executeDel(key: String) {
        if (!map.containsKey(key)) {
            println("key: $key not exist")
        } else {
            map.remove(key)
        }
    }

    private fun writeCommand(line: String) {
        val channel = FileOutputStream("data.txt", true).channel
        val fileLock = channel.lock()
        channel.position(channel.size())
        channel.write(ByteBuffer.wrap("$line\n".toByteArray()))
        fileLock.release()
        channel.close()
    }

    private fun readEndLine(): String {
        val reader = ReversedLinesFileReader(File("data.txt"), 4096, StandardCharsets.UTF_8)
        return reader.readLine()
    }

    private fun generateUnDoLog(command: Command, key: String, value: String): UnDoLog {
        return when (command) {
            SET -> {
                if (map.containsKey(key)) {
                    UnDoLog("SET $key ${map[key]}")
                } else {
                    UnDoLog("DEL $key")
                }
            }
            DEL -> {
                UnDoLog("SET $key $value")
            }
            else -> throw RuntimeException("$command not support")
        }
    }
}