package com.zshnb.kvstore

import com.zshnb.kvstore.Command.*
import org.apache.commons.io.input.ReversedLinesFileReader
import java.io.*
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.*
import kotlin.concurrent.thread

class Store(private val dataFileName: String,
            private val transactionFilePrefix: String = "") {
    private val map: MutableMap<String, Any> = mutableMapOf()
    private var reader: BufferedReader
    private var inTransaction: Boolean = false
    private var transactionId: Int = 0
    private val unDoLogs = mutableListOf<UnDoLog>()
    private val reDoLogs = mutableListOf<ReDoLog>()
    private val commitCommands = mutableListOf<String>()
    private val transactionIdFile = "$transactionFilePrefix-transactionId.txt"

    init {
        val file = File(dataFileName)
        if (!file.exists()) {
            file.createNewFile()
        }
        File(transactionIdFile).let {
            if (!it.exists()) {
                it.createNewFile()
                it.writeText("0")
            }
        }
        reader = BufferedReader(FileReader(transactionIdFile))
        transactionId = reader.readLine().toInt()
        File("./").listFiles().filter { it.name.startsWith("$transactionFilePrefix-transaction-") && it.nameWithoutExtension.split("-").last().toInt() > transactionId }
            .sortedBy { it.nameWithoutExtension.split("-").last().toInt() }
            .forEach {
                it.readLines().forEach { line -> executeCommandPurely(line) }
                val writer = BufferedWriter(FileWriter(transactionIdFile))
                writer.write((transactionId + 1).toString())
            }

        reader = BufferedReader(InputStreamReader(FileInputStream(dataFileName)))
        reader.readLines().forEach {
            executeCommandPurely(it)
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
                    if (it.context().toString().startsWith(dataFileName)) {
                        val line = readEndLine()
                        executeCommandPurely(line)
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
                    commitCommands.add(line)
                    val key = strings[1]
                    val value = strings[2]
                    if (inTransaction) {
                        val undoLog = generateUnDoLog(command, key)
                        unDoLogs.add(undoLog)
                        reDoLogs.add(ReDoLog(line))
                    } else {
                        writeCommand(line)
                    }
                    executeSet(key, value)
                    println("OK!")
                }
            }
            DEL -> {
                if (strings.size != 2) {
                    println("invalid command")
                } else {
                    commitCommands.add(line)
                    val key = strings[1]
                    if (inTransaction) {
                        val undoLog = generateUnDoLog(command, key)
                        unDoLogs.add(undoLog)
                        reDoLogs.add(ReDoLog(line))
                    } else {
                        writeCommand(line)
                    }
                    val result = executeDel(key)
                    if (result) {
                        println("OK!")
                    } else {
                        println("$key not exist")
                    }
                }
            }
            BEGIN -> {
                if (inTransaction) {
                    println("could not create nested transaction")
                    return
                }
                unDoLogs.clear()
                reDoLogs.clear()
                inTransaction = true
            }
            ROLLBACK -> {
                if (!inTransaction) {
                    println("no transaction opening")
                    return
                }
                unDoLogs.forEach {
                    executeCommandPurely(it.command)
                }
            }
            COMMIT -> {
                if (!inTransaction) {
                    println("no transaction opening")
                    return
                }
                val reader = BufferedReader(FileReader(transactionIdFile))
                transactionId = reader.readLine().toInt() + 1
                writeReDoLogs()
                if (commitCommands.isNotEmpty()) {
                    writeCommand(commitCommands.joinToString(separator = System.lineSeparator()))
                }
                val writer = BufferedWriter(FileWriter(transactionIdFile))
                writer.write(transactionId.toString())
                writer.flush()
                inTransaction = false
            }
        }
    }

    private fun executeGet(key: String) {
        if (!map.containsKey(key)) {
            println("$key not exist")
        } else {
            val value = map.getValue(key)
            println("$key: $value")
        }
    }

    private fun executeSet(key: String, value: String) {
        map[key] = value
    }

    private fun executeDel(key: String): Boolean {
        return if (!map.containsKey(key)) {
            false
        } else {
            map.remove(key)
            true
        }
    }

    private fun writeCommand(line: String) {
        val channel = FileOutputStream(dataFileName, true).channel
        val fileLock = channel.lock()
        channel.position(channel.size())
        channel.write(ByteBuffer.wrap("$line\n".toByteArray()))
        fileLock.release()
        channel.close()
    }

    private fun writeReDoLogs() {
        val reDoLogfile = File("${transactionFilePrefix}-transaction-$transactionId.txt")
        if (!reDoLogfile.exists()) {
            reDoLogfile.createNewFile()
        }
        val bufferedWriter = BufferedWriter(FileWriter(reDoLogfile))
        val reDoLogContent = reDoLogs.joinToString(separator = System.lineSeparator()) { it.command }
        bufferedWriter.write(reDoLogContent)
        bufferedWriter.flush()
    }

    private fun executeCommandPurely(line: String) {
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

    private fun readEndLine(): String {
        val reader = ReversedLinesFileReader(File(dataFileName), 4096, StandardCharsets.UTF_8)
        return reader.readLine()
    }

    private fun generateUnDoLog(command: Command, key: String): UnDoLog {
        return when (command) {
            SET -> {
                if (map.containsKey(key)) {
                    UnDoLog("SET $key ${map[key]}")
                } else {
                    UnDoLog("DEL $key")
                }
            }
            DEL -> {
                UnDoLog("SET $key ${map[key]}")
            }
            else -> throw RuntimeException("$command not support")
        }
    }
}