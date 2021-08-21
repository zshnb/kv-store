package com.zshnb.kvstore

import org.apache.commons.io.FileUtils
import org.junit.jupiter.api.*
import java.io.*

class MainTest {
    private val byteArrayOutputStream = ByteArrayOutputStream()
    private val file = File("test.txt")
    @BeforeEach
    fun setup() {
        file.delete()
        file.createNewFile()
        System.setOut(PrintStream(byteArrayOutputStream))
    }

    @Test
    fun getAndSetSuccessful() {
        val store = Store("test.txt")
        byteArrayOutputStream.reset() // skip log(load data)
        val getCommand = "GET key"
        store.executeCommand(getCommand)
        Assertions.assertEquals("key not exist${System.lineSeparator()}", byteArrayOutputStream.toString())
        byteArrayOutputStream.reset()

        val setCommand = "SET key 1"
        store.executeCommand(setCommand)
        Assertions.assertEquals("OK!${System.lineSeparator()}", byteArrayOutputStream.toString())
        byteArrayOutputStream.reset()

        store.executeCommand(getCommand)
        Assertions.assertEquals("key: 1${System.lineSeparator()}", byteArrayOutputStream.toString())
        val lines = FileUtils.readLines(file)
        Assertions.assertEquals(setCommand, lines[0])
    }

    @Test
    fun getInvalid() {
        val store = Store("test.txt")
        byteArrayOutputStream.reset() // skip log(load data)
        val getCommand = "GET key 1"
        store.executeCommand(getCommand)
        Assertions.assertEquals("invalid command${System.lineSeparator()}", byteArrayOutputStream.toString())
    }

    @Test
    fun setInvalid() {
        val store = Store("test.txt")
        byteArrayOutputStream.reset() // skip log(load data)
        val getCommand = "SET key"
        store.executeCommand(getCommand)
        Assertions.assertEquals("invalid command${System.lineSeparator()}", byteArrayOutputStream.toString())
    }

    @Test
    fun delInvalid() {
        val store = Store("test.txt")
        byteArrayOutputStream.reset() // skip log(load data)
        val getCommand = "DEL key 1"
        store.executeCommand(getCommand)
        Assertions.assertEquals("invalid command${System.lineSeparator()}", byteArrayOutputStream.toString())
    }

    @Test
    fun setAndDelSuccessful() {
        val store = Store("test.txt")
        byteArrayOutputStream.reset() // skip log(load data)
        val setCommand = "SET key 1"
        store.executeCommand(setCommand)
        Assertions.assertEquals("OK!${System.lineSeparator()}", byteArrayOutputStream.toString())
        byteArrayOutputStream.reset()

        val getCommand = "GET key"
        store.executeCommand(getCommand)
        Assertions.assertEquals("key: 1${System.lineSeparator()}", byteArrayOutputStream.toString())
        byteArrayOutputStream.reset()

        val delCommand = "DEL key"
        store.executeCommand(delCommand)
        Assertions.assertEquals("OK!${System.lineSeparator()}", byteArrayOutputStream.toString())
        byteArrayOutputStream.reset()

        store.executeCommand(getCommand)
        Assertions.assertEquals("key not exist${System.lineSeparator()}", byteArrayOutputStream.toString())
        byteArrayOutputStream.reset()
        val lines = FileUtils.readLines(file)
        Assertions.assertEquals(setCommand, lines[0])
        Assertions.assertEquals(delCommand, lines[1])
    }
}