package com.zshnb.kvstore

import java.nio.file.*

class WatchRunnable : Runnable {
    override fun run() {
        val watchService = FileSystems.getDefault().newWatchService()
        val path = Paths.get("data.txt")
        path.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY)
        while (true) {
            val watchKey = watchService.take()
            watchKey.pollEvents().forEach {
                println("context: ${it.context()}, kind: ${it.kind()}")
            }
        }
    }
}