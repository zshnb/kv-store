package com.zshnb.kvstore

enum class Command {
    GET,
    SET,
    DEL,
    BEGIN,
    ROLLBACK,
    COMMIT
}