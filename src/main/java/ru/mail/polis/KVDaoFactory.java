package ru.mail.polis;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.sql.SQLException;

public final class KVDaoFactory {

    private KVDaoFactory() {

    }

    @NotNull
    public static KVDao create() throws IOException {
        return new Dao();
    }

}
