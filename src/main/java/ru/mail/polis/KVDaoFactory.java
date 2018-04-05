package ru.mail.polis;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;

import ru.mail.polis.poletova_n.KVDaoImpl;

public final class KVDaoFactory {
    private KVDaoFactory() {
        // Not instantiatable
    }

    @NotNull
    public static KVDao create() throws IOException {
        //throw new UnsupportedOperationException("Implement me");
        return new KVDaoImpl();
    }
}
