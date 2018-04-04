package ru.mail.polis;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.zhenytim.KVDaoImpl;

import java.io.IOException;

public final class KVDaoFactory {
    private KVDaoFactory() {
        // Not instantiatable
    }

    @NotNull
    public static KVDao create() throws IOException {
        return new KVDaoImpl();
    }
}
